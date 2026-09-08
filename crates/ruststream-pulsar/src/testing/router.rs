//! Subscription registry, fanout and retained log for the in-process Pulsar stand-in.
//!
//! Core routing only: a published message reaches every live subscription whose [`Route`]
//! covers the address it went to - one topic, the list of a multi-topic subscription, or the
//! regular expression of a pattern subscription. The per-name log is what makes the stand-in a
//! log broker rather than a pipe: it backs the publish assertions, and a subscription can be
//! repositioned over it, so the crate's seek surface works in process. Pulsar's own semantics
//! (subscription types, dead-letter policies, ack timeouts, key sharing) are transport
//! behaviour and are not simulated here.
//!
//! Every subscription's queue lives here, under the one lock the log lives under. That is what
//! makes a reposition atomic: a seek drains the queue and refills it from the log in a single
//! critical section, so no publish can slip between the two halves, and the harness's in-flight
//! count is correct the moment the seek returns rather than whenever the subscriber next polls.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::task::AtomicWaker;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};

use crate::message::PulsarPosition;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
///
/// `seq` is the message's index in its address's log, assigned at fanout and preserved across
/// requeues, so a redelivered message reports the position it always had.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    pub(crate) seq: usize,
}

/// Which addresses one subscription covers.
///
/// The variants are the descriptor's own, so a subscription cannot be half a list and half a
/// pattern: [`PulsarSubscription::new`](crate::PulsarSubscription::new) and
/// [`topics`](crate::PulsarSubscription::topics) arrive as [`Route::Topics`], and
/// [`pattern`](crate::PulsarSubscription::pattern) as [`Route::Pattern`], compiled once here
/// rather than per published message.
#[derive(Debug, Clone)]
pub(crate) enum Route {
    /// Exact topic names: one for a single-topic subscription, several for a multi-topic one.
    Topics(Vec<String>),
    /// A regular expression over topic names, as Pulsar's pattern subscription applies it.
    Pattern(regex::Regex),
}

impl Route {
    /// The route of a subscription on one topic, which is what a bare name resolves to.
    pub(crate) fn topic(address: String) -> Self {
        Self::Topics(vec![address])
    }

    /// Compiles a pattern subscription's regular expression.
    pub(crate) fn pattern(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(Self::Pattern)
    }

    /// Whether a message published to `address` belongs to this subscription.
    fn covers(&self, address: &str) -> bool {
        match self {
            Self::Topics(topics) => topics.iter().any(|topic| topic == address),
            Self::Pattern(pattern) => pattern.is_match(address),
        }
    }
}

struct Subscription {
    route: Route,
    queue: VecDeque<Delivery>,
    waker: AtomicWaker,
}

/// One entry of an address's retained log.
struct LogEntry {
    message: RawMessage,
    /// Publish time in milliseconds since the Unix epoch, which
    /// [`PulsarPosition::Timestamp`] resolves against.
    at: u64,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<LogEntry>>,
}

impl RouterState {
    fn entries(&self, address: &str) -> &[LogEntry] {
        self.log.get(address).map_or(&[], Vec::as_slice)
    }
}

/// In-memory router over a retained per-address log.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    fn lock(&self) -> MutexGuard<'_, RouterState> {
        self.state
            .lock()
            .expect("pulsar test router mutex poisoned")
    }

    /// Registers a subscription covering `route`, returning the id its subscriber polls,
    /// repositions and unregisters with.
    pub(crate) fn subscribe(&self, route: Route) -> SubscriptionId {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.lock().subscriptions.insert(
            id,
            Subscription {
                route,
                queue: VecDeque::new(),
                waker: AtomicWaker::new(),
            },
        );
        id
    }

    /// Removes a subscription. No-op if the id is unknown (double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.lock().subscriptions.remove(&id);
    }

    /// Takes the next delivery queued for `id`, parking the caller on the subscription's waker
    /// while it is empty. Ends the stream once the subscription is gone (unsubscribed, or the
    /// broker shut down).
    pub(crate) fn poll_delivery(
        &self,
        id: SubscriptionId,
        cx: &Context<'_>,
    ) -> Poll<Option<Delivery>> {
        let mut state = self.lock();
        let Some(sub) = state.subscriptions.get_mut(&id) else {
            return Poll::Ready(None);
        };
        // Register before taking, so a publish landing right after an empty read still wakes us.
        sub.waker.register(cx.waker());
        let next = sub.queue.pop_front();
        drop(state);
        next.map_or(Poll::Pending, |delivery| Poll::Ready(Some(delivery)))
    }

    /// Puts `delivery` back at the end of its subscription's queue, for `nack(requeue = true)`.
    /// Reports whether the subscription was still there to take it.
    pub(crate) fn requeue(&self, id: SubscriptionId, delivery: Delivery) -> bool {
        let mut state = self.lock();
        let Some(sub) = state.subscriptions.get_mut(&id) else {
            return false;
        };
        sub.queue.push_back(delivery);
        sub.waker.wake();
        drop(state);
        true
    }

    /// Appends `payload` to the address's log and queues it on every subscription whose route
    /// covers that address. Under a harness run every live enqueue is counted with
    /// [`Coordinator::enqueued`].
    // significant_drop_tightening misfires: the guard is used up to the last statement.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let snapshot = RawMessage::new(address, payload.clone()).with_headers(headers.clone());
        let mut state = self.lock();
        let entries = state.log.entry(address.to_owned()).or_default();
        let seq = entries.len();
        entries.push(LogEntry {
            message: snapshot,
            at: now_millis(),
        });

        let delivery = Delivery {
            payload,
            headers,
            seq,
        };
        for sub in state.subscriptions.values_mut() {
            if sub.route.covers(address) {
                sub.queue.push_back(delivery.clone());
                sub.waker.wake();
                if let Some(coordinator) = coordinator {
                    coordinator.enqueued();
                }
            }
        }
    }

    /// Returns every message recorded for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.lock()
            .entries(address)
            .iter()
            .map(|entry| entry.message.clone())
            .collect()
    }

    /// Repositions subscription `id` to `position`: everything queued for it is dropped, and the
    /// log suffix from the target on takes its place.
    ///
    /// A subscription over several topics holds a log per topic, and the position applies to
    /// each of them - a seek on a real multi-topic consumer covers every topic it holds. The
    /// replayed suffixes are merged by publish time, which is the order the subscription
    /// observed them the first time round.
    ///
    /// Both halves run in one critical section, so a concurrent publish lands wholly before or
    /// wholly after the swap and cannot be lost or duplicated. The harness accounting is
    /// finished here too - the replay counted in flight, the discarded queue counted consumed -
    /// so a caller that awaits this seek can then wait for quiescence and see the replay.
    // significant_drop_tightening misfires: the guard is used up to the last statement.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn seek(
        &self,
        id: SubscriptionId,
        position: &PulsarPosition,
        coordinator: Option<&Coordinator>,
    ) {
        let mut state = self.lock();
        let Some(route) = state.subscriptions.get(&id).map(|sub| sub.route.clone()) else {
            return;
        };
        let mut merged: Vec<(u64, Delivery)> = Vec::new();
        for (address, entries) in &state.log {
            if !route.covers(address) {
                continue;
            }
            let target = resolve(entries, position);
            merged.extend(entries.iter().enumerate().skip(target).map(|(seq, entry)| {
                (
                    entry.at,
                    Delivery {
                        payload: entry.message.payload_bytes(),
                        headers: entry.message.headers().clone(),
                        seq,
                    },
                )
            }));
        }
        merged.sort_by_key(|(at, _)| *at);
        let replay: VecDeque<Delivery> = merged.into_iter().map(|(_, delivery)| delivery).collect();

        let sub = state
            .subscriptions
            .get_mut(&id)
            .expect("the subscription was present a moment ago, under this same lock");
        if let Some(coordinator) = coordinator {
            // The replay is counted in flight BEFORE the discard releases the queued
            // deliveries. Counting the other way round would let the in-flight total touch zero
            // mid-swap, and a concurrent quiescence wait could take that instant for the end of
            // the reaction.
            for _ in 0..replay.len() {
                coordinator.enqueued();
            }
            for _ in 0..sub.queue.len() {
                // Each was counted in flight when it was queued and will never be delivered.
                coordinator.consumed();
            }
        }
        sub.queue = replay;
        sub.waker.wake();
    }

    /// Drops every subscription and clears the retained log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.subscriptions.clear();
        state.log.clear();
    }
}

/// Resolves `position` to an index in `entries`, clamped to its end.
///
/// Clamping is what makes a seek past the tip mean "deliver nothing until the next publish"
/// rather than an error.
fn resolve(entries: &[LogEntry], position: &PulsarPosition) -> usize {
    let target = match position {
        PulsarPosition::Earliest => 0,
        PulsarPosition::Latest => entries.len(),
        // The stand-in keeps one log per address, so a message id addresses an entry in it: the
        // entry id IS the log index, which is what `Positioned` reports.
        PulsarPosition::MessageId(id) => usize::try_from(id.entry_id).unwrap_or(usize::MAX),
        PulsarPosition::Timestamp(millis) => entries
            .iter()
            .position(|entry| entry.at >= *millis)
            .unwrap_or(entries.len()),
    };
    target.min(entries.len())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

impl std::fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
