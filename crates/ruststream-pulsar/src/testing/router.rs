//! Consumer registry, delivery and retained log for the in-process Pulsar stand-in.
//!
//! Two steps, the way Pulsar has them. A published message reaches every SUBSCRIPTION whose
//! [`Route`] covers the address it went to - one topic, the list of a multi-topic subscription,
//! or the regular expression of a pattern subscription - and within each of those, the
//! subscription's [`SubscriptionType`] picks the one CONSUMER that takes it: `Exclusive` and
//! `Failover` deliver to the active consumer, `Shared` rotates, `KeyShared` splits by partition
//! key. Competing consumers therefore split a stream here rather than each seeing all of it,
//! which is the thing a service writes a test about.
//!
//! The per-name log is what makes the stand-in a log broker rather than a pipe: it backs the
//! publish assertions, and a consumer can be repositioned over it, so the crate's seek surface
//! works in process. What is still not simulated is the reliability machinery a Pulsar server
//! runs: dead-letter policies and their delivery counts, ack timeouts, and credit.
//!
//! Every consumer's queue lives here, under the one lock the log lives under. That is what makes
//! a reposition atomic: a seek drains the queue and refills it from the log in a single critical
//! section, so no publish can slip between the two halves, and the harness's in-flight count is
//! correct the moment the seek returns rather than whenever the subscriber next polls.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{
    Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::task::AtomicWaker;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};

use crate::PARTITION_KEY_HEADER;
use crate::message::PulsarPosition;
use crate::subscription::SubscriptionType;

/// Opaque handle identifying one consumer inside an [`AddressRouter`].
///
/// Ordering is attach order, which is what makes the first consumer of a `Failover` subscription
/// the active one and the rest its standbys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ConsumerId(u64);

/// Single delivery handed to a consumer.
///
/// `seq` is the message's index in its address's log, assigned at fanout and preserved across
/// requeues, so a redelivered message reports the position it always had. `address` travels with
/// it because a requeue goes back to the subscription rather than to the consumer that gave up
/// on it, and choosing the consumer again needs the topic it was published to.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    pub(crate) seq: usize,
    address: String,
}

/// Which addresses one consumer covers.
///
/// The variants are the descriptor's own, so a route cannot be half a list and half a pattern:
/// [`PulsarSubscription::new`](crate::PulsarSubscription::new) and
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
    /// Compiles a pattern subscription's regular expression.
    pub(crate) fn pattern(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(Self::Pattern)
    }

    /// Whether a message published to `address` belongs to this consumer.
    fn covers(&self, address: &str) -> bool {
        match self {
            Self::Topics(topics) => topics.iter().any(|topic| topic == address),
            Self::Pattern(pattern) => pattern.is_match(address),
        }
    }

    /// Whether two routes can select the same topic, which is what makes their consumers members
    /// of one subscription rather than of two that happen to share a name.
    ///
    /// Two patterns are compared by their source text: regular-expression intersection is not
    /// something to decide here, and the case that matters - one descriptor mounted twice - is
    /// exact.
    fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Topics(mine), Self::Topics(theirs)) => {
                mine.iter().any(|topic| theirs.contains(topic))
            }
            (Self::Topics(topics), Self::Pattern(pattern))
            | (Self::Pattern(pattern), Self::Topics(topics)) => {
                topics.iter().any(|topic| pattern.is_match(topic))
            }
            (Self::Pattern(mine), Self::Pattern(theirs)) => mine.as_str() == theirs.as_str(),
        }
    }
}

/// The subscription a consumer joined: its name, and the rule by which that subscription hands
/// its deliveries to the consumers on it.
///
/// The rule is the descriptor's own [`SubscriptionType`], so the stand-in and the server read one
/// declaration rather than two.
#[derive(Debug, Clone)]
pub(crate) struct Membership {
    name: String,
    sharing: SubscriptionType,
}

impl Membership {
    pub(crate) fn new(name: String, sharing: SubscriptionType) -> Self {
        Self { name, sharing }
    }
}

/// A consumer's attempt to join a subscription another consumer holds exclusively, or to claim
/// exclusively one that is already held. A Pulsar broker refuses the attach with `ConsumerBusy`.
#[derive(Debug)]
pub(crate) struct ExclusiveHeld {
    pub(crate) subscription: String,
}

impl fmt::Display for ExclusiveHeld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "subscription '{}' is exclusive and already has a consumer",
            self.subscription
        )
    }
}

impl std::error::Error for ExclusiveHeld {}

/// One attached consumer: what it covers, which subscription it belongs to, and its queue.
struct Consumer {
    route: Route,
    membership: Membership,
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
    consumers: HashMap<ConsumerId, Consumer>,
    log: HashMap<String, Vec<LogEntry>>,
    /// Where each `Shared` subscription's rotation stands, by subscription name. Kept across a
    /// consumer joining or leaving, so the rotation carries on rather than restarting.
    rotation: HashMap<String, usize>,
}

impl RouterState {
    fn entries(&self, address: &str) -> &[LogEntry] {
        self.log.get(address).map_or(&[], Vec::as_slice)
    }

    /// The consumers over `address`, grouped into the subscriptions they belong to and ordered
    /// within each by attach order. A message reaches every one of those subscriptions; which of
    /// its consumers takes it is [`RouterState::choose`].
    fn subscriptions_over(
        &self,
        address: &str,
    ) -> Vec<(String, SubscriptionType, Vec<ConsumerId>)> {
        let mut grouped: BTreeMap<&str, Vec<ConsumerId>> = BTreeMap::new();
        for (id, consumer) in &self.consumers {
            if consumer.route.covers(address) {
                grouped
                    .entry(consumer.membership.name.as_str())
                    .or_default()
                    .push(*id);
            }
        }
        grouped
            .into_iter()
            .map(|(name, mut members)| {
                members.sort_unstable();
                // The consumer that established the subscription sets its type, as it does on a
                // server, where a later attach naming another type is refused.
                let sharing = self.consumers[&members[0]].membership.sharing;
                (name.to_owned(), sharing, members)
            })
            .collect()
    }

    /// The members of one named subscription over `address`, in attach order.
    fn members_of(&self, address: &str, subscription: &str) -> Vec<ConsumerId> {
        let mut members: Vec<ConsumerId> = self
            .consumers
            .iter()
            .filter(|(_, consumer)| {
                consumer.membership.name == subscription && consumer.route.covers(address)
            })
            .map(|(id, _)| *id)
            .collect();
        members.sort_unstable();
        members
    }

    /// Which consumer of a subscription takes `delivery`.
    ///
    /// `Exclusive` and `Failover` deliver to the one active consumer - for `Failover` that is the
    /// first attached, and its standbys wait for it to go. `Shared` rotates over the members, so
    /// competing consumers split the stream rather than each seeing all of it. `KeyShared` splits
    /// it by the partition key instead, so one key always lands on one consumer.
    ///
    /// `members` must be non-empty and in attach order.
    fn choose(
        &mut self,
        subscription: &str,
        sharing: SubscriptionType,
        members: &[ConsumerId],
        delivery: &Delivery,
    ) -> ConsumerId {
        match sharing {
            SubscriptionType::Exclusive | SubscriptionType::Failover => members[0],
            SubscriptionType::Shared => {
                let turn = self.rotation.entry(subscription.to_owned()).or_default();
                let chosen = members[*turn % members.len()];
                *turn = turn.wrapping_add(1);
                chosen
            }
            SubscriptionType::KeyShared => members[key_slot(delivery, members.len())],
        }
    }

    /// Queues `delivery` for `id` and wakes it. Reports whether the consumer was still there.
    fn enqueue(&mut self, id: ConsumerId, delivery: Delivery) -> bool {
        let Some(consumer) = self.consumers.get_mut(&id) else {
            return false;
        };
        consumer.queue.push_back(delivery);
        consumer.waker.wake();
        true
    }
}

/// Which consumer of a `KeyShared` subscription owns a delivery's key.
///
/// Pulsar hands each consumer a RANGE of key hashes; this is the key's hash modulo the consumer
/// count. The property a test rests on holds either way - one consumer per key, stable while the
/// consumer set is - but which consumer that turns out to be differs from a server's, and so does
/// what a join or a departure reshuffles. A delivery with no partition key hashes as the empty
/// key, so unkeyed traffic gathers on one consumer.
fn key_slot(delivery: &Delivery, consumers: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    delivery
        .headers
        .get(PARTITION_KEY_HEADER)
        .unwrap_or_default()
        .hash(&mut hasher);
    let count = u64::try_from(consumers).unwrap_or(1).max(1);
    usize::try_from(hasher.finish() % count).unwrap_or(0)
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

    /// Attaches a consumer covering `route` to the subscription `membership` names, returning
    /// the id its subscriber polls, repositions and detaches with.
    ///
    /// # Errors
    ///
    /// Returns [`ExclusiveHeld`] when the subscription already has a consumer over the same
    /// topics and either side asked for [`SubscriptionType::Exclusive`], which is the attach a
    /// Pulsar broker refuses.
    // significant_drop_tightening misfires: the guard reads the consumers and then inserts under
    // the same lock, which is what keeps two racing attaches from both passing the check.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn subscribe(
        &self,
        route: Route,
        membership: Membership,
    ) -> Result<ConsumerId, ExclusiveHeld> {
        let mut state = self.lock();
        let contested = state.consumers.values().any(|consumer| {
            consumer.membership.name == membership.name
                && consumer.route.overlaps(&route)
                && (consumer.membership.sharing == SubscriptionType::Exclusive
                    || membership.sharing == SubscriptionType::Exclusive)
        });
        if contested {
            return Err(ExclusiveHeld {
                subscription: membership.name,
            });
        }

        let id = ConsumerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        state.consumers.insert(
            id,
            Consumer {
                route,
                membership,
                queue: VecDeque::new(),
                waker: AtomicWaker::new(),
            },
        );
        Ok(id)
    }

    /// Detaches a consumer. No-op if the id is unknown (double-drop of the subscriber). A
    /// `Failover` standby becomes the active consumer here, by being the first one left.
    pub(crate) fn unsubscribe(&self, id: ConsumerId) {
        self.lock().consumers.remove(&id);
    }

    /// Takes the next delivery queued for `id`, parking the caller on the consumer's waker while
    /// it is empty. Ends the stream once the consumer is gone (detached, or the broker shut
    /// down).
    pub(crate) fn poll_delivery(&self, id: ConsumerId, cx: &Context<'_>) -> Poll<Option<Delivery>> {
        let mut state = self.lock();
        let Some(consumer) = state.consumers.get_mut(&id) else {
            return Poll::Ready(None);
        };
        // Register before taking, so a publish landing right after an empty read still wakes us.
        consumer.waker.register(cx.waker());
        let next = consumer.queue.pop_front();
        drop(state);
        next.map_or(Poll::Pending, |delivery| Poll::Ready(Some(delivery)))
    }

    /// Returns `delivery` to the subscription `id` belongs to, for `nack(requeue = true)`.
    ///
    /// The redelivery goes to the subscription rather than back at the consumer that gave up on
    /// it, so the type picks a consumer again: on a `Shared` subscription the retry can land on a
    /// sibling, as it can on a server, while `Exclusive`, `Failover` and a `KeyShared` key all
    /// resolve to the consumer they resolved to before. Reports whether a consumer was still
    /// there to take it.
    // significant_drop_tightening misfires: the guard is used up to the last statement.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn requeue(&self, id: ConsumerId, delivery: Delivery) -> bool {
        let mut state = self.lock();
        let Some(consumer) = state.consumers.get(&id) else {
            return false;
        };
        let membership = consumer.membership.clone();
        let members = state.members_of(&delivery.address, &membership.name);
        if members.is_empty() {
            return false;
        }
        let target = state.choose(&membership.name, membership.sharing, &members, &delivery);
        state.enqueue(target, delivery)
    }

    /// Appends `payload` to the address's log and hands it to every subscription over that
    /// address - once each, to the one consumer the subscription's type selects. Under a harness
    /// run every live enqueue is counted with [`Coordinator::enqueued`].
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
            address: address.to_owned(),
        };
        for (name, sharing, members) in state.subscriptions_over(address) {
            let target = state.choose(&name, sharing, &members, &delivery);
            if state.enqueue(target, delivery.clone())
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
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

    /// Repositions consumer `id` to `position`: everything queued for it is dropped, and the log
    /// suffix from the target on takes its place.
    ///
    /// A consumer over several topics reads a log per topic, and the position applies to each of
    /// them - a seek on a real multi-topic consumer covers every topic it holds. The replayed
    /// suffixes are merged by publish time, which is the order the consumer observed them the
    /// first time round.
    ///
    /// It moves the consumer that asked, not its whole subscription: on a server the cursor
    /// belongs to the subscription, so a seek from one consumer of a `Shared` subscription moves
    /// its siblings too. Modelling that needs a per-subscription cursor rather than a per-consumer
    /// queue, and a service seeks from a subscription it holds alone.
    ///
    /// Both halves run in one critical section, so a concurrent publish lands wholly before or
    /// wholly after the swap and cannot be lost or duplicated. The harness accounting is
    /// finished here too - the replay counted in flight, the discarded queue counted consumed -
    /// so a caller that awaits this seek can then wait for quiescence and see the replay.
    // significant_drop_tightening misfires: the guard is used up to the last statement.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn seek(
        &self,
        id: ConsumerId,
        position: &PulsarPosition,
        coordinator: Option<&Coordinator>,
    ) {
        let mut state = self.lock();
        let Some(route) = state
            .consumers
            .get(&id)
            .map(|consumer| consumer.route.clone())
        else {
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
                        address: address.clone(),
                    },
                )
            }));
        }
        merged.sort_by_key(|(at, _)| *at);
        let replay: VecDeque<Delivery> = merged.into_iter().map(|(_, delivery)| delivery).collect();

        let consumer = state
            .consumers
            .get_mut(&id)
            .expect("the consumer was present a moment ago, under this same lock");
        if let Some(coordinator) = coordinator {
            // The replay is counted in flight BEFORE the discard releases the queued
            // deliveries. Counting the other way round would let the in-flight total touch zero
            // mid-swap, and a concurrent quiescence wait could take that instant for the end of
            // the reaction.
            for _ in 0..replay.len() {
                coordinator.enqueued();
            }
            for _ in 0..consumer.queue.len() {
                // Each was counted in flight when it was queued and will never be delivered.
                coordinator.consumed();
            }
        }
        consumer.queue = replay;
        consumer.waker.wake();
    }

    /// Detaches every consumer and clears the retained log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.consumers.clear();
        state.log.clear();
        state.rotation.clear();
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

impl fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("AddressRouter")
            .field("consumers", &state.consumers.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
