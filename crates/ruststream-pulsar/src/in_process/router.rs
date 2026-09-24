//! Consumer registry, delivery and retained log of the in-process transport.
//!
//! Two steps, the way Pulsar has them. A published message reaches every SUBSCRIPTION whose
//! [`Route`] reads the topic it went to, and within each of those the subscription's
//! [`SubscriptionType`] picks the one CONSUMER that takes it: `Exclusive` and `Failover` deliver
//! to the active consumer, `Shared` rotates, `KeyShared` splits by partition key. Competing
//! consumers therefore split a stream here rather than each seeing all of it.
//!
//! Topics are fully qualified here, as on a server. A pattern subscription reads the topics that
//! exist when it opens, and a topic created later from the first time the client lists the
//! namespace again: every thirty seconds of the runtime's clock after the subscription opened.
//! What reaches such a topic before that is not delivered to the pattern subscription, because
//! the subscription the client then creates on the topic starts at its tip.
//!
//! The per-topic log is what makes the transport a log broker rather than a pipe: it backs the
//! publish assertions, and a subscription can be repositioned over it. A consumer also counts the
//! redeliveries of a message and, at the limit the registration declared, produces it to the
//! dead-letter topic, which is where the Pulsar client does it too.
//!
//! Every consumer's queue lives here, under the one lock the log lives under. That is what makes
//! a reposition atomic: a seek drains the queues and refills them from the log in a single
//! critical section, so no publish can slip between the two halves, and the harness's in-flight
//! count is correct the moment the seek returns.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::task::AtomicWaker;
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage};
use tokio::time::Instant;

use crate::in_process::route::Route;
use crate::message::{PARTITION_KEY_HEADER, PulsarPosition};
use crate::subscription::{DeadLetterRoute, SubscriptionType};
use crate::topic::PulsarTopic;

/// How often the client lists the lookup namespace again for a pattern subscription: its own
/// default, which this crate leaves in place.
const PATTERN_REFRESH: Duration = Duration::from_secs(30);

/// Opaque handle identifying one consumer inside an [`AddressRouter`].
///
/// Ordering is attach order, which is what makes the first consumer of a `Failover` subscription
/// the active one and the rest its standbys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ConsumerId(u64);

/// Single delivery handed to a consumer.
///
/// `seq` is the message's index in its topic's log, assigned at fanout and preserved across
/// requeues, so a redelivered message reports the position it always had. `topic` travels with it
/// because a requeue goes back to the subscription rather than to the consumer that gave up on it.
/// `redeliveries` is the count the Pulsar client keeps and compares against the consumer's
/// dead-letter limit; the handler does not see it, because the client does not show it either.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    pub(crate) seq: usize,
    pub(crate) topic: String,
    pub(crate) redeliveries: u32,
}

/// The subscription a consumer joined: its name, and the rule by which that subscription hands
/// its deliveries to the consumers on it.
#[derive(Debug, Clone)]
pub(crate) struct Membership {
    name: String,
    sharing: SubscriptionType,
}

impl Membership {
    pub(crate) const fn new(name: String, sharing: SubscriptionType) -> Self {
        Self { name, sharing }
    }
}

/// A consumer's attempt to join a subscription another consumer holds exclusively, or to claim
/// exclusively one that is already held. A Pulsar broker answers the attach with `ConsumerBusy`.
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

/// When something happened in the router: the runtime's clock, which a paused test clock can
/// hold still, and the order of events, which tells apart two that happened at one instant.
#[derive(Debug, Clone, Copy)]
struct Moment {
    at: Instant,
    order: u64,
}

/// One attached consumer: what it reads, which subscription it belongs to, the dead-letter
/// policy the registration declared for it, and its queue.
struct Consumer {
    route: Route,
    /// When the consumer opened, which is when a pattern subscription first listed its topics.
    opened: Moment,
    membership: Membership,
    dead_letter: Option<DeadLetterRoute>,
    queue: VecDeque<Delivery>,
    waker: AtomicWaker,
}

/// One entry of a topic's retained log.
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
    /// When each topic came to exist: its first publish, or the first subscription naming it.
    created: HashMap<String, Moment>,
    /// How many events the router has ordered so far.
    events: u64,
    /// Where each `Shared` subscription's rotation stands, by subscription name. Kept across a
    /// consumer joining or leaving, so the rotation carries on rather than restarting.
    rotation: HashMap<String, usize>,
}

impl RouterState {
    /// The moment of an event happening now.
    fn moment(&mut self) -> Moment {
        self.events += 1;
        Moment {
            at: Instant::now(),
            order: self.events,
        }
    }

    fn entries(&self, topic: &str) -> &[LogEntry] {
        self.log.get(topic).map_or(&[], Vec::as_slice)
    }

    /// Whether `consumer` reads `topic` at `now`: its route selects the topic, and, for a
    /// pattern, the client has listed the topic since it came to exist.
    fn reads(&self, consumer: &Consumer, topic: &str, now: Instant) -> bool {
        let Ok(parsed) = PulsarTopic::parse(topic) else {
            return false;
        };
        if !consumer.route.selects(&parsed) {
            return false;
        }
        if !consumer.route.is_pattern() {
            return true;
        }
        self.created
            .get(topic)
            .is_some_and(|&created| listed_by(consumer.opened, created, now))
    }

    /// The consumers reading `topic`, grouped into the subscriptions they belong to and ordered
    /// within each by attach order. A message reaches every one of those subscriptions; which of
    /// its consumers takes it is [`RouterState::choose`].
    fn subscriptions_over(
        &self,
        topic: &str,
        now: Instant,
    ) -> Vec<(String, SubscriptionType, Vec<ConsumerId>)> {
        let mut grouped: BTreeMap<&str, Vec<ConsumerId>> = BTreeMap::new();
        for (id, consumer) in &self.consumers {
            if self.reads(consumer, topic, now) {
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

    /// The members of one named subscription reading `topic`, in attach order.
    fn members_of(&self, topic: &str, subscription: &str, now: Instant) -> Vec<ConsumerId> {
        let mut members: Vec<ConsumerId> = self
            .consumers
            .iter()
            .filter(|(_, consumer)| {
                consumer.membership.name == subscription && self.reads(consumer, topic, now)
            })
            .map(|(id, _)| *id)
            .collect();
        members.sort_unstable();
        members
    }

    /// Which consumer of a subscription takes `delivery`.
    ///
    /// `Exclusive` and `Failover` deliver to the one active consumer, the first attached.
    /// `Shared` rotates over the members, so competing consumers split the stream. `KeyShared`
    /// splits it by the partition key, so one key always lands on one consumer.
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

    /// Appends a message to `topic` and hands it to every subscription reading that topic, once
    /// each. Runs under the caller's lock, so a dead-letter produce can share it with the
    /// requeue that triggered it.
    fn deliver(
        &mut self,
        topic: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let moment = self.moment();
        let now = moment.at;
        self.created.entry(topic.to_owned()).or_insert(moment);
        let snapshot = RawMessage::new(topic, payload.clone()).with_headers(headers.clone());
        let entries = self.log.entry(topic.to_owned()).or_default();
        let seq = entries.len();
        entries.push(LogEntry {
            message: snapshot,
            at: now_millis(),
        });

        let delivery = Delivery {
            payload,
            headers,
            seq,
            topic: topic.to_owned(),
            redeliveries: 0,
        };
        for (name, sharing, members) in self.subscriptions_over(topic, now) {
            let target = self.choose(&name, sharing, &members, &delivery);
            if self.enqueue(target, delivery.clone())
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
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

/// Whether a pattern subscription opened at `opened` has listed a topic created at `created` by
/// `now`: the topic existed when it opened, or a refresh has run since the topic appeared.
fn listed_by(opened: Moment, created: Moment, now: Instant) -> bool {
    if created.order < opened.order {
        return true;
    }
    let refresh = PATTERN_REFRESH.as_nanos();
    let since = created.at.saturating_duration_since(opened.at).as_nanos();
    // The first listing after the one the subscription opened with.
    let ticks = since.div_ceil(refresh).max(1);
    let listed_at = u64::try_from(ticks.saturating_mul(refresh))
        .ok()
        .and_then(|nanos| opened.at.checked_add(Duration::from_nanos(nanos)));
    listed_at.is_some_and(|listed_at| listed_at <= now)
}

/// Which consumer of a `KeyShared` subscription owns a delivery's key.
///
/// Pulsar hands each consumer a RANGE of key hashes; this is the key's hash modulo the consumer
/// count. One key stays on one consumer while the consumer set does, which is what a test rests
/// on, but which consumer that is differs from a server's. A delivery with no partition key
/// hashes as the empty key, so unkeyed traffic gathers on one consumer.
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

/// In-memory router over a retained per-topic log.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    fn lock(&self) -> MutexGuard<'_, RouterState> {
        self.state
            .lock()
            .expect("the router is never held across a panic")
    }

    /// Attaches a consumer reading `route` to the subscription `membership` names, returning the
    /// id its subscriber polls, repositions and detaches with. A consumer naming a topic creates
    /// it, as a subscription on a server does.
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
        dead_letter: Option<DeadLetterRoute>,
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

        let opened = state.moment();
        for topic in route.topics() {
            state.created.entry(topic.clone()).or_insert(opened);
        }
        let id = ConsumerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        state.consumers.insert(
            id,
            Consumer {
                route,
                opened,
                membership,
                dead_letter,
                queue: VecDeque::new(),
                waker: AtomicWaker::new(),
            },
        );
        Ok(id)
    }

    /// Detaches a consumer. No-op if the id is unknown. A `Failover` standby becomes the active
    /// consumer here, by being the first one left.
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

    /// Returns `delivery` to the subscription `id` belongs to, for a negative acknowledgement.
    ///
    /// The redelivery goes to the subscription rather than back at the consumer that gave up on
    /// it, so the type picks a consumer again: on a `Shared` subscription the retry can land on a
    /// sibling, as it can on a server.
    ///
    /// A consumer carrying the registration's dead-letter policy counts the redelivery first,
    /// and at the limit produces the message to the dead-letter topic instead of handing it
    /// back, which is where the Pulsar client applies the policy too.
    ///
    /// Reports whether a consumer was still there to take it; a dead-lettered message reports
    /// `false`, because the produce has already counted its own enqueues.
    // significant_drop_tightening misfires: the guard is used up to the last statement.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn requeue(
        &self,
        id: ConsumerId,
        mut delivery: Delivery,
        coordinator: Option<&Coordinator>,
    ) -> bool {
        let mut state = self.lock();
        let Some(consumer) = state.consumers.get(&id) else {
            return false;
        };
        let membership = consumer.membership.clone();
        let dead_letter = consumer.dead_letter.clone();
        delivery.redeliveries = delivery.redeliveries.saturating_add(1);
        if let Some(policy) = dead_letter
            && delivery.redeliveries >= policy.max_deliveries
        {
            // The registration's declaration parsed the topic when it was taken in.
            let topic = PulsarTopic::parse(&policy.topic)
                .map_or(policy.topic, |topic| topic.as_str().to_owned());
            state.deliver(&topic, delivery.payload, delivery.headers, coordinator);
            return false;
        }
        let members = state.members_of(&delivery.topic, &membership.name, Instant::now());
        if members.is_empty() {
            return false;
        }
        let target = state.choose(&membership.name, membership.sharing, &members, &delivery);
        state.enqueue(target, delivery)
    }

    /// Appends `payload` to the topic's log and hands it to every subscription reading the topic,
    /// once each, to the one consumer the subscription's type selects. Under a harness run every
    /// live enqueue is counted with [`Coordinator::enqueued`].
    pub(crate) fn publish(
        &self,
        topic: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        self.lock().deliver(topic, payload, headers, coordinator);
    }

    /// Returns every message recorded for `topic`, in publish order.
    pub(crate) fn published(&self, topic: &str) -> Vec<RawMessage> {
        self.lock()
            .entries(topic)
            .iter()
            .map(|entry| entry.message.clone())
            .collect()
    }

    /// Repositions the subscription consumer `id` belongs to: everything queued for any of its
    /// consumers is dropped, and the log suffix from the target on takes its place, shared out
    /// among them as fresh deliveries are.
    ///
    /// The cursor belongs to the subscription on a server, so a seek from one consumer moves its
    /// siblings too. A consumer over several topics reads a log per topic, and the position
    /// applies to each of them; the replayed suffixes are merged by publish time. A
    /// non-persistent topic keeps no log on a server, so a seek over one replays nothing from it.
    ///
    /// Both halves run in one critical section, so a concurrent publish lands wholly before or
    /// wholly after the swap. The harness accounting is finished here too, so a caller that
    /// awaits this seek can then wait for quiescence and see the replay.
    // significant_drop_tightening misfires: the guard is used up to the last statement.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn seek(
        &self,
        id: ConsumerId,
        position: &PulsarPosition,
        coordinator: Option<&Coordinator>,
    ) {
        let mut state = self.lock();
        let now = Instant::now();
        let Some(seeker) = state.consumers.get(&id) else {
            return;
        };
        let subscription = seeker.membership.clone();
        let route = seeker.route.clone();
        let mut topics: Vec<String> = state
            .log
            .keys()
            .chain(seeker.route.topics())
            .filter(|topic| {
                !topic.starts_with("non-persistent://") && state.reads(seeker, topic, now)
            })
            .cloned()
            .collect();
        topics.sort_unstable();
        topics.dedup();

        let mut merged: Vec<(u64, Delivery)> = Vec::new();
        for topic in &topics {
            let entries = state.entries(topic);
            let target = resolve(entries, position);
            merged.extend(entries.iter().enumerate().skip(target).map(|(seq, entry)| {
                (
                    entry.at,
                    Delivery {
                        payload: entry.message.payload_bytes(),
                        headers: entry.message.headers().clone(),
                        seq,
                        topic: topic.clone(),
                        // A replay is a fresh delivery of the log entry, which is how a server
                        // counts one after a seek.
                        redeliveries: 0,
                    },
                )
            }));
        }
        merged.sort_by_key(|(at, _)| *at);

        let siblings: Vec<ConsumerId> = state
            .consumers
            .iter()
            .filter(|(_, consumer)| {
                consumer.membership.name == subscription.name && consumer.route.overlaps(&route)
            })
            .map(|(sibling, _)| *sibling)
            .collect();
        let mut discarded = 0;
        for sibling in &siblings {
            if let Some(consumer) = state.consumers.get_mut(sibling) {
                discarded += consumer.queue.len();
                consumer.queue.clear();
            }
        }
        let mut replayed = 0;
        for (_, delivery) in merged {
            let members = state.members_of(&delivery.topic, &subscription.name, now);
            if members.is_empty() {
                continue;
            }
            let target = state.choose(
                &subscription.name,
                subscription.sharing,
                &members,
                &delivery,
            );
            if state.enqueue(target, delivery) {
                replayed += 1;
            }
        }
        if let Some(coordinator) = coordinator {
            // The replay is counted in flight before the discarded deliveries are released, so
            // the in-flight total cannot touch zero mid-swap and look like the end of the
            // reaction to a concurrent quiescence wait.
            for _ in 0..replayed {
                coordinator.enqueued();
            }
            for _ in 0..discarded {
                // Each was counted in flight when it was queued and will never be delivered.
                coordinator.consumed();
            }
        }
        for sibling in &siblings {
            if let Some(consumer) = state.consumers.get(sibling) {
                consumer.waker.wake();
            }
        }
    }

    /// Detaches every consumer and clears the retained log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        for consumer in state.consumers.values() {
            consumer.waker.wake();
        }
        state.consumers.clear();
        state.log.clear();
        state.created.clear();
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
        // One log per topic, so a message id addresses an entry in it: the entry id IS the log
        // index, which is what the in-process delivery reports as its position.
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
            .field("topics", &state.log.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A topic that existed when the pattern subscription opened is read at once; one created
    /// later is read from the first refresh after it appeared, and not before.
    #[test]
    fn a_pattern_reads_a_later_topic_from_the_next_refresh() {
        let start = Instant::now();
        let at = |order, after: u64| Moment {
            at: start + Duration::from_secs(after),
            order,
        };
        let opened = at(2, 0);

        assert!(listed_by(opened, at(1, 0), start), "it existed at open");
        assert!(
            !listed_by(opened, at(3, 0), start + Duration::from_secs(29)),
            "created at the same instant, after the open",
        );
        assert!(listed_by(opened, at(3, 0), start + PATTERN_REFRESH));
        assert!(!listed_by(
            opened,
            at(3, 5),
            start + Duration::from_secs(29)
        ));
        assert!(listed_by(opened, at(3, 5), start + PATTERN_REFRESH));
        assert!(!listed_by(
            opened,
            at(3, 31),
            start + Duration::from_secs(59)
        ));
        assert!(listed_by(opened, at(3, 31), start + PATTERN_REFRESH * 2));
    }
}
