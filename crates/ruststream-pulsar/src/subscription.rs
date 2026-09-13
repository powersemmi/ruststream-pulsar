//! [`PulsarSubscription`]: the subscription descriptor.
//!
//! The subscription type is an enum, not a set of sibling flags, so combinations that do not
//! exist are unrepresentable; the ack timeout and the dead-letter policy the registration
//! declares are consumer-side settings the Pulsar client owns.

use std::time::Duration;

use ruststream::{BrokerMoves, RetryDeclaration, SubscriptionSource};

use crate::broker::ConnectedPulsarBroker;
use crate::error::PulsarError;
use crate::subscriber::PulsarSubscriber;
use crate::topic::PulsarTopic;

/// How competing consumers on one subscription share its messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubscriptionType {
    /// One consumer holds the subscription; a second attach is rejected.
    Exclusive,
    /// Competing consumers, round-robin. The default.
    #[default]
    Shared,
    /// One active consumer with hot standbys.
    Failover,
    /// Competing consumers with per-key ordering (the `Partitioned` capability's transport).
    KeyShared,
}

/// The consumer's dead-letter policy, as the registration declared it: a delivery limit and the
/// topic a spent delivery moves to.
///
/// Both halves together, because that is what the Pulsar client applies. The live consumer and
/// the in-process stand-in read this one value, so a cap driven in a test is the cap production
/// configures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeadLetterRoute {
    pub(crate) topic: String,
    /// How many times the message is handed to a handler before it moves on.
    pub(crate) max_deliveries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Topics {
    List(Vec<String>),
    Pattern(String),
}

/// How long a partial batch waits for more deliveries before the handler sees it, unless the
/// descriptor names another value.
///
/// Short, because the cost of waiting is latency on a batch that is already useful; a service
/// trading latency for fuller batches raises it with [`PulsarSubscription::batch_wait`].
pub(crate) const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(10);

/// The subscription a bare topic name joins, on the real broker and on the stand-in alike.
///
/// A `#[subscriber("orders")]` names no subscription and Pulsar has no anonymous consumer, so
/// the crate supplies one: by-name handlers share this durable subscription under the default
/// [`SubscriptionType::Shared`], which is what makes two instances of a service competing
/// consumers rather than two independent readers of one topic.
pub(crate) const DEFAULT_SUBSCRIPTION: &str = "ruststream";

/// A subscription descriptor for one Pulsar subscription over one or more topics.
///
/// Where the subscription starts reading is not a descriptor option: it is the framework's
/// `start_at(..)` clause over [`PulsarPosition`](crate::PulsarPosition), which the `Seekable`
/// capability backs.
///
/// How often a spent delivery is retried, and where it goes afterwards, is not a descriptor
/// option either: the registration declares it at the mount site with
/// `b.include(handler).max_attempts(nonzero!(5)).dead_letter("orders-dlq")`, and this descriptor
/// turns the declaration into the consumer's own `DeadLetterPolicy`. Nothing is republished from
/// the service, so `out_retry(..)` does not compile over this descriptor.
///
/// Implements [`SubscriptionSource`] for the real broker and, behind the `testing` feature, for
/// the in-process stand-in, so the declaration below sits inline in the `#[subscriber(..)]`
/// decorator and mounts on either:
///
/// ```
/// use std::time::Duration;
/// use ruststream_pulsar::{PulsarSubscription, SubscriptionType};
///
/// let source = PulsarSubscription::new("orders", "workers")
///     .subscription_type(SubscriptionType::Shared)
///     .ack_timeout(Duration::from_secs(30));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct PulsarSubscription {
    pub(crate) topics: Topics,
    pub(crate) subscription: String,
    pub(crate) sub_type: SubscriptionType,
    pub(crate) ack_timeout: Option<Duration>,
    pub(crate) batch_wait: Duration,
    /// What the registration declared about its retries, taken in before the consumer opens.
    retry: RetryDeclaration,
}

impl PulsarSubscription {
    /// Subscribes `subscription` to `topic` (a bare name, a `tenant/namespace/topic` triple,
    /// or a fully qualified name).
    pub fn new(topic: impl Into<String>, subscription: impl Into<String>) -> Self {
        Self {
            topics: Topics::List(vec![topic.into()]),
            subscription: subscription.into(),
            sub_type: SubscriptionType::default(),
            ack_timeout: None,
            batch_wait: DEFAULT_BATCH_WAIT,
            retry: RetryDeclaration::new(),
        }
    }

    /// Subscribes to every topic in the list.
    pub fn topics<I, S>(topics: I, subscription: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            topics: Topics::List(topics.into_iter().map(Into::into).collect()),
            ..Self::new(String::new(), subscription)
        }
    }

    /// Subscribes to every topic in the lookup namespace whose name matches `pattern` (a
    /// regular expression, validated on subscribe).
    pub fn pattern(pattern: impl Into<String>, subscription: impl Into<String>) -> Self {
        Self {
            topics: Topics::Pattern(pattern.into()),
            ..Self::new(String::new(), subscription)
        }
    }

    /// Sets the subscription type. Defaults to [`SubscriptionType::Shared`].
    pub fn subscription_type(mut self, sub_type: SubscriptionType) -> Self {
        self.sub_type = sub_type;
        self
    }

    /// Redelivers messages that stay unacknowledged longer than `timeout`.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = Some(timeout);
        self
    }

    /// Caps how long a partial batch waits for more deliveries after its first one.
    ///
    /// Only a batch handler observes this: the client hands over one delivery at a time, so a
    /// `batch(n)` registration on this subscription assembles its batches here, and a batch
    /// closes when it holds `n` deliveries or when this has elapsed, whichever comes first.
    /// Defaults to 10 ms; raising it trades latency for fuller batches on a sparse topic.
    pub fn batch_wait(mut self, wait: Duration) -> Self {
        self.batch_wait = wait;
        self
    }

    /// The subscription name.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// The name the framework reports for a subscription on this descriptor: the topic when
    /// there is exactly one, the subscription name otherwise, since a list and a pattern have no
    /// single topic to name. Both [`SubscriptionSource`] impls read it, so a handler is reported
    /// under one name whichever broker it mounted on.
    fn source_name(&self) -> &str {
        match &self.topics {
            Topics::List(topics) if topics.len() == 1 => &topics[0],
            _ => &self.subscription,
        }
    }

    /// The consumer's dead-letter policy, resolved from what the registration declared.
    ///
    /// Pulsar's policy is a limit and a topic together - the client counts redeliveries and,
    /// past the limit, produces the message to that topic and acknowledges the original - so
    /// half a declaration is not a policy it can apply. A registration that declares one half
    /// refuses to start rather than running with a cap nobody enforces.
    pub(crate) fn dead_letter_policy(&self) -> Result<Option<DeadLetterRoute>, PulsarError> {
        match (self.retry.max_attempts(), self.retry.dead_letter()) {
            (Some(attempts), Some(topic)) => {
                let _ = PulsarTopic::parse(topic)?;
                Ok(Some(DeadLetterRoute {
                    topic: topic.to_owned(),
                    max_deliveries: attempts.get(),
                }))
            }
            (None, None) => Ok(None),
            // The mount chain lets the two steps be written apart, so the pairing cannot be a
            // compile error here; the startup refusal is what keeps a half declaration from
            // looking applied.
            (Some(_), None) => Err(PulsarError::Invalid(format!(
                "subscription '{}' declares max_attempts with no dead_letter: Pulsar applies a \
                 delivery limit only together with the topic a spent delivery moves to, so \
                 declare dead_letter(..) beside it or drop both",
                self.subscription
            ))),
            (None, Some(topic)) => Err(PulsarError::Invalid(format!(
                "subscription '{}' declares dead_letter('{topic}') with no max_attempts: Pulsar \
                 moves a delivery to the dead-letter topic by counting redeliveries, so declare \
                 max_attempts(..) beside it or drop both",
                self.subscription
            ))),
        }
    }

    pub(crate) fn display_topic(&self) -> String {
        match &self.topics {
            Topics::List(topics) => topics.join(","),
            Topics::Pattern(pattern) => pattern.clone(),
        }
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), PulsarError> {
        if self.subscription.is_empty() {
            return Err(PulsarError::Invalid(
                "subscription name must be non-empty".into(),
            ));
        }
        match &self.topics {
            Topics::List(topics) => {
                if topics.is_empty() || topics.iter().any(String::is_empty) {
                    return Err(PulsarError::Invalid("topics must be non-empty".into()));
                }
                for topic in topics {
                    let _ = PulsarTopic::parse(topic)?;
                }
            }
            Topics::Pattern(pattern) => {
                regex::Regex::new(pattern).map_err(|e| {
                    PulsarError::Invalid(format!("invalid topic pattern '{pattern}': {e}"))
                })?;
            }
        }
        self.dead_letter_policy()?;
        Ok(())
    }
}

/// The Pulsar client moves a spent delivery itself: it counts the redeliveries of a message and,
/// past the consumer's limit, produces it to the dead-letter topic and acknowledges the original.
/// Nothing is republished from the service, on any of the three addressing forms, so
/// `out_retry(..)` over this descriptor is a compile error and the registration's declaration
/// reaches the consumer instead.
impl SubscriptionSource<ConnectedPulsarBroker> for PulsarSubscription {
    type Subscriber = PulsarSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.source_name()
    }

    async fn subscribe(
        self,
        connected: &ConnectedPulsarBroker,
    ) -> Result<PulsarSubscriber, PulsarError> {
        connected.subscribe_descriptor(self).await
    }

    /// Records the declaration; the consumer is built from it in `subscribe`, where the
    /// connection exists.
    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.retry = declaration.clone();
        self
    }
}

/// The descriptor is a source for the in-process stand-in too, so the declaration a service
/// ships is the one its tests run: the same `#[subscriber(PulsarSubscription::new(..))]` mounts
/// on [`PulsarTestBroker`](crate::testing::PulsarTestBroker) under a
/// [`TestApp`](ruststream::testing::TestApp), with no second descriptor and nothing to change at
/// the mount site.
///
/// All three forms route: one topic, the list of
/// [`topics`](PulsarSubscription::topics), and the regular expression of
/// [`pattern`](PulsarSubscription::pattern), which the stand-in matches against every topic
/// published to, including topics that first appear after the subscription opened. The
/// [`subscription_type`](PulsarSubscription::subscription_type) decides which consumer of the
/// subscription takes a message, so competing consumers split a stream in process as they do in
/// production. So does the registration's declaration: the stand-in counts a message's
/// redeliveries and moves it to the declared dead-letter topic at the limit, the way the client
/// does. What is left to the server - the ack timeout and redelivery timing - the
/// [`testing` module docs](crate::testing) name.
#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedPulsarTestBroker> for PulsarSubscription {
    type Subscriber = crate::testing::PulsarTestSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.source_name()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedPulsarTestBroker,
    ) -> Result<Self::Subscriber, PulsarError> {
        connected.subscribe_descriptor(self).await
    }

    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.retry = declaration.clone();
        self
    }
}

#[cfg(test)]
mod tests {
    use ruststream::nonzero;

    use super::*;

    /// Takes the declaration in the way the runtime does. Spelled out because the descriptor is
    /// a source for two brokers, so the bare method call names no impl.
    fn declared(
        subscription: PulsarSubscription,
        declaration: &RetryDeclaration,
    ) -> PulsarSubscription {
        <PulsarSubscription as SubscriptionSource<ConnectedPulsarBroker>>::declare_retry(
            subscription,
            declaration,
        )
    }

    #[test]
    fn empty_subscription_is_rejected_before_io() {
        assert!(matches!(
            PulsarSubscription::new("orders", "").validate(),
            Err(PulsarError::Invalid(_))
        ));
    }

    #[test]
    fn malformed_topics_are_rejected_before_io() {
        assert!(matches!(
            PulsarSubscription::new("a/b", "workers").validate(),
            Err(PulsarError::Invalid(_))
        ));
    }

    /// Half a declaration is not a policy the client can apply, so it never reaches the broker.
    #[test]
    fn half_a_retry_declaration_is_rejected_before_io() {
        let capped = declared(
            PulsarSubscription::new("orders", "workers"),
            &RetryDeclaration::new().with_max_attempts(nonzero!(3u32)),
        );
        let message = match capped.validate() {
            Err(PulsarError::Invalid(message)) => message,
            other => panic!("a cap with no destination must be refused, got {other:?}"),
        };
        assert!(message.contains("dead_letter"), "{message}");

        let addressed = declared(
            PulsarSubscription::new("orders", "workers"),
            &RetryDeclaration::new().with_dead_letter("orders-dlq"),
        );
        let message = match addressed.validate() {
            Err(PulsarError::Invalid(message)) => message,
            other => panic!("a destination with no cap must be refused, got {other:?}"),
        };
        assert!(message.contains("max_attempts"), "{message}");
    }

    /// Both halves become the consumer's own policy, with the limit counted in deliveries.
    #[test]
    fn a_whole_declaration_becomes_the_consumers_policy() {
        let policy = declared(
            PulsarSubscription::new("orders", "workers"),
            &RetryDeclaration::new()
                .with_max_attempts(nonzero!(3u32))
                .with_dead_letter("orders-dlq"),
        );
        assert_eq!(
            policy.dead_letter_policy().expect("both halves declared"),
            Some(DeadLetterRoute {
                topic: "orders-dlq".to_owned(),
                max_deliveries: 3,
            })
        );
    }

    #[test]
    fn malformed_patterns_are_rejected_before_io() {
        assert!(matches!(
            PulsarSubscription::pattern("orders-(", "workers").validate(),
            Err(PulsarError::Invalid(_))
        ));
    }
}
