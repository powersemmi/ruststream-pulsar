//! [`PulsarSubscription`]: the subscription descriptor.
//!
//! The subscription type is an enum, not a set of sibling flags, so combinations that do not
//! exist are unrepresentable; the dead-letter policy and the ack timeout are consumer-side
//! settings the product owns.

use std::time::Duration;

use ruststream::SubscriptionSource;

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

/// The consumer-side dead-letter policy: after `max_deliveries` redeliveries the broker routes
/// the message to the dead-letter topic.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::DeadLetter;
///
/// let policy = DeadLetter::new("orders-dlq").max_deliveries(5);
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct DeadLetter {
    pub(crate) topic: String,
    pub(crate) max_deliveries: usize,
}

impl DeadLetter {
    /// Routes exhausted messages to `topic` after the default of 5 deliveries.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            max_deliveries: 5,
        }
    }

    /// Sets the delivery-attempt limit.
    pub fn max_deliveries(mut self, max: usize) -> Self {
        self.max_deliveries = max;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Topics {
    List(Vec<String>),
    Pattern(String),
}

/// How long a partial page waits for more deliveries before the handler sees it, unless the
/// descriptor names another value.
///
/// Short, because the cost of waiting is latency on a page that is already useful; a service
/// trading latency for fuller pages raises it with [`PulsarSubscription::page_wait`].
pub(crate) const DEFAULT_PAGE_WAIT: Duration = Duration::from_millis(10);

/// A subscription descriptor for one Pulsar subscription over one or more topics.
///
/// Where the subscription starts reading is not a descriptor option: it is the framework's
/// `start_at(..)` clause over [`PulsarPosition`](crate::PulsarPosition), which the `Seekable`
/// capability backs.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
///
/// ```
/// use std::time::Duration;
/// use ruststream_pulsar::{DeadLetter, PulsarSubscription, SubscriptionType};
///
/// let source = PulsarSubscription::new("orders", "workers")
///     .subscription_type(SubscriptionType::Shared)
///     .dead_letter(DeadLetter::new("orders-dlq").max_deliveries(5))
///     .ack_timeout(Duration::from_secs(30));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct PulsarSubscription {
    pub(crate) topics: Topics,
    pub(crate) subscription: String,
    pub(crate) sub_type: SubscriptionType,
    pub(crate) dead_letter: Option<DeadLetter>,
    pub(crate) ack_timeout: Option<Duration>,
    pub(crate) page_wait: Duration,
}

impl PulsarSubscription {
    /// Subscribes `subscription` to `topic` (a bare name, a `tenant/namespace/topic` triple,
    /// or a fully qualified name).
    pub fn new(topic: impl Into<String>, subscription: impl Into<String>) -> Self {
        Self {
            topics: Topics::List(vec![topic.into()]),
            subscription: subscription.into(),
            sub_type: SubscriptionType::default(),
            dead_letter: None,
            ack_timeout: None,
            page_wait: DEFAULT_PAGE_WAIT,
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

    /// Sets the consumer-side dead-letter policy.
    pub fn dead_letter(mut self, dead_letter: DeadLetter) -> Self {
        self.dead_letter = Some(dead_letter);
        self
    }

    /// Redelivers messages that stay unacknowledged longer than `timeout`.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = Some(timeout);
        self
    }

    /// Caps how long a partial page waits for more deliveries after its first one.
    ///
    /// Only a page handler observes this: the client hands over one delivery at a time, so a
    /// `batch(n)` registration on this subscription assembles its pages here, and a page closes
    /// when it holds `n` deliveries or when this has elapsed, whichever comes first. Defaults to
    /// 10 ms; raising it trades latency for fuller pages on a sparse topic.
    pub fn page_wait(mut self, wait: Duration) -> Self {
        self.page_wait = wait;
        self
    }

    /// The subscription name.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.subscription
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
        Ok(())
    }
}

impl SubscriptionSource<ConnectedPulsarBroker> for PulsarSubscription {
    type Subscriber = PulsarSubscriber;

    fn name(&self) -> &str {
        match &self.topics {
            Topics::List(topics) if topics.len() == 1 => &topics[0],
            _ => &self.subscription,
        }
    }

    async fn subscribe(
        self,
        connected: &ConnectedPulsarBroker,
    ) -> Result<PulsarSubscriber, PulsarError> {
        connected.subscribe_descriptor(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn malformed_patterns_are_rejected_before_io() {
        assert!(matches!(
            PulsarSubscription::pattern("orders-(", "workers").validate(),
            Err(PulsarError::Invalid(_))
        ));
    }
}
