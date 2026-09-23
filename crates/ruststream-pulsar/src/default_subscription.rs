//! Whether a broker names the subscription a bare topic name joins, carried in its type.
//!
//! `#[subscriber("orders")]` names a topic and no subscription, and Pulsar has no anonymous
//! consumer, so a subscription by bare topic name needs a name from the broker. A broker that
//! sets one with [`PulsarBroker::default_subscription`](crate::PulsarBroker::default_subscription)
//! changes type to carry it, and only that type opens a bare topic name: mounting one on a broker
//! without it does not compile, and the error names the fix.

/// The broker form that names no default subscription: what `PulsarBroker::new` returns.
///
/// Every [`PulsarSubscription`](crate::PulsarSubscription) descriptor mounts on it; a bare topic
/// name does not, because there is no subscription for it to join.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::{NoDefaultSubscription, PulsarBroker};
///
/// let broker: PulsarBroker<NoDefaultSubscription> = PulsarBroker::new("pulsar://localhost:6650");
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoDefaultSubscription;

/// The broker form that names the durable subscription a bare topic name joins: what
/// [`default_subscription`](crate::PulsarBroker::default_subscription) returns.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::{DefaultSubscription, PulsarBroker};
///
/// let broker: PulsarBroker<DefaultSubscription> =
///     PulsarBroker::new("pulsar://localhost:6650").default_subscription("orders-worker");
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultSubscription(String);

impl DefaultSubscription {
    pub(crate) fn new(subscription: impl Into<String>) -> Self {
        Self(subscription.into())
    }
}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for DefaultSubscription {}

/// A broker form that names the subscription a bare topic name joins; only
/// [`DefaultSubscription`] is one.
///
/// It is the bound a bare topic name meets when it mounts, so a broker without a default
/// subscription is a compile error at the mount, naming `Broker` and the two fixes. Sealed: the
/// set of forms is this crate's.
#[diagnostic::on_unimplemented(
    message = "`{Broker}` names no default subscription, so a subscription by bare topic name \
               has no subscription to join",
    label = "this broker names no default subscription",
    note = "name one on the broker: `.default_subscription(\"orders-worker\")` after `::new(..)`",
    note = "or mount a descriptor that names its own: \
            `#[subscriber(PulsarSubscription::new(\"orders\", \"orders-worker\"))]`"
)]
pub trait NamesDefaultSubscription<Broker>: sealed::Sealed {
    /// The subscription a bare topic name joins.
    fn subscription(&self) -> &str;
}

impl<Broker> NamesDefaultSubscription<Broker> for DefaultSubscription {
    fn subscription(&self) -> &str {
        &self.0
    }
}
