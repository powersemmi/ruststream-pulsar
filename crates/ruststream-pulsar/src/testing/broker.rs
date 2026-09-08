//! [`PulsarTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, Publisher, RawMessage, Subscribe,
};

use crate::error::PulsarError;
use crate::publisher::PulsarPublishExt;
use crate::subscription::{PulsarSubscription, Topics};
use crate::testing::router::{AddressRouter, Route};
use crate::testing::subscriber::PulsarTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    /// Set by `shutdown`. The ladder makes owner-side misuse a compile error, but handles that
    /// alias the transport - a publisher taken before the shutdown, a clone of the connected
    /// form - outlive it and must report the closure instead of routing into a dead router, as
    /// they do against a server.
    closed: AtomicBool,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// `Ok` while the transport is live, [`PulsarError::NotConnected`] once it has shut down -
    /// the error the real broker's own handles report then.
    fn ensure_open(&self) -> Result<(), PulsarError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PulsarError::NotConnected);
        }
        Ok(())
    }

    /// Closes the transport: every aliasing handle reports the closure from here on.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn publish(
        &self,
        name: &str,
        payload: Bytes,
        headers: ruststream::HeaderMap,
    ) -> Result<(), PulsarError> {
        self.ensure_open()?;
        self.router
            .publish(name, payload, headers, self.coordinator());
        Ok(())
    }
}

/// An in-process stand-in for [`PulsarBroker`](crate::PulsarBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::testing::PulsarTestBroker;
///
/// let broker = PulsarTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct PulsarTestBroker {
    state: Arc<TestState>,
}

impl PulsarTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> PulsarTestPublisher {
        PulsarTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl Broker for PulsarTestBroker {
    type Error = PulsarError;
    type Connected = ConnectedPulsarTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedPulsarTestBroker { state: self.state }))
    }
}

/// The connected form of [`PulsarTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedPulsarTestBroker {
    state: Arc<TestState>,
}

impl ConnectedPulsarTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> PulsarTestPublisher {
        PulsarTestPublisher {
            state: Arc::clone(&self.state),
        }
    }

    /// Opens the subscription `descriptor` describes, in process.
    ///
    /// Mirrors [`ConnectedPulsarBroker::subscribe_descriptor`](crate::ConnectedPulsarBroker::subscribe_descriptor),
    /// which is what lets the descriptor a service mounts in production mount here too: it is
    /// validated first, so a malformed topic name, or a pattern that is not a regular
    /// expression, fails at startup exactly as it does against a server, and its addressing
    /// half - the topic, the topic list, or the pattern - becomes the subscription's route. The
    /// settings a Pulsar server owns carry no behaviour here; the [module docs](crate::testing)
    /// list them.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Invalid`] when the descriptor carries no subscription name, no
    /// topic, a malformed topic name, or a pattern that is not a regular expression.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Broker;
    /// use ruststream_pulsar::PulsarSubscription;
    /// use ruststream_pulsar::testing::PulsarTestBroker;
    ///
    /// # async fn demo() -> Result<(), ruststream_pulsar::PulsarError> {
    /// let connected = PulsarTestBroker::new().connect().await?;
    /// let subscriber = connected
    ///     .subscribe_descriptor(PulsarSubscription::new("orders", "workers"))
    ///     .await?;
    /// # let _ = subscriber;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe_descriptor(
        &self,
        descriptor: PulsarSubscription,
    ) -> impl Future<Output = Result<PulsarTestSubscriber, PulsarError>> {
        ready(self.open(descriptor))
    }

    /// The synchronous body of [`Self::subscribe_descriptor`]: registering a subscription is a
    /// map insertion, so nothing here awaits.
    fn open(&self, descriptor: PulsarSubscription) -> Result<PulsarTestSubscriber, PulsarError> {
        descriptor.validate()?;
        self.state.ensure_open()?;
        let route = match descriptor.topics {
            Topics::List(topics) => Route::Topics(topics),
            // `validate` already compiled the pattern, so this cannot fail; it is mapped rather
            // than unwrapped because the compiled form is what the router matches with.
            Topics::Pattern(pattern) => Route::pattern(&pattern).map_err(|err| {
                PulsarError::Invalid(format!("invalid topic pattern '{pattern}': {err}"))
            })?,
        };
        Ok(self.open_route(route))
    }

    /// Registers `route` and wraps it in the subscriber the harness drives.
    fn open_route(&self, route: Route) -> PulsarTestSubscriber {
        let id = self.state.router.subscribe(route);
        PulsarTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            self.state.coordinator().cloned(),
        )
    }
}

impl ConnectedBroker for ConnectedPulsarTestBroker {
    type Error = PulsarError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.close();
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedPulsarTestBroker {
    type Subscriber = PulsarTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(
            self.state
                .ensure_open()
                .map(|()| self.open_route(Route::topic(name.to_owned()))),
        )
    }
}

impl TestableBroker for ConnectedPulsarTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        // An injection stands in for an external producer and the trait gives it no way to
        // report; against a shut-down transport it is dropped, which is what a producer talking
        // to a closed broker achieves in effect.
        let _ = self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedPulsarTestBroker);

/// Publisher for the in-process broker.
///
/// Usable from the moment the broker exists, since there is no connection to wait for, and until
/// the transport shuts down; afterwards every publish reports
/// [`PulsarError::NotConnected`](crate::PulsarError::NotConnected), as the real publisher does
/// against a closed connection.
#[derive(Debug, Clone)]
pub struct PulsarTestPublisher {
    state: Arc<TestState>,
}

// Mirrors the real publisher's arguments, so a tested handler runs the chain it will in production.
impl PulsarPublishExt for PulsarTestPublisher {}

impl Publisher for PulsarTestPublisher {
    type Error = PulsarError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        ))
    }
}

/// The stand-in's default is the crate's own [`PulsarPublish`](crate::PulsarPublish), which
/// pairs against this broker too, so a handler replying through the broker default replies
/// through the same declaration it will in production.
impl DefaultPublish for ConnectedPulsarTestBroker {
    type Policy = crate::PulsarPublish;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The framework's `harness::lifecycle` asserts this against every broker; these two name
    /// the error, which the suite only requires to be an error.
    #[tokio::test]
    async fn a_publisher_outliving_the_transport_reports_the_closure() {
        let broker = PulsarTestBroker::new();
        let early = broker.publisher();
        let connected = broker.connect().await.expect("the stand-in connects");
        let live = connected.publisher();
        connected.shutdown().await.expect("shutdown");

        for publisher in [early, live] {
            let err = publisher
                .publish(OutgoingMessage::new("orders", b"late".as_slice()))
                .await
                .expect_err("a publish into a shut-down transport must not report success");
            assert!(matches!(err, PulsarError::NotConnected));
        }
    }

    #[tokio::test]
    async fn subscribing_after_shutdown_reports_the_closure() {
        let connected = PulsarTestBroker::new()
            .connect()
            .await
            .expect("the stand-in connects");
        let alias = connected.clone();
        connected.shutdown().await.expect("shutdown");

        let by_name = Subscribe::subscribe(&alias, "orders")
            .await
            .expect_err("a subscription on a shut-down transport must not open");
        assert!(matches!(by_name, PulsarError::NotConnected));

        let by_descriptor = alias
            .subscribe_descriptor(PulsarSubscription::new("orders", "workers"))
            .await
            .expect_err("a subscription on a shut-down transport must not open");
        assert!(matches!(by_descriptor, PulsarError::NotConnected));
    }
}
