//! [`PulsarTestBroker`]: the in-process transport and its connected form.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::PulsarError;
use crate::publisher::PulsarPublishExt;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::PulsarTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::Headers) {
        self.router
            .publish(name, payload, headers, self.coordinator());
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

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedPulsarTestBroker { state: self.state })
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
}

impl ConnectedBroker for ConnectedPulsarTestBroker {
    type Error = PulsarError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

impl Subscribe for ConnectedPulsarTestBroker {
    type Subscriber = PulsarTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        Ok(PulsarTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        ))
    }
}

impl TestableBroker for ConnectedPulsarTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
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
#[derive(Debug, Clone)]
pub struct PulsarTestPublisher {
    state: Arc<TestState>,
}

// Mirrors the real publisher's arguments, so a tested handler runs the chain it will in production.
impl PulsarPublishExt for PulsarTestPublisher {}

impl Publisher for PulsarTestPublisher {
    type Error = PulsarError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        Ok(())
    }
}

/// The publish policy for [`PulsarTestPublisher`], mirroring
/// [`PulsarPublish`](crate::PulsarPublish) on the real broker.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::testing::PulsarTestPublish;
///
/// let policy = PulsarTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct PulsarTestPublish;

impl PublishPolicy<ConnectedPulsarTestBroker> for PulsarTestPublish {
    type Live = PulsarTestPublisher;

    async fn pair(self, connected: &ConnectedPulsarTestBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedPulsarTestBroker {
    type Policy = PulsarTestPublish;
}
