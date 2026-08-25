//! [`PulsarTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::PulsarError;
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
}

impl ConnectedBroker for ConnectedPulsarTestBroker {
    type Error = PulsarError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedPulsarTestBroker {
    type Subscriber = PulsarTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        ready(Ok(PulsarTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )))
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

impl Publisher for PulsarTestPublisher {
    type Error = PulsarError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
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

    fn pair(
        self,
        connected: &ConnectedPulsarTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedPulsarTestBroker {
    type Policy = PulsarTestPublish;
}
