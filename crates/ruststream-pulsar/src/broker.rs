//! The broker ladder: [`PulsarBroker`] -> [`ConnectedPulsarBroker`].
//!
//! Construction is synchronous and I/O-free; the client dials in the consuming
//! [`Broker::connect`], and the connected form holds the live client directly. One shared cell
//! remains so publishers can be handed out while the application is still being assembled,
//! before `connect` runs.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pulsar::{Authentication, Pulsar, TokioExecutor};
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::{Mutex, OnceCell};

use crate::error::{PulsarError, box_err};
use crate::publisher::{PulsarProducer, PulsarPublish, PulsarPublisher};
use crate::subscriber::PulsarSubscriber;
use crate::subscription::{DEFAULT_SUBSCRIPTION, PulsarSubscription};

/// The live client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: the client handle is `Clone` and would happily
/// reconnect after our typed shutdown, and publishers may be handed out before `connect` and
/// outlive `shutdown` (aliasing) - so the closed state is an explicit flag a stale handle
/// trips over instead of silently succeeding.
pub(crate) struct Core {
    pub(crate) client: Pulsar<TokioExecutor>,
    pub(crate) closed: AtomicBool,
    /// Per-topic producers, shared by every publisher handle so shutdown can close them.
    pub(crate) producers: Mutex<HashMap<String, Arc<Mutex<PulsarProducer>>>>,
}

impl Core {
    pub(crate) fn ensure_open(&self) -> Result<(), PulsarError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PulsarError::NotConnected);
        }
        Ok(())
    }
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Arc<Core>>>;

/// An Apache Pulsar broker for the `RustStream` messaging framework.
///
/// `new` is synchronous and records only configuration; the runtime dials once at startup via
/// the consuming [`Broker::connect`]. That is what lets a service compose with the synchronous
/// `#[ruststream::app]` builder.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::PulsarBroker;
///
/// let broker = PulsarBroker::new("pulsar://localhost:6650");
/// let secured = PulsarBroker::new("pulsar+ssl://broker:6651").token("jwt...");
/// # let _ = (broker, secured);
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct PulsarBroker {
    url: String,
    token: Option<String>,
    // Shared with publishers handed out before connect; the consuming connect fills it.
    cell: CoreCell,
}

impl PulsarBroker {
    /// Records the service URL (`pulsar://` or `pulsar+ssl://`). No I/O.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Authenticates with a JWT token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> PulsarPublisher {
        PulsarPublisher::new(Arc::clone(&self.cell))
    }
}

impl Broker for PulsarBroker {
    type Error = PulsarError;
    type Connected = ConnectedPulsarBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                let mut builder = Pulsar::builder(self.url.clone(), TokioExecutor);
                if let Some(token) = &self.token {
                    builder = builder.with_auth(Authentication {
                        name: "token".to_owned(),
                        data: token.clone().into_bytes(),
                    });
                }
                let client = builder
                    .build()
                    .await
                    .map_err(|e| PulsarError::Connect(box_err(e)))?;
                Ok::<_, PulsarError>(Arc::new(Core {
                    client,
                    closed: AtomicBool::new(false),
                    producers: Mutex::new(HashMap::new()),
                }))
            })
            .await?
            .clone();
        Ok(ConnectedPulsarBroker {
            core,
            cell: self.cell,
        })
    }
}

impl DescribeServer for PulsarBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(
            self.url
                .trim_start_matches("pulsar+ssl://")
                .trim_start_matches("pulsar://"),
            "pulsar",
        )
    }
}

/// The typed witness that `connect` succeeded: holds the live client directly.
#[derive(Debug)]
pub struct ConnectedPulsarBroker {
    pub(crate) core: Arc<Core>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl ConnectedPulsarBroker {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as
    /// the early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> PulsarPublisher {
        PulsarPublisher::new(Arc::clone(&self.cell))
    }

    /// Opens the subscription described by `descriptor`.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError`] when the descriptor is invalid, the consumer cannot be created,
    /// or the broker is shut down.
    pub async fn subscribe_descriptor(
        &self,
        descriptor: PulsarSubscription,
    ) -> Result<PulsarSubscriber, PulsarError> {
        descriptor.validate()?;
        self.core.ensure_open()?;
        PulsarSubscriber::open(&self.core, descriptor).await
    }
}

impl ConnectedBroker for ConnectedPulsarBroker {
    type Error = PulsarError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        // The client has no close of its own; producers are the handles holding broker-side
        // state worth a clean goodbye.
        let producers: Vec<_> = {
            let mut map = self.core.producers.lock().await;
            map.drain().map(|(_, producer)| producer).collect()
        };
        for producer in producers {
            let mut producer = producer.lock().await;
            if let Err(err) = Box::pin(producer.close()).await {
                tracing::debug!(error = %err, "pulsar producer close failed");
            }
        }
        Ok(())
    }
}

impl Subscribe for ConnectedPulsarBroker {
    type Subscriber = PulsarSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        // By-name subscriptions share one durable subscription, matching competing-consumer
        // expectations. The stand-in reads the same constant, so the two cannot drift apart.
        self.subscribe_descriptor(PulsarSubscription::new(name, DEFAULT_SUBSCRIPTION))
            .await
    }
}

impl DefaultPublish for ConnectedPulsarBroker {
    type Policy = PulsarPublish;
}
