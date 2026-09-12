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
use crate::subscription::PulsarSubscription;

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

/// The address a client dials, taken from the service URL: the host and its port, and nothing
/// else.
///
/// The generated `AsyncAPI` document is published and shared, so a `user:password@` in the URL
/// must not reach it. The userinfo is cut at the LAST `@` of the authority, because a password
/// may contain one; the path and query are cut first, so an `@` further along the URL cannot be
/// mistaken for the delimiter.
fn server_address(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme
        .split_once(['/', '?', '#'])
        .map_or(after_scheme, |(authority, _)| authority);
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host)
}

impl DescribeServer for PulsarBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(server_address(&self.url), "pulsar")
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
        // By-name subscriptions share one durable subscription named after the service-wide
        // convention "ruststream", matching competing-consumer expectations.
        self.subscribe_descriptor(PulsarSubscription::new(name, "ruststream"))
            .await
    }
}

impl DefaultPublish for ConnectedPulsarBroker {
    type Policy = PulsarPublish;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every URL shape this broker accepts, reduced to the address a client dials.
    #[test]
    fn the_description_carries_the_address_alone() {
        for (url, address) in [
            ("pulsar://broker:6650", "broker:6650"),
            ("pulsar+ssl://broker:6651", "broker:6651"),
            ("pulsar://broker", "broker"),
            ("pulsar://broker:6650/", "broker:6650"),
            ("pulsar://admin:s3cret@broker:6650", "broker:6650"),
            ("pulsar+ssl://admin:s3cret@broker", "broker"),
            // A password may hold an `@`, so the userinfo ends at the last one.
            ("pulsar://admin:p@ssw0rd@broker:6650", "broker:6650"),
            // Pulsar takes a comma-separated broker list, which is the address as it stands.
            ("pulsar://one:6650,two:6650", "one:6650,two:6650"),
        ] {
            assert_eq!(
                PulsarBroker::new(url).describe_server().host.as_deref(),
                Some(address),
                "url {url}",
            );
        }
    }

    /// The generated document is published and shared, so what a service put in its URL to
    /// authenticate must not be in it.
    #[test]
    fn credentials_never_reach_the_description() {
        let described = PulsarBroker::new("pulsar://admin:s3cret@broker:6650").describe_server();
        let host = described.host.expect("a URL with a host describes one");
        assert!(!host.contains('@'), "userinfo survived in {host:?}");
        assert!(!host.contains("admin"), "user name survived in {host:?}");
        assert!(!host.contains("s3cret"), "password survived in {host:?}");
    }
}
