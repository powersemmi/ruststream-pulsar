//! The broker ladder: [`PulsarBroker`] -> [`ConnectedPulsarBroker`].
//!
//! Construction is synchronous and I/O-free; the client dials in the consuming
//! [`Broker::connect`], and the connected form holds the live client directly. One shared cell
//! remains so publishers can be handed out while the application is still being assembled,
//! before `connect` runs.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pulsar::{Authentication, OperationRetryOptions, Pulsar, TokioExecutor};
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    Broker, BrokerMoves, ConnectedBroker, DeclareRetryError, DefaultPublish, DescribeServer,
    RetryDeclaration, ServerSpec, Subscribe,
};
#[cfg(feature = "testing")]
use ruststream::{BytesMut, OutgoingMessage, RawMessage};
use tokio::sync::{Mutex, OnceCell};

use crate::error::{PulsarError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::{self, Bus, Route, Subscriptions};
use crate::publisher::{PulsarProducer, PulsarPublish, PulsarPublisher};
use crate::subscriber::PulsarSubscriber;
use crate::subscription::{DeclaredRetries, PulsarSubscription};

/// How long the client keeps asking for an operation a server has not accepted yet.
///
/// The client retries an answer that says "not now" - a broker that is still coming up, a
/// producer or a consumer the topic already has - rather than reporting it, and by default it
/// retries for ever, five seconds apart. That is what waits out a broker restart, and it is also
/// what makes a subscription a server refuses (an
/// [`Exclusive`](crate::SubscriptionType::Exclusive) one another consumer holds) a call that
/// never returns. [`attempts`](Self::attempts) puts a number on it.
///
/// The bound covers every retried operation of the connection, the consumer's own reconnect
/// included, so a service that shortens it trades waiting out an outage for hearing about it.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream::nonzero;
/// use ruststream_pulsar::{OperationRetries, PulsarBroker};
///
/// // Three tries a second apart, then the call reports what the server said.
/// let broker = PulsarBroker::new("pulsar://localhost:6650").operation_retries(
///     OperationRetries::attempts(nonzero!(3u32)).delay(Duration::from_secs(1)),
/// );
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct OperationRetries {
    attempts: Option<NonZeroU32>,
    delay: Duration,
    timeout: Duration,
}

/// The client's own numbers, which apply unless a step below names another.
const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

impl OperationRetries {
    /// Keeps asking for ever, which is the client's own behaviour.
    pub const fn unbounded() -> Self {
        Self {
            attempts: None,
            delay: DEFAULT_RETRY_DELAY,
            timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }

    /// Gives up after `attempts` tries and reports what the server answered.
    ///
    /// One attempt means the first answer is the final one.
    pub const fn attempts(attempts: NonZeroU32) -> Self {
        Self {
            attempts: Some(attempts),
            ..Self::unbounded()
        }
    }

    /// Waits `delay` between tries. Defaults to five seconds.
    pub const fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Gives one try `timeout` to be answered at all. Defaults to thirty seconds.
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl Default for OperationRetries {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl From<OperationRetries> for OperationRetryOptions {
    /// The client counts the retries that follow the first try, so a bound of `n` tries is
    /// `n - 1` retries.
    fn from(retries: OperationRetries) -> Self {
        Self {
            operation_timeout: retries.timeout,
            retry_delay: retries.delay,
            max_retries: retries.attempts.map(|attempts| attempts.get() - 1),
        }
    }
}

/// What a connected broker and every handle derived from it speak over: the client, or, under
/// the `testing` feature, the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the client itself and every `match`
/// on it is irrefutable: a production build carries no second transport and no branch to it.
pub(crate) enum Transport {
    Client(Pulsar<TokioExecutor>),
    #[cfg(feature = "testing")]
    InProcess(Arc<Bus>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives
// the transport exactly the size of the client it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Transport>() == size_of::<Pulsar<TokioExecutor>>());

/// The live client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: the client handle is `Clone` and would happily
/// reconnect after our typed shutdown, and publishers may be handed out before `connect` and
/// outlive `shutdown` (aliasing) - so the closed state is an explicit flag a stale handle
/// trips over instead of silently succeeding.
pub(crate) struct Core {
    pub(crate) transport: Transport,
    pub(crate) closed: AtomicBool,
    /// Per-topic producers, shared by every publisher handle so shutdown can close them.
    pub(crate) producers: Mutex<HashMap<String, Arc<Mutex<PulsarProducer>>>>,
    /// The subscription a bare topic name joins; see [`PulsarBroker::default_subscription`].
    default_subscription: Option<String>,
    /// What registrations mounted by a bare topic name declared about their retries.
    pub(crate) declared_retries: DeclaredRetries,
    /// The subscriptions this connection opened, which the test harness's routing answer reads
    /// on either transport.
    #[cfg(feature = "testing")]
    opened: Subscriptions,
}

impl Core {
    fn new(transport: Transport, default_subscription: Option<String>) -> Self {
        Self {
            transport,
            closed: AtomicBool::new(false),
            producers: Mutex::new(HashMap::new()),
            default_subscription,
            declared_retries: DeclaredRetries::default(),
            #[cfg(feature = "testing")]
            opened: Subscriptions::default(),
        }
    }

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
    default_subscription: Option<String>,
    // None leaves the client's own retry behaviour in place, which is unbounded.
    retries: Option<OperationRetries>,
    // Shared with publishers handed out before connect; the consuming connect fills it.
    cell: CoreCell,
}

impl PulsarBroker {
    /// Records the service URL (`pulsar://` or `pulsar+ssl://`). No I/O.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: None,
            default_subscription: None,
            retries: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Authenticates with a JWT token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// The durable subscription a subscription by bare topic name joins.
    ///
    /// `#[subscriber("orders")]` names a topic and no subscription, and Pulsar has no anonymous
    /// consumer, so the name comes from here. A subscription name is the cursor on the server:
    /// every consumer under it shares one backlog, the handlers of this service and those of any
    /// other service that names the same one. Name it after the service that owns the cursor.
    /// A bare topic name on a broker without it fails at startup with
    /// [`PulsarError::Invalid`] naming this setting; a [`PulsarSubscription`] names its own
    /// subscription and needs none.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_pulsar::PulsarBroker;
    ///
    /// let broker =
    ///     PulsarBroker::new("pulsar://localhost:6650").default_subscription("orders-worker");
    /// # let _ = broker;
    /// ```
    pub fn default_subscription(mut self, subscription: impl Into<String>) -> Self {
        self.default_subscription = Some(subscription.into());
        self
    }

    /// Bounds how long the client keeps asking for an operation a server has not accepted.
    ///
    /// Left alone, the client retries such an answer for ever, five seconds apart, which is what
    /// waits out a broker that is restarting. The same patience covers a subscription the server
    /// refuses because another consumer holds it, so a second consumer of an
    /// [`Exclusive`](crate::SubscriptionType::Exclusive) subscription neither takes it nor
    /// reports anything - it waits, and the service does not start. A service that has to hear
    /// about that instead bounds the tries here, and `subscribe` then returns
    /// [`PulsarError::Subscribe`](PulsarError) naming the topic.
    ///
    /// The bound is the connection's, not one operation's: it covers the lookups, the producers
    /// and the consumer's own reconnect after a disconnect, so a short bound turns a long outage
    /// into a subscription that gives up rather than one that waits.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::nonzero;
    /// use ruststream_pulsar::{OperationRetries, PulsarBroker};
    ///
    /// let broker = PulsarBroker::new("pulsar://localhost:6650")
    ///     .operation_retries(OperationRetries::attempts(nonzero!(3u32)));
    /// # let _ = broker;
    /// ```
    pub fn operation_retries(mut self, retries: OperationRetries) -> Self {
        self.retries = Some(retries);
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
                if let Some(retries) = self.retries.clone() {
                    builder = builder.with_operation_retry_options(retries.into());
                }
                let client = builder
                    .build()
                    .await
                    .map_err(|e| PulsarError::Connect(box_err(e)))?;
                Ok::<_, PulsarError>(Arc::new(Core::new(
                    Transport::Client(client),
                    self.default_subscription.clone(),
                )))
            })
            .await?
            .clone();
        Ok(ConnectedPulsarBroker {
            core,
            cell: self.cell,
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, carrying the
/// in-process transport in place of the client and every setting of this broker.
///
/// The service URL is parsed as the client parses it, so a broker a service could not connect is
/// not one a test can connect either. The connection cell is the one `connect` fills, so a
/// publisher taken from the broker before the harness connected it publishes in process.
#[cfg(feature = "testing")]
impl InProcess for PulsarBroker {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        in_process::check_url(&self.url)?;
        let core = self
            .cell
            .get_or_init(async || {
                Arc::new(Core::new(
                    Transport::InProcess(Bus::new()),
                    self.default_subscription.clone(),
                ))
            })
            .await
            .clone();
        Ok(ConnectedPulsarBroker {
            core,
            cell: self.cell,
        })
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(PulsarBroker);

/// The description carries the address a client dials and nothing else: the generated `AsyncAPI`
/// document is published and shared, so a `user:password@` in the service URL must not reach it.
/// `ServerSpec::from_url` is the framework's own reduction, so every broker crate drops the
/// userinfo the same way.
impl DescribeServer for PulsarBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(&self.url, "pulsar")
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
        #[cfg(feature = "testing")]
        let (route, name, subscription) = (
            Route::of(&descriptor)?,
            descriptor.source_name().to_owned(),
            descriptor.subscription().to_owned(),
        );
        #[cfg(feature = "testing")]
        let recorded = route.clone();
        let subscriber = match &self.core.transport {
            Transport::Client(client) => PulsarSubscriber::open(client, descriptor).await,
            #[cfg(feature = "testing")]
            Transport::InProcess(bus) => in_process::subscribe(bus, descriptor, route),
        }?;
        #[cfg(feature = "testing")]
        self.core.opened.record(name, subscription, recorded);
        Ok(subscriber)
    }
}

impl ConnectedBroker for ConnectedPulsarBroker {
    type Error = PulsarError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        #[cfg(feature = "testing")]
        if let Transport::InProcess(bus) = &self.core.transport {
            bus.close();
        }
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
    /// A bare name opens the same consumer a descriptor does, so a spent delivery moves at the
    /// client here too, under the policy the registration declared.
    type Copies = BrokerMoves;

    /// A bare name joins the broker's [default subscription](PulsarBroker::default_subscription),
    /// so the handlers mounted by name compete on one durable cursor. A broker that names none
    /// refuses with [`PulsarError::Invalid`] naming the setting.
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let subscription = self.core.default_subscription.as_deref().ok_or_else(|| {
            PulsarError::Invalid(format!(
                "bare topic name '{name}' has no subscription to join: set \
                 `PulsarBroker::default_subscription(..)`, or mount \
                 `PulsarSubscription::new(topic, subscription)`"
            ))
        })?;
        let descriptor = self.core.declared_retries.subscription(name, subscription);
        self.subscribe_descriptor(descriptor).await
    }

    /// Maps the declaration onto the consumer this name opens: the limit and the topic become
    /// the client's own `DeadLetterPolicy`, built in `subscribe` where the connection exists.
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        self.core.declared_retries.declare(name, declaration)
    }
}

impl DefaultPublish for ConnectedPulsarBroker {
    type Policy = PulsarPublish;
}

/// The harness's view of the connected broker: what it injects and reads back on the in-process
/// transport, and, on either transport, which subscriptions a publish reaches.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the transport `connect_in_process` produced, and a live connection has no log to read and no
/// synchronous way to take a message. `inject` panics on a destination that is no topic name,
/// which a server refuses and the trait gives no way to report.
#[cfg(feature = "testing")]
impl TestableBroker for ConnectedPulsarBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Transport::InProcess(bus) = &self.core.transport {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let bus = self.bus("inject");
        // An injection stands in for an external producer; against a shut-down transport it is
        // dropped, which is what a producer talking to a closed broker achieves in effect.
        if self.core.ensure_open().is_err() {
            return;
        }
        let name = message.name();
        let owned = OutgoingMessage::produced(name, BytesMut::from(message.payload()))
            .with_headers(message.headers().clone());
        if let Err(err) = bus.publish(owned, None) {
            panic!("the injected message to {name:?} is not one a server takes: {err}");
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published").published(name)
    }

    /// Pulsar's routing: a message reaches every subscription over its topic once, and within one
    /// subscription one of its consumers, so each durable subscription the topic reaches is owed
    /// the message once, by the first of its consumers in `subscriptions`. A topic reads the same
    /// under either spelling, and a pattern matches the fully qualified names of the
    /// `public/default` namespace.
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        self.core.opened.routes(destination, subscriptions)
    }
}

#[cfg(feature = "testing")]
impl ConnectedPulsarBroker {
    /// The in-process transport, which is all the harness injects into and reads back from.
    fn bus(&self, what: &str) -> &Arc<Bus> {
        match &self.core.transport {
            Transport::InProcess(bus) => bus,
            Transport::Client(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the transport `connect_in_process` produces"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "testing")]
    use ruststream::Publisher;
    use ruststream::nonzero;

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

    /// The client counts what follows the first try, so the bound a service writes in tries is
    /// one more than the number the client is given. An off-by-one here is a service that gives
    /// up a try early or waits one longer than it asked to.
    #[test]
    fn a_bound_in_tries_becomes_the_retries_that_follow_the_first() {
        let bounded = OperationRetryOptions::from(OperationRetries::attempts(nonzero!(3u32)));
        assert_eq!(bounded.max_retries, Some(2));

        let once = OperationRetryOptions::from(OperationRetries::attempts(nonzero!(1u32)));
        assert_eq!(
            once.max_retries,
            Some(0),
            "one try must leave the first answer final",
        );

        let unbounded = OperationRetryOptions::from(OperationRetries::unbounded());
        assert_eq!(unbounded.max_retries, None);
        assert_eq!(unbounded.retry_delay, DEFAULT_RETRY_DELAY);
        assert_eq!(unbounded.operation_timeout, DEFAULT_OPERATION_TIMEOUT);
    }

    /// A URL the client cannot connect with is refused in process as `connect` refuses it, so a
    /// test cannot pass on a broker the service could not start.
    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn an_address_connect_refuses_is_refused_in_process() {
        for url in ["not a url", "pulsar://"] {
            let refused = PulsarBroker::new(url).connect_in_process().await;
            assert!(
                matches!(refused, Err(PulsarError::Connect(_))),
                "{url:?} must be refused, got {refused:?}",
            );
        }
    }

    /// A publisher taken off the broker before the harness connected it shares the connection the
    /// in-process transition fills, as it shares the one `connect` fills.
    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn an_early_publisher_publishes_in_process() {
        let broker = PulsarBroker::new("pulsar://localhost:6650");
        let early = broker.publisher();
        let connected = broker
            .connect_in_process()
            .await
            .expect("connects in process");

        early
            .publish(
                OutgoingMessage::produced("orders", BytesMut::from(&b"{}"[..])),
                None,
            )
            .await
            .expect("the early publisher reaches the in-process transport");
        assert_eq!(connected.published("orders").len(), 1);

        connected.shutdown().await.expect("shutdown");
        let late = early
            .publish(
                OutgoingMessage::produced("orders", BytesMut::from(&b"{}"[..])),
                None,
            )
            .await;
        assert!(matches!(late, Err(PulsarError::NotConnected)), "{late:?}");
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
