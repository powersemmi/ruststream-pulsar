//! [`PulsarPublisher`], its [`PulsarPublish`] policy, and the crate's per-message publish
//! arguments ([`PulsarPublishExt`]).

use std::future::{Future, ready};
use std::iter::once;
use std::sync::Arc;

use pulsar::TokioExecutor;
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};
use tokio::sync::Mutex;

use crate::broker::{ConnectedPulsarBroker, Core, CoreCell};
use crate::error::{PulsarError, box_err};
use crate::message::{PARTITION_KEY_HEADER, to_pulsar_message};
use crate::topic::PulsarTopic;

pub(crate) type PulsarProducer = pulsar::Producer<TokioExecutor>;

/// Publishes messages to Pulsar topics, one producer per topic, created lazily and shared
/// through the broker core (so `shutdown` can close them).
///
/// A `partition-key` header becomes the message's partition key, which keyed routing and
/// `KeyShared` subscriptions order by; [`PulsarPublishExt::with_partition_key`] names that key
/// as a publish argument instead. Awaits the broker's send receipt, so `Ok` means the
/// broker stored the message. Buildable before `connect` and usable until `shutdown`;
/// afterwards every publish reports [`PulsarError::NotConnected`].
#[derive(Clone)]
pub struct PulsarPublisher {
    cell: CoreCell,
}

impl std::fmt::Debug for PulsarPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarPublisher").finish_non_exhaustive()
    }
}

impl PulsarPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell }
    }

    fn core(&self) -> Result<&Core, PulsarError> {
        let core = self.cell.get().ok_or(PulsarError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }

    /// The per-topic producer, created on first use and cached on the core.
    // The map guard intentionally spans the build so two callers cannot race a double
    // producer for the same topic.
    #[allow(clippy::significant_drop_tightening)]
    async fn producer_for(
        &self,
        core: &Core,
        topic: &str,
    ) -> Result<Arc<Mutex<PulsarProducer>>, PulsarError> {
        let full = PulsarTopic::parse(topic)?.as_str().to_owned();
        let mut producers = core.producers.lock().await;
        if let Some(producer) = producers.get(&full) {
            return Ok(Arc::clone(producer));
        }
        let producer = Box::pin(core.client.producer().with_topic(&full).build())
            .await
            .map_err(|e| PulsarError::Publish {
                topic: topic.to_owned(),
                source: box_err(e),
            })?;
        let producer = Arc::new(Mutex::new(producer));
        producers.insert(full, Arc::clone(&producer));
        Ok(producer)
    }
}

impl Publisher for PulsarPublisher {
    type Error = PulsarError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let producer = Box::pin(self.producer_for(core, msg.name())).await?;
        let message = to_pulsar_message(&msg);
        let receipt = {
            let mut producer = producer.lock().await;
            Box::pin(producer.send_non_blocking(message))
                .await
                .map_err(|e| PulsarError::Publish {
                    topic: msg.name().to_owned(),
                    source: box_err(e),
                })?
        };
        receipt.await.map(|_| ()).map_err(|e| PulsarError::Publish {
            topic: msg.name().to_owned(),
            source: box_err(e),
        })
    }
}

/// Pulsar's per-message publish arguments, attached to a publisher ahead of the publish
/// builder.
///
/// Each method returns an adapter to start the publish builder from, so the argument travels
/// with the message without taking any of the builder's own positions.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::runtime::PublishExt;
/// use ruststream_pulsar::PulsarPublishExt;
/// use ruststream_pulsar::testing::PulsarTestBroker;
///
/// let publisher = PulsarTestBroker::new().publisher();
/// publisher
///     .with_partition_key("user-42")
///     .raw(b"{\"id\":1}")
///     .to("orders")
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait PulsarPublishExt: Publisher + Sized {
    /// Publishes through this publisher with `key` as the message's partition key, which keyed
    /// routing and `KeyShared` subscriptions order by.
    ///
    /// The key travels as the [`PARTITION_KEY_HEADER`] header, sent under the publish's own
    /// headers: a publish that names `partition-key` itself overrides this one, and a publish
    /// that names other keys keeps it. A message with a declared header contract can carry both.
    ///
    /// The key applies to publishes assembled by the builder; a raw [`Publisher::publish`] call
    /// bypasses the header merge, as it does for any base headers.
    ///
    /// The returned adapter borrows the publisher and lives for the publish it is chained onto.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "testing")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::runtime::PublishExt;
    /// use ruststream_pulsar::PulsarPublishExt;
    /// use ruststream_pulsar::testing::PulsarTestBroker;
    ///
    /// let broker = PulsarTestBroker::new();
    /// broker
    ///     .publisher()
    ///     .with_partition_key("user-42")
    ///     .raw(b"{}")
    ///     .to("orders")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    fn with_partition_key(&self, key: impl Into<String>) -> PartitionKeyed<'_, Self> {
        PartitionKeyed::new(self, key)
    }
}

impl PulsarPublishExt for PulsarPublisher {}

/// A publisher that carries a partition key under every message published through it.
///
/// Built by [`PulsarPublishExt::with_partition_key`] and used as the publish builder's starting
/// point, so it never appears in a type annotation.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::runtime::PublishExt;
/// use ruststream_pulsar::PulsarPublishExt;
/// use ruststream_pulsar::testing::PulsarTestBroker;
///
/// let publisher = PulsarTestBroker::new().publisher();
/// let keyed = publisher.with_partition_key("user-42");
/// keyed.raw(b"{}").to("orders").publish().await?;
/// keyed.raw(b"{}").to("orders.audit").publish().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PartitionKeyed<'a, P> {
    inner: &'a P,
    base: HeaderMap,
}

impl<'a, P> PartitionKeyed<'a, P> {
    fn new(inner: &'a P, key: impl Into<String>) -> Self {
        Self {
            inner,
            base: once((PARTITION_KEY_HEADER, key.into())).collect(),
        }
    }
}

impl<P: Publisher> Publisher for PartitionKeyed<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
    }
}

/// The publish policy for [`PulsarPublisher`]: pure declaration, constructible anywhere,
/// paired with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::PulsarPublish;
///
/// let policy = PulsarPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct PulsarPublish;

impl PublishPolicy<ConnectedPulsarBroker> for PulsarPublish {
    type Live = PulsarPublisher;

    fn pair(
        self,
        connected: &ConnectedPulsarBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::testing::TestableBroker;
    use ruststream::{HeaderMap, Outgoing};
    use serde::Serialize;

    use super::PulsarPublishExt;
    use crate::PARTITION_KEY_HEADER;
    use crate::testing::{ConnectedPulsarTestBroker, PulsarTestBroker};
    use ruststream::Broker;

    async fn connected() -> ConnectedPulsarTestBroker {
        PulsarTestBroker::new()
            .connect()
            .await
            .expect("the in-process broker connects")
    }

    #[tokio::test]
    async fn the_argument_becomes_the_partition_key_header() {
        let broker = connected().await;
        broker
            .publisher()
            .with_partition_key("user-42")
            .raw(b"{}")
            .to("orders")
            .publish()
            .await
            .expect("publish succeeds");

        let sent = broker.published("orders");
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].headers().get(PARTITION_KEY_HEADER),
            Some(b"user-42".as_slice())
        );
    }

    #[tokio::test]
    async fn the_argument_keeps_the_headers_the_caller_supplied() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        broker
            .publisher()
            .with_partition_key("user-42")
            .raw(b"{}")
            .with_headers(headers)
            .to("orders")
            .publish()
            .await
            .expect("publish succeeds");

        let sent = broker.published("orders");
        assert_eq!(sent[0].headers().get_str("x-tenant"), Some("acme"));
        assert_eq!(
            sent[0].headers().get(PARTITION_KEY_HEADER),
            Some(b"user-42".as_slice())
        );
    }

    #[derive(Serialize)]
    struct OrderMeta {
        tenant: &'static str,
    }

    #[derive(Outgoing, Serialize)]
    #[outgoing(name = "orders.done", headers = OrderMeta)]
    struct OrderDone {
        id: u64,
    }

    #[tokio::test]
    async fn a_call_site_key_overrides_the_argument() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-7");
        broker
            .publisher()
            .with_partition_key("user-42")
            .raw(b"{}")
            .with_headers(headers)
            .to("orders")
            .publish()
            .await
            .expect("publish succeeds");

        let sent = broker.published("orders");
        assert_eq!(
            sent[0].headers().get(PARTITION_KEY_HEADER),
            Some(b"user-7".as_slice())
        );
    }

    // The case the argument exists for: a hand-written header cannot reach a publish whose
    // headers position is taken by a declared contract.
    #[tokio::test]
    async fn the_argument_composes_with_a_declared_header_contract() {
        let broker = connected().await;
        broker
            .publisher()
            .with_partition_key("user-42")
            .message(&OrderDone { id: 1 })
            .with_headers(&OrderMeta { tenant: "acme" })
            .publish()
            .await
            .expect("publish succeeds");

        let sent = broker.published("orders.done");
        assert_eq!(sent[0].headers().get_str("tenant"), Some("acme"));
        assert_eq!(
            sent[0].headers().get(PARTITION_KEY_HEADER),
            Some(b"user-42".as_slice())
        );
    }
}
