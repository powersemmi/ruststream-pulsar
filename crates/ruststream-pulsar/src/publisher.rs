//! [`PulsarPublisher`], its [`PulsarPublish`] policy, and the crate's per-message publish
//! arguments ([`PulsarPublishExt`]).

use std::sync::Arc;

use bytes::Bytes;
use pulsar::TokioExecutor;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};
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
/// The framework routes every publish through one builder
/// (`publisher.message(&value).publish()`), whose positions - the codec, the destination, the
/// headers - belong to the framework. A broker argument that is per-message rather than
/// per-publisher attaches one step earlier, on the publisher itself: the method returns a small
/// adapter that carries the argument and applies it as the message passes through, so the
/// builder keeps every position it had and the argument composes with all of them.
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
    /// The same key the [`PARTITION_KEY_HEADER`] header carries, named as the argument it is:
    /// the header form competes for the publish's single headers position, so a message
    /// declaring a typed header contract cannot also carry a hand-written partition key, while
    /// this one applies below the builder and composes with any of them.
    ///
    /// The adapter borrows the publisher and lives for the publish it is chained onto; for a
    /// key fixed for the lifetime of a publisher, keep the publisher and pass the key per call
    /// anyway - the adapter costs nothing to build.
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

/// A publisher that stamps a partition key onto every message it forwards.
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
    key: Bytes,
}

impl<'a, P> PartitionKeyed<'a, P> {
    fn new(inner: &'a P, key: impl Into<String>) -> Self {
        Self {
            inner,
            // Converted once, so each publish pays a refcount bump rather than an allocation.
            key: Bytes::from(key.into()),
        }
    }
}

impl<P: Publisher> Publisher for PartitionKeyed<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        // The builder hands over a finished message whose header map it does not lend mutably,
        // so the key goes into a copy; on the ordinary path (no headers of the caller's own)
        // that copy is an empty map, which allocates nothing.
        let mut headers = msg.headers().clone();
        headers.insert(PARTITION_KEY_HEADER, self.key.clone());
        self.inner.publish(msg.with_headers(headers)).await
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

    async fn pair(self, connected: &ConnectedPulsarBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::testing::TestableBroker;
    use ruststream::{Headers, Outgoing};
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
        let mut headers = Headers::new();
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

    // The point of the argument over the header: the headers position of a publish belongs to
    // the message's declared contract, so a hand-written partition key has nowhere to go here.
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
