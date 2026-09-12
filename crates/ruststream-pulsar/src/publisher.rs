//! [`PulsarPublisher`], its [`PulsarPublish`] policy, and the per-message settings a call site
//! adjusts ([`PulsarPublishOptions`], [`PulsarPublishSteps`]).

use std::future::{Future, ready};
use std::sync::Arc;

use pulsar::TokioExecutor;
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};
use tokio::sync::Mutex;

use crate::broker::{ConnectedPulsarBroker, Core, CoreCell};
use crate::error::{PulsarError, box_err};
use crate::message::to_pulsar_message;
use crate::topic::PulsarTopic;

pub(crate) type PulsarProducer = pulsar::Producer<TokioExecutor>;

/// What one Pulsar publish differs from the next in.
///
/// Every field is optional: a message carries what its call site named with a step of
/// [`PulsarPublishSteps`], and nothing else. The [`PulsarPublish`] policy fixes no key of its
/// own, so a publish that names no step goes unkeyed.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::PulsarPublishOptions;
///
/// let keyed = PulsarPublishOptions {
///     partition_key: Some("user-42".to_owned()),
/// };
/// # let _ = keyed;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PulsarPublishOptions {
    /// The message's partition key: keyed routing places the message by it and `KeyShared`
    /// subscriptions order by it.
    ///
    /// `None` leaves the key to the `partition-key` header, the portable spelling of the same
    /// value; with neither, the message is unkeyed.
    pub partition_key: Option<String>,
}

/// Publishes messages to Pulsar topics, one producer per topic, created lazily and shared
/// through the broker core (so `shutdown` can close them).
///
/// The partition key travels per message: [`PulsarPublishSteps::partition_key`] names it at the
/// call site, and a `partition-key` header names the same key for a caller that writes its
/// headers itself. Awaits the broker's send receipt, so `Ok` means the
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
    /// The partition key, the one value Pulsar lets one publish differ from the next in on this
    /// crate's surface.
    type Options = PulsarPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = self.core()?;
        let producer = Box::pin(self.producer_for(core, msg.name())).await?;
        let key = options.and_then(|options| options.partition_key.as_deref());
        let message = to_pulsar_message(&msg, key);
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

/// Pulsar's per-message steps on the framework's publish builder.
///
/// A step sets one field of [`PulsarPublishOptions`] for the publish being assembled and returns
/// the builder, so the message still leaves through the entry the mount site named, with that
/// entry's codec, transforms and slot attribution. The bound is on the publisher's settings type,
/// so these steps appear on a builder over a Pulsar publisher and over no other broker's.
///
/// The trait is in the crate [prelude](crate::prelude). A handler body that names a step imports
/// that glob and bounds its slot `Out<impl Publisher<Options = PulsarPublishOptions>, Marker>`;
/// every other body imports the framework prelude alone.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::Outgoing;
/// use ruststream::runtime::PublishExt;
/// use ruststream_pulsar::PulsarPublishSteps;
/// use ruststream_pulsar::testing::PulsarTestBroker;
///
/// #[derive(Outgoing, serde::Serialize)]
/// #[outgoing(name = "orders")]
/// struct Order {
///     id: u64,
/// }
///
/// PulsarTestBroker::new()
///     .publisher()
///     .message(&Order { id: 1 })
///     .partition_key("user-42")
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait PulsarPublishSteps {
    /// Sends this one message under `key` as its partition key, which keyed routing places the
    /// message by and `KeyShared` subscriptions order by.
    ///
    /// The key wins over a `partition-key` header the call site wrote itself; without either the
    /// message is unkeyed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "testing")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::{Outgoing, Serialized};
    /// use ruststream::runtime::PublishExt;
    /// use ruststream_pulsar::PulsarPublishSteps;
    /// use ruststream_pulsar::testing::PulsarTestBroker;
    ///
    /// // An already-encoded record: the newtype says the bytes are the wire form, so no
    /// // codec runs on them.
    /// #[derive(Outgoing, Serialized)]
    /// struct Record(Vec<u8>);
    ///
    /// PulsarTestBroker::new()
    ///     .publisher()
    ///     .message(&Record(b"{}".to_vec()))
    ///     .to("orders")
    ///     .partition_key("user-42")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    fn partition_key(self, key: impl Into<String>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> PulsarPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = PulsarPublishOptions>,
{
    fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(PulsarPublishOptions::default)
            .partition_key = Some(key.into());
        self
    }
}

/// The publish policy for [`PulsarPublisher`]: pure declaration, constructible anywhere,
/// paired with the connected broker by the runtime after `connect`.
///
/// It pairs against the in-process stand-in too, so a routes file writes `.out(Reply, Publish)`
/// once and mounts it on either broker.
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

/// The policy fixes no defaults - a partition key belongs to one message, not to a mount site -
/// so there is nothing here for the stand-in to honour or to drop quietly; what differs between
/// the two impls is only the live form the policy pairs into, which is the publisher that broker
/// sends with.
#[cfg(feature = "testing")]
impl PublishPolicy<crate::testing::ConnectedPulsarTestBroker> for PulsarPublish {
    type Live = crate::testing::PulsarTestPublisher;

    fn pair(
        self,
        connected: &crate::testing::ConnectedPulsarTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::testing::TestableBroker;
    use ruststream::{Broker, HeaderMap, Outgoing, Serialized};
    use serde::Serialize;

    use super::PulsarPublishSteps;
    use crate::PARTITION_KEY_HEADER;
    use crate::testing::{ConnectedPulsarTestBroker, PulsarTestBroker};

    /// The payload these tests carry: what they assert on is the key the publish carried, so the
    /// body is deliberately opaque bytes rather than a model.
    #[derive(Outgoing, Serialized)]
    struct Record(&'static [u8]);

    #[derive(Serialize)]
    struct OrderMeta {
        tenant: &'static str,
    }

    #[derive(Outgoing, Serialize)]
    #[outgoing(name = "orders.done", headers = OrderMeta)]
    struct OrderDone {
        id: u64,
    }

    async fn connected() -> ConnectedPulsarTestBroker {
        PulsarTestBroker::new()
            .connect()
            .await
            .expect("the in-process broker connects")
    }

    /// The step wins over the portable spelling of the same value, so a call site that sets both
    /// gets the one it wrote last in the chain rather than a silent merge.
    #[tokio::test]
    async fn the_step_wins_over_a_call_site_header() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-7");
        broker
            .publisher()
            .message(&Record(b"{}"))
            .with_headers(headers)
            .to("orders")
            .partition_key("user-42")
            .publish()
            .await
            .expect("publish succeeds");

        let sent = broker.published("orders");
        assert_eq!(
            sent[0].headers().get(PARTITION_KEY_HEADER),
            Some(b"user-42".as_slice())
        );
    }

    /// The case a header cannot reach: the publish's headers position is taken by a declared
    /// contract, and the key is a setting rather than one more entry in that map.
    #[tokio::test]
    async fn the_step_composes_with_a_declared_header_contract() {
        let broker = connected().await;
        broker
            .publisher()
            .message(&OrderDone { id: 1 })
            .with_headers(&OrderMeta { tenant: "acme" })
            .partition_key("user-42")
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
