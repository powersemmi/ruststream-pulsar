//! [`PulsarPublisher`] and its [`PulsarPublish`] policy.

use std::sync::Arc;

use pulsar::TokioExecutor;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};
use tokio::sync::Mutex;

use crate::broker::{ConnectedPulsarBroker, Core, CoreCell};
use crate::error::{PulsarError, box_err};
use crate::message::to_pulsar_message;
use crate::topic::PulsarTopic;

pub(crate) type PulsarProducer = pulsar::Producer<TokioExecutor>;

/// Publishes messages to Pulsar topics, one producer per topic, created lazily and shared
/// through the broker core (so `shutdown` can close them).
///
/// A `partition-key` header becomes the message's partition key, which keyed routing and
/// `KeyShared` subscriptions order by. Awaits the broker's send receipt, so `Ok` means the
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
