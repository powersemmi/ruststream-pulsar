//! The subscriber and delivery halves of the in-process transport: [`Queued`], the wire form a
//! [`PulsarSubscriber`](crate::PulsarSubscriber) reads when it was opened in process, and
//! [`Settlement`], what a [`PulsarMessage`] of that subscription settles through.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{AckError, HeaderMap};
use tokio::time::sleep;

use crate::error::PulsarError;
use crate::in_process::bus::Bus;
use crate::in_process::router::{ConsumerId, Delivery};
use crate::in_process::seek::LogSeeker;
use crate::message::PulsarMessage;
use crate::subscriber::PulsarSeeker;

/// One consumer's queue in the router, read one delivery at a time.
#[derive(Debug)]
pub(crate) struct Queued {
    bus: Arc<Bus>,
    id: ConsumerId,
    /// The harness coordinator, threaded into each delivery so a requeue re-counts and a settled
    /// delivery is released. `None` outside a harness run.
    coordinator: Option<Coordinator>,
    /// The subscription's acknowledgement timeout, which bounds a delayed retry here as it does
    /// against a server.
    ack_timeout: Option<Duration>,
}

impl Queued {
    pub(crate) fn new(bus: Arc<Bus>, id: ConsumerId, ack_timeout: Option<Duration>) -> Self {
        let coordinator = bus.coordinator().cloned();
        Self {
            bus,
            id,
            coordinator,
            ack_timeout,
        }
    }

    /// The next delivery, or `Pending` with the caller's waker registered.
    pub(crate) fn poll_next(
        &self,
        cx: &Context<'_>,
    ) -> Poll<Option<Result<PulsarMessage, PulsarError>>> {
        self.bus.router().poll_delivery(self.id, cx).map(|next| {
            next.map(|delivery| {
                let settlement = Settlement {
                    bus: Arc::clone(&self.bus),
                    id: self.id,
                    seq: delivery.seq,
                    redeliveries: delivery.redeliveries,
                    coordinator: self.coordinator.clone(),
                    unsettled: Some(delivery.clone()),
                };
                Ok(PulsarMessage::in_process(
                    delivery,
                    settlement,
                    self.ack_timeout,
                ))
            })
        })
    }

    /// The handle that repositions this subscription over the retained log.
    pub(crate) fn seeker(&self) -> PulsarSeeker {
        PulsarSeeker::in_process(LogSeeker::new(
            Arc::clone(&self.bus),
            self.id,
            self.coordinator.clone(),
        ))
    }
}

/// Dropping the subscriber detaches its consumer, so a handler stops receiving as soon as its task
/// finishes, and a `Failover` standby takes over.
impl Drop for Queued {
    fn drop(&mut self) {
        self.bus
            .router()
            .unsubscribe(self.id, self.coordinator.as_ref());
    }
}

/// How one in-process delivery settles: the consumer it came from, where it sits in its topic's
/// log, and how often it has been redelivered.
///
/// Dropping it releases the delivery to the harness coordinator once, whether it was acknowledged,
/// negatively acknowledged or dropped unsettled; a requeue counts its fresh delivery first, so the
/// in-flight count stays balanced.
#[derive(Debug)]
pub(crate) struct Settlement {
    bus: Arc<Bus>,
    id: ConsumerId,
    seq: usize,
    redeliveries: u32,
    coordinator: Option<Coordinator>,
    /// The delivery until it is settled: one dropped without a settlement stays unacknowledged
    /// on its consumer, and comes back once that consumer closes.
    unsettled: Option<Delivery>,
}

impl Drop for Settlement {
    fn drop(&mut self) {
        if let Some(delivery) = self.unsettled.take() {
            self.bus
                .router()
                .hold(self.id, delivery, self.coordinator.as_ref());
        }
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl Settlement {
    /// Settles the delivery for good: acknowledged, or rejected, which Pulsar performs as an
    /// acknowledgement.
    pub(crate) fn settle(mut self) {
        self.unsettled = None;
    }

    /// The log index of the delivery, which is its message id's entry id.
    pub(crate) const fn seq(&self) -> usize {
        self.seq
    }

    /// The handle that repositions the delivery's subscription.
    pub(crate) fn seeker(&self) -> PulsarSeeker {
        PulsarSeeker::in_process(LogSeeker::new(
            Arc::clone(&self.bus),
            self.id,
            self.coordinator.clone(),
        ))
    }

    fn delivery(&self, payload: Bytes, headers: HeaderMap, topic: String) -> Delivery {
        Delivery {
            payload,
            headers,
            seq: self.seq,
            topic,
            redeliveries: self.redeliveries,
        }
    }

    /// Hands the delivery back to its subscription, which is what a negative acknowledgement
    /// asks the broker for.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] once the broker shut down: nothing is left to hand the
    /// delivery back to, and an `Ok` would promise a redelivery that never comes.
    pub(crate) fn requeue(
        mut self,
        payload: Bytes,
        headers: HeaderMap,
        topic: String,
    ) -> Result<(), AckError> {
        self.unsettled = None;
        let delivery = self.delivery(payload, headers, topic);
        let queued = self
            .bus
            .router()
            .requeue(self.id, delivery, self.coordinator.as_ref())
            .ok_or_else(consumer_gone)?;
        // The requeue bypasses fanout, so the re-enqueue is counted here to balance this
        // delivery's release on drop. A delivery that reached the dead-letter limit reports no
        // requeue and counts its own produce.
        if queued && let Some(coordinator) = &self.coordinator {
            coordinator.enqueued();
        }
        Ok(())
    }

    /// Holds the delivery for `delay`, then hands it back to its subscription.
    ///
    /// Under the harness the wait is registered with the coordinator rather than slept on, so a
    /// test drives it with `TestApp::advance` and sees nothing come back before the delay is over.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] once the broker shut down, as [`requeue`](Self::requeue)
    /// does.
    pub(crate) fn nack_after(
        mut self,
        delay: Duration,
        payload: Bytes,
        headers: HeaderMap,
        topic: String,
    ) -> Result<(), AckError> {
        if !self.bus.router().settles(self.id) {
            return Err(consumer_gone());
        }
        self.unsettled = None;
        let delivery = self.delivery(payload, headers, topic);
        let bus = Arc::clone(&self.bus);
        let id = self.id;
        if let Some(coordinator) = self.coordinator.clone() {
            let counter = coordinator.clone();
            coordinator.schedule_redelivery(delay, move || {
                // The same accounting a requeue does: a delivery that reached the dead-letter
                // limit is produced there instead and counts its own enqueues.
                if bus.router().requeue(id, delivery, Some(&counter)) == Some(true) {
                    counter.enqueued();
                }
            });
            return Ok(());
        }
        // On the runtime the broker connected on, not the settling caller's: a handler on a
        // dedicated thread settles from a runtime that may stop before the delay is out.
        self.bus.runtime().spawn(async move {
            sleep(delay).await;
            bus.router().requeue(id, delivery, None);
        });
        Ok(())
    }
}

/// The answer a settlement gets once its consumer is gone.
fn consumer_gone() -> AckError {
    AckError::Broker(Box::from(
        "the broker shut down, so the delivery cannot be handed back",
    ))
}
