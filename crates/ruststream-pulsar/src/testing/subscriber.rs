//! [`PulsarTestSubscriber`] and [`PulsarTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use futures::Stream;

use pulsar::proto::MessageIdData;
use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned,
    Positioned, Seekable, Subscriber, testing::Coordinator,
};

use crate::PARTITION_KEY_HEADER;
use crate::error::PulsarError;
use crate::message::PulsarPosition;
use crate::subscriber::PulsarSeeker;
use crate::subscription::DEFAULT_BATCH_WAIT;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, SubscriptionId};
use crate::testing::seek::LogSeeker;

/// Subscriber returned by [`ConnectedPulsarTestBroker`](crate::testing::ConnectedPulsarTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes.
///
/// It batches the way the real subscriber does - through the framework's client-side buffer over
/// a one-at-a-time queue - so a batch handler under test runs the code path it will in
/// production, and a batch never carries more than the size its registration named.
pub struct PulsarTestSubscriber {
    inner: BufferedSubscriber<Queued>,
}

/// The stand-in's wire form: the subscription's queue in the router, one delivery at a time.
struct Queued {
    state: Arc<TestState>,
    id: SubscriptionId,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl std::fmt::Debug for PulsarTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarTestSubscriber")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Queued {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queued").finish_non_exhaustive()
    }
}

impl PulsarTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            inner: BufferedSubscriber::new(Queued {
                state,
                id,
                coordinator,
            })
            .max_wait(DEFAULT_BATCH_WAIT),
        }
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

/// Seeking is native to the stand-in: it keeps an append-only log per address, so a
/// subscription can be refilled from any suffix of it. That is what a service's `start_at(..)`
/// clause and its [`SeekHandle`](crate::SeekHandle) key ride on when the service is tested with
/// the framework's harness instead of against a server.
impl Seekable for Queued {
    type Seeker = PulsarSeeker;

    fn seeker(&self) -> PulsarSeeker {
        PulsarSeeker::in_process(LogSeeker::new(
            Arc::clone(&self.state),
            self.id,
            self.coordinator.clone(),
        ))
    }
}

impl Subscriber for Queued {
    type Message = PulsarTestMessage;
    type Error = PulsarError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let state = Arc::clone(&self.state);
        let id = self.id;
        let coordinator = self.coordinator.clone();
        // Minted once per stream rather than once per delivery: a message clones the `Arc`, so
        // building a per-delivery context costs reference-count bumps and nothing else.
        let seeker = Arc::new(Seekable::seeker(self));
        // Poll the queue in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (the runtime and the conformance
        // helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            state.router.poll_delivery(id, cx).map(|next| {
                next.map(|delivery| {
                    Ok(PulsarTestMessage::new(
                        delivery,
                        Arc::clone(&state),
                        id,
                        Arc::clone(&seeker),
                        coordinator.clone(),
                    ))
                })
            })
        })
    }
}

/// The seeker reaches through the buffer, so a batch subscription on the stand-in opens with
/// `start_at(..)` and repositions from a batch body, as it does on a server.
impl Seekable for PulsarTestSubscriber {
    type Seeker = PulsarSeeker;

    fn seeker(&self) -> PulsarSeeker {
        self.inner.seeker()
    }
}

impl Subscriber for PulsarTestSubscriber {
    type Message = PulsarTestMessage;
    type Error = PulsarError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
    }
}

impl BatchSubscriber for PulsarTestSubscriber {
    type Batch = Vec<PulsarTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, PulsarError>> + Send + '_ {
        self.inner.batches(size)
    }
}

/// Message handed to handlers from a [`PulsarTestSubscriber`].
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription so the next handler invocation sees it again; `nack(requeue = false)` drops it,
/// matching the real subscriber's reject path in effect.
pub struct PulsarTestMessage {
    delivery: Option<Delivery>,
    state: Arc<TestState>,
    id: SubscriptionId,
    /// The subscription's seeker, shared by every delivery it yields; the per-delivery and batch
    /// contexts clone it out of here.
    seek: Arc<PulsarSeeker>,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for PulsarTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for PulsarTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarTestMessage").finish_non_exhaustive()
    }
}

impl PulsarTestMessage {
    pub(crate) fn new(
        delivery: Delivery,
        state: Arc<TestState>,
        id: SubscriptionId,
        seek: Arc<PulsarSeeker>,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            state,
            id,
            seek,
            coordinator,
        }
    }

    /// The subscription's reposition handle, which the delivery and batch contexts read.
    pub(crate) fn seeker(&self) -> &PulsarSeeker {
        &self.seek
    }
}

/// The stand-in keeps one log per address, so a message id addresses an entry in it: the entry
/// id is the log index, which is what a seek to that position resolves back to. The index is
/// assigned at fanout and survives a requeue, so a redelivered message reports the position it
/// always had, as on a real broker.
impl Positioned for PulsarTestMessage {
    type Position = PulsarPosition;

    fn position(&self) -> PulsarPosition {
        let seq = self.delivery.as_ref().map_or(0, |d| d.seq);
        PulsarPosition::MessageId(MessageIdData {
            ledger_id: 0,
            entry_id: seq as u64,
            // The sentinel addresses the address as a whole, as it does for the real broker's
            // end-of-log marks.
            partition: Some(-1),
            ..MessageIdData::default()
        })
    }
}

impl Partitioned for PulsarTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for PulsarTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("PulsarTestMessage ack/nack invoked twice");
        if requeue {
            let queued = self.state.router.requeue(self.id, delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this
            // message's `Drop` decrement. The redelivered copy is consumed in turn.
            if queued && let Some(coordinator) = &self.coordinator {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}
