//! [`PulsarSubscriber`]: a stream of deliveries backed by a driver task.
//!
//! The client's acknowledgement API needs `&mut Consumer` while an ack token must be
//! `Send + 'static`, so the crate owns a driver task per subscription: it polls the consumer
//! stream, forwards deliveries into a bounded channel, and applies settlement commands shipped
//! back from message handles.
//!
//! The client hands over one delivery at a time - it has no consumer-side batch receive, only a
//! flow-control window - so the batches a batch handler asks for are assembled on the client, by
//! the framework's own [`BufferedSubscriber`]. Nothing at the mount site says so: a service
//! names a batch size and gets batches.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};

use pulsar::consumer::{Consumer, DeadLetterPolicy};
use pulsar::proto::MessageIdData;
use pulsar::{Pulsar, SubType, TokioExecutor};
use ruststream::{AckError, BatchSubscriber, BufferedSubscriber, Seekable, Subscriber};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::warn;

use crate::error::{PulsarError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::{LogSeeker, Queued};
use crate::message::{
    DriverCmd, NackAfterCmd, PulsarMessage, PulsarPosition, SeekCmd, SettleKind, SettleSender,
    send_settle,
};
use crate::subscription::{PulsarSubscription, SubscriptionType, Topics};

/// How many undelivered messages may sit between the driver and the consumer. Real prefetch is
/// the client's own flow control (`batch_size` permits); this only decouples the two loops.
const CHANNEL_CAPACITY: usize = 16;

/// A subscription to one or more Pulsar topics; yields [`PulsarMessage`]s, one at a time or in
/// batches.
///
/// Dropping the subscriber stops the driver task, which closes the client consumer.
pub struct PulsarSubscriber {
    topic: String,
    /// The wire deliveries plus client-side batching. Every capability of the subscription
    /// reaches through the wrapper: buffering does not move the subscription, so the seeker
    /// underneath is the subscription's own.
    inner: BufferedSubscriber<Deliveries>,
}

/// The wire form of a subscription: one delivery at a time, off the driver task's channel, or,
/// under the `testing` feature, off the in-process transport's queue.
///
/// This is everything the client offers, and [`PulsarSubscriber`] is this plus the batches.
/// Without the feature there is one variant, so the type is the consumer's channel itself.
#[derive(Debug)]
enum Deliveries {
    Consumer(ConsumerDeliveries),
    #[cfg(feature = "testing")]
    InProcess(Queued),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives
// the wire form exactly the size of the consumer channel it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Deliveries>() == size_of::<ConsumerDeliveries>());

/// A live consumer's deliveries, off its driver task's channel.
#[derive(Debug)]
struct ConsumerDeliveries {
    rx: mpsc::Receiver<(u64, Result<PulsarMessage, PulsarError>)>,
    cmd: SettleSender,
    /// The delivery generation: a seek bumps it, and items queued under an older generation
    /// are discarded on the way out - a reposition must not deliver stale buffered messages.
    epoch: Arc<AtomicU64>,
}

impl ConsumerDeliveries {
    /// The next delivery of the current generation. Items queued under an older generation
    /// (before a seek) are discarded here.
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<PulsarMessage, PulsarError>>> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some((epoch, item))) => {
                    if epoch == self.epoch.load(Ordering::Acquire) {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl std::fmt::Debug for PulsarSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarSubscriber")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl PulsarSubscriber {
    /// The topic list or pattern this subscription consumes from.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Opens the client consumer and starts the subscription's driver on `runtime`, the one the
    /// broker connected on.
    pub(crate) async fn open(
        client: &Pulsar<TokioExecutor>,
        descriptor: PulsarSubscription,
        runtime: &Handle,
    ) -> Result<Self, PulsarError> {
        let display = descriptor.display_topic();
        let batch_wait = descriptor.batch_wait;
        let mut builder = client
            .consumer()
            .with_subscription(&descriptor.subscription)
            .with_subscription_type(match descriptor.sub_type {
                SubscriptionType::Exclusive => SubType::Exclusive,
                SubscriptionType::Shared => SubType::Shared,
                SubscriptionType::Failover => SubType::Failover,
                SubscriptionType::KeyShared => SubType::KeyShared,
            });
        match &descriptor.topics {
            Topics::List(topics) => {
                builder = builder.with_topics(topics);
            }
            Topics::Pattern(pattern) => {
                let regex = regex::Regex::new(pattern)
                    .map_err(|e| PulsarError::Invalid(format!("invalid pattern: {e}")))?;
                builder = builder.with_topic_regex(regex);
            }
        }
        if let Some(dead_letter) = descriptor.dead_letter_policy()? {
            // The client delivers while the redelivery count is below the limit, so the limit is
            // the number of deliveries the registration asked for.
            builder = builder.with_dead_letter_policy(DeadLetterPolicy {
                max_redeliver_count: usize::try_from(dead_letter.max_deliveries)
                    .unwrap_or(usize::MAX),
                dead_letter_topic: dead_letter.topic,
            });
        }
        if descriptor.ack_timeout.is_some() {
            builder = builder.with_unacked_message_resend_delay(descriptor.ack_timeout);
        }

        let consumer: Consumer<Vec<u8>, TokioExecutor> =
            builder.build().await.map_err(|e| PulsarError::Subscribe {
                topic: display.clone(),
                source: box_err(e),
            })?;

        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (settle_tx, settle_rx) = mpsc::unbounded_channel();
        let epoch = Arc::new(AtomicU64::new(0));
        runtime.spawn(drive(
            consumer,
            client.clone(),
            out_tx,
            settle_tx.clone(),
            settle_rx,
            display.clone(),
            Arc::clone(&epoch),
            descriptor.ack_timeout,
        ));

        Ok(Self::batching(
            display,
            Deliveries::Consumer(ConsumerDeliveries {
                rx: out_rx,
                cmd: settle_tx,
                epoch,
            }),
            batch_wait,
        ))
    }

    /// A subscription on the in-process transport, batched the way a live one is.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(topic: String, queued: Queued, batch_wait: Duration) -> Self {
        Self::batching(topic, Deliveries::InProcess(queued), batch_wait)
    }

    /// Wraps the wire deliveries in the framework's client-side buffer.
    ///
    /// The batch size is not this crate's to choose - it arrives per subscription, as the
    /// argument of [`BatchSubscriber::batches`]. The deadline that closes a partial batch is,
    /// and the descriptor's `batch_wait` is where a service names it.
    fn batching(topic: String, wire: Deliveries, batch_wait: Duration) -> Self {
        Self {
            topic,
            inner: BufferedSubscriber::new(wire).max_wait(batch_wait),
        }
    }
}

/// Repositions a subscription while its stream runs; minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker).
///
/// A seek covers every topic (and partition) of the subscription's consumer, and the broker
/// redelivers from the new position; per-message acknowledgement state needs no reset.
///
/// One type serves both transports, so a service that repositions itself reads the same
/// [`SeekHandle`](crate::SeekHandle) key whether it runs against a server or in process under
/// the test harness.
#[derive(Clone)]
pub struct PulsarSeeker {
    inner: SeekerKind,
}

/// Which subscription the handle moves. The in-process arm exists only with the `testing`
/// feature, and the field is private, so a service sees one type either way.
#[derive(Clone)]
enum SeekerKind {
    /// A live consumer, repositioned through its subscription's driver task.
    Consumer(SettleSender),
    /// An in-process subscription, repositioned over the transport's retained log.
    #[cfg(feature = "testing")]
    InProcess(LogSeeker),
}

impl std::fmt::Debug for PulsarSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarSeeker").finish_non_exhaustive()
    }
}

impl PulsarSeeker {
    /// Mints a handle over the subscription's driver channel. Cloning the sender is a
    /// reference-count bump, so a per-delivery context costs no allocation.
    pub(crate) fn new(cmd: SettleSender) -> Self {
        Self {
            inner: SeekerKind::Consumer(cmd),
        }
    }

    /// Mints a handle over the in-process transport's retained log.
    #[cfg(feature = "testing")]
    pub(crate) const fn in_process(seeker: LogSeeker) -> Self {
        Self {
            inner: SeekerKind::InProcess(seeker),
        }
    }
}

impl ruststream::Seeker for PulsarSeeker {
    type Position = PulsarPosition;
    type Error = PulsarError;

    async fn seek(&self, to: PulsarPosition) -> Result<(), PulsarError> {
        match &self.inner {
            SeekerKind::Consumer(cmd) => {
                let (done, wait) = oneshot::channel();
                cmd.send(DriverCmd::Seek(SeekCmd { position: to, done }))
                    .map_err(|_| PulsarError::Receive {
                        topic: String::new(),
                        source: Box::from("the subscription's driver task has shut down"),
                    })?;
                wait.await.map_err(|_| PulsarError::Receive {
                    topic: String::new(),
                    source: Box::from("the subscription's driver task has shut down"),
                })?
            }
            #[cfg(feature = "testing")]
            SeekerKind::InProcess(seeker) => {
                seeker.seek(&to);
                Ok(())
            }
        }
    }
}

impl Seekable for Deliveries {
    type Seeker = PulsarSeeker;

    fn seeker(&self) -> PulsarSeeker {
        match self {
            Self::Consumer(consumer) => PulsarSeeker::new(consumer.cmd.clone()),
            #[cfg(feature = "testing")]
            Self::InProcess(queued) => queued.seeker(),
        }
    }
}

impl Subscriber for Deliveries {
    type Message = PulsarMessage;
    type Error = PulsarError;

    fn stream(&mut self) -> impl Stream<Item = Result<PulsarMessage, PulsarError>> + Send + '_ {
        // Poll in place rather than wrapping the source in an owning stream, so `stream` can be
        // called again after the returned stream is dropped (the runtime and the conformance
        // helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| match self {
            Self::Consumer(consumer) => consumer.poll_next(cx),
            #[cfg(feature = "testing")]
            Self::InProcess(queued) => queued.poll_next(cx),
        })
    }
}

/// Buffering does not move the subscription, so the handle is the wire subscriber's own: a batch
/// subscription still opens at a chosen position and still repositions from a handler.
impl Seekable for PulsarSubscriber {
    type Seeker = PulsarSeeker;

    fn seeker(&self) -> PulsarSeeker {
        self.inner.seeker()
    }
}

impl Subscriber for PulsarSubscriber {
    type Message = PulsarMessage;
    type Error = PulsarError;

    fn stream(&mut self) -> impl Stream<Item = Result<PulsarMessage, PulsarError>> + Send + '_ {
        self.inner.stream()
    }
}

/// Batches assembled on the client, because the transport has none of its own: the client's
/// consumer yields one delivery at a time, and its `batch_size` is a flow-control window rather
/// than a receive size, so there is nothing to translate a batch size into on the wire.
///
/// A batch never carries more than the size the registration named - the buffer closes it there -
/// and it carries fewer whenever the deadline elapsed first.
impl BatchSubscriber for PulsarSubscriber {
    type Batch = Vec<PulsarMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, PulsarError>> + Send + '_ {
        self.inner.batches(size)
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    mut consumer: Consumer<Vec<u8>, TokioExecutor>,
    client: Pulsar<TokioExecutor>,
    out: mpsc::Sender<(u64, Result<PulsarMessage, PulsarError>)>,
    settle_tx: SettleSender,
    mut settle_rx: mpsc::UnboundedReceiver<DriverCmd>,
    topic: String,
    epoch: Arc<AtomicU64>,
    // Carried onto every delivery: a delayed retry has to know when the consumer would
    // redeliver the message on its own.
    ack_timeout: Option<Duration>,
) {
    // Deliveries carry the generation captured when they were pulled off the consumer:
    // stamping at send time would let a seek's bump - which lands before the seek command is
    // processed - leak onto a delivery positioned before the seek.
    let mut pending: Option<(u64, PulsarMessage)> = None;
    loop {
        if let Some((stamp, msg)) = pending.take() {
            // A delivery is waiting for channel capacity; keep settling while it waits so an
            // unpolled stream can never wedge in-flight acks.
            tokio::select! {
                biased;
                cmd = settle_rx.recv() => {
                    match cmd {
                        Some(DriverCmd::Seek(seek)) => {
                            // The reposition drops the delivery waiting for capacity too.
                            epoch.fetch_add(1, Ordering::Release);
                            apply_seek(&mut consumer, &client, seek).await;
                        }
                        Some(cmd) => {
                            apply(&mut consumer, &client, cmd).await;
                            pending = Some((stamp, msg));
                        }
                        None => pending = Some((stamp, msg)),
                    }
                }
                permit = out.reserve() => match permit {
                    Ok(permit) => permit.send((stamp, Ok(msg))),
                    Err(_) => break, // subscriber dropped
                },
            }
        } else {
            let current = epoch.load(Ordering::Acquire);
            tokio::select! {
                biased;
                cmd = settle_rx.recv() => {
                    match cmd {
                        Some(DriverCmd::Seek(seek)) => {
                            epoch.fetch_add(1, Ordering::Release);
                            apply_seek(&mut consumer, &client, seek).await;
                        }
                        Some(cmd) => apply(&mut consumer, &client, cmd).await,
                        None => {}
                    }
                }
                () = out.closed() => break, // subscriber dropped
                next = consumer.next() => match next {
                    Some(Ok(message)) => {
                        pending = Some((
                            current,
                            PulsarMessage::new(&message, settle_tx.clone(), ack_timeout),
                        ));
                    }
                    Some(Err(err)) => {
                        // Single-topic consumers surface transient errors here while the
                        // client reconnects underneath; forward and keep going.
                        if out
                            .send((
                                current,
                                Err(PulsarError::Receive {
                                    topic: topic.clone(),
                                    source: box_err(err),
                                }),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    None => {
                        // The engine gave up (retries exhausted): the stream is dead for good.
                        let _ = out
                            .send((
                                current,
                                Err(PulsarError::Receive {
                                    topic: topic.clone(),
                                    source: Box::from("the consumer stream ended"),
                                }),
                            ))
                            .await;
                        break;
                    }
                },
            }
        }
    }

    // Outstanding message handles may still settle; serve them until every clone of the
    // settle sender is gone.
    drop(settle_tx);
    while let Some(cmd) = settle_rx.recv().await {
        apply(&mut consumer, &client, cmd).await;
    }
    if let Err(err) = Box::pin(consumer.close()).await {
        tracing::debug!(topic = %topic, error = %err, "pulsar consumer close failed");
    }
}

async fn apply(
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
    client: &Pulsar<TokioExecutor>,
    cmd: DriverCmd,
) {
    match cmd {
        DriverCmd::NackAfter(cmd) => wait_then_nack(cmd),
        DriverCmd::Settle(cmd) => {
            let result = match cmd.kind {
                SettleKind::Ack => consumer.ack_with_id(&cmd.topic, cmd.id).await,
                SettleKind::Nack => consumer.nack_with_id(&cmd.topic, cmd.id).await,
            };
            let _ = cmd
                .done
                .send(result.map_err(|e| AckError::Broker(box_err(e))));
        }
        DriverCmd::Seek(seek) => apply_seek(consumer, client, seek).await,
    }
}

/// Waits out a delayed retry, then negatively acknowledges the delivery through its own channel.
///
/// The driver runs on the runtime the broker connected on, so the wait does too, whichever thread
/// settled the delivery.
fn wait_then_nack(
    NackAfterCmd {
        topic,
        id,
        delay,
        back,
    }: NackAfterCmd,
) {
    tokio::spawn(async move {
        sleep(delay).await;
        let named = topic.clone();
        if let Err(err) = send_settle(back, topic, id, SettleKind::Nack).await {
            warn!(
                target: "ruststream_pulsar::subscriber",
                topic = %named,
                delay = ?delay,
                error = %err,
                "a delayed retry could not be negatively acknowledged; the broker redelivers it \
                 once the consumer's ack timeout elapses",
            );
        }
    });
}

/// The ids the protocol reserves for the two ends of a log, spelled `-1` for the beginning and
/// `i64::MAX` for the tip (the Java and Go clients agree on both). `ledger_id` and `entry_id`
/// are unsigned on the wire, so the beginning travels as the all-ones pattern.
const EARLIEST_MARK: u64 = u64::MAX;
const LATEST_MARK: u64 = i64::MAX.unsigned_abs();

fn end_of_log(mark: u64) -> MessageIdData {
    MessageIdData {
        ledger_id: mark,
        entry_id: mark,
        // The sentinel addresses the topic as a whole, which is what -1 means here.
        partition: Some(-1),
        ..MessageIdData::default()
    }
}

async fn apply_seek(
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
    client: &Pulsar<TokioExecutor>,
    SeekCmd { position, done }: SeekCmd,
) {
    let (message_id, timestamp) = match position {
        PulsarPosition::Earliest => (Some(end_of_log(EARLIEST_MARK)), None),
        PulsarPosition::Latest => (Some(end_of_log(LATEST_MARK)), None),
        PulsarPosition::MessageId(id) => (Some(id), None),
        PulsarPosition::Timestamp(millis) => (None, Some(millis)),
    };
    // A multi-topic consumer (a topic list or a pattern) seeks per topic and rejects an
    // unnamed set, so the subscription's own topics are always spelled out; the single-topic
    // consumer ignores the list.
    let topics = consumer.topics();
    let result =
        Box::pin(consumer.seek(Some(topics.clone()), message_id, timestamp, client.clone()))
            .await
            .map_err(|e| PulsarError::Receive {
                topic: topics.join(","),
                source: box_err(e),
            });
    let _ = done.send(result);
}
