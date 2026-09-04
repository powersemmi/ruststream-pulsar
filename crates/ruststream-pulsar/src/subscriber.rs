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
use std::time::Duration;

use futures::{Stream, StreamExt};

use pulsar::consumer::{Consumer, DeadLetterPolicy};
use pulsar::proto::MessageIdData;
use pulsar::{SubType, TokioExecutor};
use ruststream::{AckError, BatchSubscriber, BufferedSubscriber, Seekable, Subscriber};
use tokio::sync::mpsc;

use crate::broker::Core;
use crate::error::{PulsarError, box_err};
use crate::message::{DriverCmd, PulsarMessage, PulsarPosition, SeekCmd, SettleKind, SettleSender};
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

/// The wire form of a subscription: one delivery at a time, off the driver task's channel.
///
/// This is everything the client offers, and [`PulsarSubscriber`] is this plus the batches.
#[derive(Debug)]
struct Deliveries {
    rx: mpsc::Receiver<(u64, Result<PulsarMessage, PulsarError>)>,
    cmd: SettleSender,
    /// The delivery generation: a seek bumps it, and items queued under an older generation
    /// are discarded on the way out - a reposition must not deliver stale buffered messages.
    epoch: Arc<AtomicU64>,
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

    pub(crate) async fn open(
        core: &Core,
        descriptor: PulsarSubscription,
    ) -> Result<Self, PulsarError> {
        let display = descriptor.display_topic();
        let batch_wait = descriptor.batch_wait;
        let mut builder = core
            .client
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
        if let Some(dead_letter) = &descriptor.dead_letter {
            builder = builder.with_dead_letter_policy(DeadLetterPolicy {
                max_redeliver_count: dead_letter.max_deliveries,
                dead_letter_topic: dead_letter.topic.clone(),
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
        tokio::spawn(drive(
            consumer,
            core.client.clone(),
            out_tx,
            settle_tx.clone(),
            settle_rx,
            display.clone(),
            Arc::clone(&epoch),
        ));

        Ok(Self::batching(
            display,
            Deliveries {
                rx: out_rx,
                cmd: settle_tx,
                epoch,
            },
            batch_wait,
        ))
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
/// [`SeekHandle`](crate::SeekHandle) key whether it runs against a server or against the
/// in-process stand-in.
#[derive(Clone)]
pub struct PulsarSeeker {
    inner: SeekerKind,
}

/// Which subscription the handle moves. The stand-in's arm exists only with the `testing`
/// feature, and the field is private, so a service sees one type either way.
#[derive(Clone)]
enum SeekerKind {
    /// A live consumer, repositioned through its subscription's driver task.
    Consumer(SettleSender),
    /// An in-process subscription, repositioned over the stand-in's retained log.
    #[cfg(feature = "testing")]
    InProcess(crate::testing::seek::LogSeeker),
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

    /// Mints a handle over the in-process stand-in's retained log.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(seeker: crate::testing::seek::LogSeeker) -> Self {
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
                let (done, wait) = tokio::sync::oneshot::channel();
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
        PulsarSeeker::new(self.cmd.clone())
    }
}

impl Subscriber for Deliveries {
    type Message = PulsarMessage;
    type Error = PulsarError;

    fn stream(&mut self) -> impl Stream<Item = Result<PulsarMessage, PulsarError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call). Items queued under an older generation
        // (before a seek) are discarded here.
        futures::stream::poll_fn(move |cx| {
            loop {
                match self.rx.poll_recv(cx) {
                    std::task::Poll::Ready(Some((epoch, item))) => {
                        if epoch == self.epoch.load(Ordering::Acquire) {
                            return std::task::Poll::Ready(Some(item));
                        }
                    }
                    std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
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

async fn drive(
    mut consumer: Consumer<Vec<u8>, TokioExecutor>,
    client: pulsar::Pulsar<TokioExecutor>,
    out: mpsc::Sender<(u64, Result<PulsarMessage, PulsarError>)>,
    settle_tx: SettleSender,
    mut settle_rx: mpsc::UnboundedReceiver<DriverCmd>,
    topic: String,
    epoch: Arc<AtomicU64>,
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
                        pending = Some((current, PulsarMessage::new(&message, settle_tx.clone())));
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
    client: &pulsar::Pulsar<TokioExecutor>,
    cmd: DriverCmd,
) {
    match cmd {
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
    client: &pulsar::Pulsar<TokioExecutor>,
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
