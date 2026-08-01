//! [`PulsarSubscriber`]: a stream of deliveries backed by a driver task.
//!
//! The client's acknowledgement API needs `&mut Consumer` while an ack token must be
//! `Send + 'static`, so the crate owns a driver task per subscription: it polls the consumer
//! stream, forwards deliveries into a bounded channel, and applies settlement commands shipped
//! back from message handles.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{Stream, StreamExt};

use pulsar::consumer::{Consumer, DeadLetterPolicy, InitialPosition};
use pulsar::{ConsumerOptions, SubType, TokioExecutor};
use ruststream::{AckError, Subscriber};
use tokio::sync::mpsc;

use crate::broker::Core;
use crate::error::{PulsarError, box_err};
use crate::message::{DriverCmd, PulsarMessage, PulsarPosition, SeekCmd, SettleKind, SettleSender};
use crate::subscription::{PulsarSubscription, SubscriptionType, Topics};

/// How many undelivered messages may sit between the driver and the consumer. Real prefetch is
/// the client's own flow control (`batch_size` permits); this only decouples the two loops.
const CHANNEL_CAPACITY: usize = 16;

/// A subscription to one or more Pulsar topics; yields [`PulsarMessage`]s.
///
/// Dropping the subscriber stops the driver task, which closes the client consumer.
pub struct PulsarSubscriber {
    topic: String,
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
        if descriptor.earliest {
            builder = builder.with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
            );
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

        Ok(Self {
            topic: display,
            rx: out_rx,
            cmd: settle_tx,
            epoch,
        })
    }
}

/// Repositions a [`PulsarSubscriber`] while its stream runs; minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker).
///
/// A seek covers every topic (and partition) of the subscription's consumer, and the broker
/// redelivers from the new position; per-message acknowledgement state needs no reset.
#[derive(Clone)]
pub struct PulsarSeeker {
    cmd: SettleSender,
}

impl std::fmt::Debug for PulsarSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarSeeker").finish_non_exhaustive()
    }
}

impl ruststream::Seeker for PulsarSeeker {
    type Position = PulsarPosition;
    type Error = PulsarError;

    async fn seek(&self, to: PulsarPosition) -> Result<(), PulsarError> {
        let (done, wait) = tokio::sync::oneshot::channel();
        self.cmd
            .send(DriverCmd::Seek(SeekCmd { position: to, done }))
            .map_err(|_| PulsarError::Receive {
                topic: String::new(),
                source: Box::from("the subscription's driver task has shut down"),
            })?;
        wait.await.map_err(|_| PulsarError::Receive {
            topic: String::new(),
            source: Box::from("the subscription's driver task has shut down"),
        })?
    }
}

impl ruststream::Seekable for PulsarSubscriber {
    type Seeker = PulsarSeeker;

    fn seeker(&self) -> PulsarSeeker {
        PulsarSeeker {
            cmd: self.cmd.clone(),
        }
    }
}

impl Subscriber for PulsarSubscriber {
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

async fn drive(
    mut consumer: Consumer<Vec<u8>, TokioExecutor>,
    client: pulsar::Pulsar<TokioExecutor>,
    out: mpsc::Sender<(u64, Result<PulsarMessage, PulsarError>)>,
    settle_tx: SettleSender,
    mut settle_rx: mpsc::UnboundedReceiver<DriverCmd>,
    topic: String,
    epoch: Arc<AtomicU64>,
) {
    let mut pending: Option<PulsarMessage> = None;
    loop {
        if let Some(msg) = pending.take() {
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
                            pending = Some(msg);
                        }
                        None => pending = Some(msg),
                    }
                }
                permit = out.reserve() => match permit {
                    Ok(permit) => permit.send((epoch.load(Ordering::Acquire), Ok(msg))),
                    Err(_) => break, // subscriber dropped
                },
            }
        } else {
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
                        pending = Some(PulsarMessage::new(&message, settle_tx.clone()));
                    }
                    Some(Err(err)) => {
                        // Single-topic consumers surface transient errors here while the
                        // client reconnects underneath; forward and keep going.
                        if out
                            .send((
                                epoch.load(Ordering::Acquire),
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
                                epoch.load(Ordering::Acquire),
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

async fn apply_seek(
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
    client: &pulsar::Pulsar<TokioExecutor>,
    SeekCmd { position, done }: SeekCmd,
) {
    let (message_id, timestamp) = match position {
        PulsarPosition::MessageId(id) => (Some(id), None),
        PulsarPosition::Timestamp(millis) => (None, Some(millis)),
    };
    let result = Box::pin(consumer.seek(None, message_id, timestamp, client.clone()))
        .await
        .map_err(|e| PulsarError::Receive {
            topic: consumer.topics().join(","),
            source: box_err(e),
        });
    let _ = done.send(result);
}
