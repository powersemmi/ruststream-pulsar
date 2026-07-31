//! [`PulsarSubscriber`]: a stream of deliveries backed by a driver task.
//!
//! The client's acknowledgement API needs `&mut Consumer` while an ack token must be
//! `Send + 'static`, so the crate owns a driver task per subscription: it polls the consumer
//! stream, forwards deliveries into a bounded channel, and applies settlement commands shipped
//! back from message handles.

use futures::{Stream, StreamExt};

use pulsar::consumer::{Consumer, DeadLetterPolicy, InitialPosition};
use pulsar::{ConsumerOptions, SubType, TokioExecutor};
use ruststream::{AckError, Subscriber};
use tokio::sync::mpsc;

use crate::broker::Core;
use crate::error::{PulsarError, box_err};
use crate::message::{PulsarMessage, SettleCmd, SettleKind, SettleSender};
use crate::subscription::{PulsarSubscription, SubscriptionType, Topics};

/// How many undelivered messages may sit between the driver and the consumer. Real prefetch is
/// the client's own flow control (`batch_size` permits); this only decouples the two loops.
const CHANNEL_CAPACITY: usize = 16;

/// A subscription to one or more Pulsar topics; yields [`PulsarMessage`]s.
///
/// Dropping the subscriber stops the driver task, which closes the client consumer.
pub struct PulsarSubscriber {
    topic: String,
    rx: mpsc::Receiver<Result<PulsarMessage, PulsarError>>,
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
        tokio::spawn(drive(
            consumer,
            out_tx,
            settle_tx,
            settle_rx,
            display.clone(),
        ));

        Ok(Self {
            topic: display,
            rx: out_rx,
        })
    }
}

impl Subscriber for PulsarSubscriber {
    type Message = PulsarMessage;
    type Error = PulsarError;

    fn stream(&mut self) -> impl Stream<Item = Result<PulsarMessage, PulsarError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

async fn drive(
    mut consumer: Consumer<Vec<u8>, TokioExecutor>,
    out: mpsc::Sender<Result<PulsarMessage, PulsarError>>,
    settle_tx: SettleSender,
    mut settle_rx: mpsc::UnboundedReceiver<SettleCmd>,
    topic: String,
) {
    let mut pending: Option<PulsarMessage> = None;
    loop {
        if let Some(msg) = pending.take() {
            // A delivery is waiting for channel capacity; keep settling while it waits so an
            // unpolled stream can never wedge in-flight acks.
            tokio::select! {
                biased;
                cmd = settle_rx.recv() => {
                    if let Some(cmd) = cmd {
                        apply(&mut consumer, cmd).await;
                    }
                    pending = Some(msg);
                }
                permit = out.reserve() => match permit {
                    Ok(permit) => permit.send(Ok(msg)),
                    Err(_) => break, // subscriber dropped
                },
            }
        } else {
            tokio::select! {
                biased;
                cmd = settle_rx.recv() => {
                    if let Some(cmd) = cmd {
                        apply(&mut consumer, cmd).await;
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
                        if out.send(Err(PulsarError::Receive {
                            topic: topic.clone(),
                            source: box_err(err),
                        }))
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                    None => {
                        // The engine gave up (retries exhausted): the stream is dead for good.
                        let _ = out
                            .send(Err(PulsarError::Receive {
                                topic: topic.clone(),
                                source: Box::from("the consumer stream ended"),
                            }))
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
        apply(&mut consumer, cmd).await;
    }
    if let Err(err) = Box::pin(consumer.close()).await {
        tracing::debug!(topic = %topic, error = %err, "pulsar consumer close failed");
    }
}

async fn apply(consumer: &mut Consumer<Vec<u8>, TokioExecutor>, cmd: SettleCmd) {
    let result = match cmd.kind {
        SettleKind::Ack => consumer.ack_with_id(&cmd.topic, cmd.id).await,
        SettleKind::Nack => consumer.nack_with_id(&cmd.topic, cmd.id).await,
    };
    let _ = cmd
        .done
        .send(result.map_err(|e| AckError::Broker(box_err(e))));
}
