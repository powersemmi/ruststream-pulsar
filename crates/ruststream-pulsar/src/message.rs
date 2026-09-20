//! [`PulsarMessage`] and the mapping between `RustStream` headers and message properties.
//!
//! Message properties carry headers directly - no envelope format is invented - and the
//! partition key rides the message's own `partition_key` in both directions.

use std::collections::HashMap;
use std::future::{Future, ready};
use std::time::Duration;

use bytes::Bytes;
use pulsar::proto::MessageIdData;
use ruststream::{
    AckError, BytesMut, HeaderMap, IncomingMessage, OutgoingMessage, Partitioned, Positioned, Str,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::warn;

use crate::error::PulsarError;

/// Header carrying the partition key, mapped onto the message's `partition_key` (which
/// `KeyShared` subscriptions order by).
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// How a delivered message asks its driver task to settle it.
#[derive(Debug)]
pub(crate) enum SettleKind {
    /// Acknowledge the delivery.
    Ack,
    /// Ask the broker to redeliver it (negative acknowledgement).
    Nack,
}

/// A position in a topic's retained log, accepted by
/// [`Seeker::seek`](ruststream::Seeker::seek).
///
/// Captured positions ([`Positioned::position`]) carry the pinned semantics the framework
/// defines: seeking to one redelivers exactly that message. The timestamp form keeps the
/// broker's own publish-time semantics instead.
///
/// The constructors exist because the `start_at(..)` clause of `#[subscriber]` recovers the
/// position type from the constructor path; a bare variant path does not name its type in the
/// tokens the macro sees.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::PulsarPosition;
///
/// let from_the_top = PulsarPosition::earliest();
/// let from_now_on = PulsarPosition::latest();
/// let from_a_point_in_time = PulsarPosition::timestamp(1_700_000_000_000);
/// # let _ = (from_the_top, from_now_on, from_a_point_in_time);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PulsarPosition {
    /// The beginning of the log: every message the topics still retain is redelivered.
    ///
    /// This is a seek, not the server-side initial position, which Pulsar applies only when a
    /// subscription is first created. A subscription is durable broker-side state, so a
    /// `start_at(PulsarPosition::earliest())` clause rewinds an existing subscription's cursor
    /// on every startup, replaying the retained backlog each time the service starts.
    Earliest,
    /// The tip of the log: only messages published after the seek are delivered.
    Latest,
    /// The position of a delivered message.
    MessageId(MessageIdData),
    /// Publish time, in milliseconds since the Unix epoch.
    Timestamp(u64),
}

impl PulsarPosition {
    /// The beginning of the log; see [`PulsarPosition::Earliest`] for how it interacts with a
    /// durable subscription's cursor.
    #[must_use]
    pub fn earliest() -> Self {
        Self::Earliest
    }

    /// The tip of the log; see [`PulsarPosition::Latest`].
    #[must_use]
    pub fn latest() -> Self {
        Self::Latest
    }

    /// A publish time, in milliseconds since the Unix epoch; see
    /// [`PulsarPosition::Timestamp`].
    #[must_use]
    pub fn timestamp(millis: u64) -> Self {
        Self::Timestamp(millis)
    }
}

/// A repositioning request shipped from a seeker handle to the subscription's driver task.
#[derive(Debug)]
pub(crate) struct SeekCmd {
    pub(crate) position: PulsarPosition,
    pub(crate) done: oneshot::Sender<Result<(), PulsarError>>,
}

/// A settlement request shipped from a message handle to the subscription's driver task
/// (the client's ack API needs `&mut Consumer`, which the driver owns).
#[derive(Debug)]
pub(crate) struct SettleCmd {
    pub(crate) topic: String,
    pub(crate) id: MessageIdData,
    pub(crate) kind: SettleKind,
    pub(crate) done: oneshot::Sender<Result<(), AckError>>,
}

/// Everything the driver task can be asked to do while its stream runs.
#[derive(Debug)]
pub(crate) enum DriverCmd {
    Settle(SettleCmd),
    Seek(SeekCmd),
}

pub(crate) type SettleSender = mpsc::UnboundedSender<DriverCmd>;

/// Refuses a delay the consumer's own timer would cut short.
///
/// A delayed retry is held in this process, so the delivery stays unacknowledged for the whole
/// delay. A consumer with an acknowledgement timeout redelivers an unacknowledged message on its
/// own once that timeout elapses, which would bring the message back before the delay is over.
/// The call says so rather than returning a delay it cannot keep.
pub(crate) fn within_ack_timeout(
    delay: Duration,
    ack_timeout: Option<Duration>,
    topic: &str,
) -> Result<(), AckError> {
    match ack_timeout {
        Some(timeout) if delay >= timeout => Err(AckError::Broker(Box::from(format!(
            "topic '{topic}': a retry delay of {delay:?} is not shorter than the subscription's              ack_timeout of {timeout:?}. A delayed retry is held in this process, and the              consumer redelivers an unacknowledged message once the ack timeout elapses, so the              message would come back early. Raise ack_timeout above the delay, or shorten it"
        )))),
        _ => Ok(()),
    }
}

/// A message delivered by a [`PulsarSubscriber`](crate::PulsarSubscriber).
///
/// `ack` acknowledges; `nack(requeue = true)` asks the broker to redeliver, which is what
/// drives the delivery count towards the subscription's dead-letter policy.
/// `nack(requeue = false)` acknowledges: Pulsar has no terminal reject verb - poison routing
/// belongs to the dead-letter policy, reached by repeated redelivery. The client queues
/// acknowledgements asynchronously, so `Ok` means "queued", not "broker confirmed".
///
/// `nack_after(delay)` is the delayed form, and the delay is this process's: Pulsar's negative
/// acknowledgement carries none, so the delivery is held unacknowledged until the delay is over
/// and the negative acknowledgement goes out then. The redelivery itself is the broker's, and so
/// is the count it advances.
pub struct PulsarMessage {
    payload: Bytes,
    headers: HeaderMap,
    topic: String,
    id: MessageIdData,
    settle: SettleSender,
    /// The subscription's acknowledgement timeout, which bounds how long a delayed retry may
    /// hold the delivery before the consumer redelivers it anyway.
    ack_timeout: Option<Duration>,
}

impl std::fmt::Debug for PulsarMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarMessage")
            .field("topic", &self.topic)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl PulsarMessage {
    pub(crate) fn new(
        message: &pulsar::consumer::Message<Vec<u8>>,
        settle: SettleSender,
        ack_timeout: Option<Duration>,
    ) -> Self {
        let metadata = message.metadata();
        let mut headers = HeaderMap::with_capacity(metadata.properties.len() + 1);
        for kv in &metadata.properties {
            headers.insert(kv.key.clone(), kv.value.clone());
        }
        if let Some(key) = &metadata.partition_key {
            // A header key is a shared string: the static form hands the map the constant itself
            // rather than a copy of it made on every delivery.
            headers.insert(Str::from_static(PARTITION_KEY_HEADER), key.clone());
        }
        Self {
            payload: Bytes::copy_from_slice(&message.payload.data),
            headers,
            topic: message.topic.clone(),
            id: message.message_id().clone(),
            settle,
            ack_timeout,
        }
    }

    /// The fully resolved topic this message arrived on (with its partition suffix when the
    /// topic is partitioned).
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The channel back to the subscription's driver task, which owns both settlement and
    /// seeking. The per-delivery context mints its seeker off this, so a delivery carries the
    /// reposition handle without the subscriber having to stamp one onto every message.
    pub(crate) fn driver(&self) -> &SettleSender {
        &self.settle
    }

    async fn send_settle(self, kind: SettleKind) -> Result<(), AckError> {
        let (done, wait) = oneshot::channel();
        self.settle
            .send(DriverCmd::Settle(SettleCmd {
                topic: self.topic,
                id: self.id,
                kind,
                done,
            }))
            .map_err(|_| {
                AckError::Broker(Box::from("the subscription's driver task has shut down"))
            })?;
        wait.await.map_err(|_| {
            AckError::Broker(Box::from("the subscription's driver task has shut down"))
        })?
    }
}

impl Positioned for PulsarMessage {
    type Position = PulsarPosition;

    fn position(&self) -> PulsarPosition {
        PulsarPosition::MessageId(self.id.clone())
    }
}

impl Partitioned for PulsarMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for PulsarMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.send_settle(SettleKind::Ack).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            self.send_settle(SettleKind::Nack).await
        } else {
            // Acknowledging IS the drop: Pulsar has no terminal reject, and the dead-letter
            // policy owns poison-message routing via repeated redelivery.
            self.send_settle(SettleKind::Ack).await
        }
    }

    /// The delay is kept by this process rather than by the broker, and that is still native
    /// delayed redelivery as far as the framework is concerned: no copy of the message is
    /// published, and the redelivery that follows is the broker's own, counted by the consumer's
    /// dead-letter policy like any other.
    fn supports_nack_after(&self) -> bool {
        true
    }

    /// Holds the delivery unacknowledged for `delay`, then negatively acknowledges it so the
    /// broker redelivers.
    ///
    /// The wait runs on a task of its own: the subscription's dispatch loop must not stop for it,
    /// and the delivery's settlement channel keeps the subscription's driver alive until the wait
    /// is over. `Ok` therefore means the delay was accepted, not that the redelivery has happened
    /// yet.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when the delay is not shorter than the subscription's
    /// `ack_timeout`, which would redeliver the message before the delay was over.
    ///
    /// # Cancel safety
    ///
    /// At-most-once over the delay window: the message is unacknowledged throughout, so a process
    /// that exits before the wait ends loses only the wait. The broker redelivers the message
    /// once the consumer's `ack_timeout` elapses, and at once where the consumer disconnects.
    fn nack_after(self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send {
        if let Err(err) = within_ack_timeout(delay, self.ack_timeout, &self.topic) {
            return ready(Err(err));
        }
        let topic = self.topic.clone();
        tokio::spawn(async move {
            sleep(delay).await;
            if let Err(err) = self.send_settle(SettleKind::Nack).await {
                warn!(
                    target: "ruststream_pulsar::subscriber",
                    topic = %topic,
                    delay = ?delay,
                    error = %err,
                    "a delayed retry could not be negatively acknowledged; the broker redelivers \
                     it once the consumer's ack timeout elapses",
                );
            }
        });
        ready(Ok(()))
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Builds the client message for an outgoing publish.
///
/// `key` is the partition key the call site named with
/// [`partition_key`](crate::PulsarPublishSteps::partition_key); it wins over a
/// [`PARTITION_KEY_HEADER`] header the call site wrote itself, and with neither the message
/// leaves unkeyed. Either way the key becomes the message's own `partition_key` rather than a
/// property, which is how it comes back on delivery.
pub(crate) fn to_pulsar_message(
    msg: OutgoingMessage<'_, BytesMut>,
    key: Option<&str>,
) -> pulsar::producer::Message {
    let (_topic, payload, headers) = msg.into_parts();
    let mut properties = HashMap::with_capacity(headers.len());
    let mut partition_key = key.map(ToOwned::to_owned);
    for (name, value) in headers.iter() {
        let text = String::from_utf8_lossy(value).into_owned();
        if name == PARTITION_KEY_HEADER {
            partition_key.get_or_insert(text);
        } else {
            properties.insert(name.to_owned(), text);
        }
    }
    pulsar::producer::Message {
        // The client owns the payload, and the buffer the framework wrote is the vector it
        // wants: taking it costs nothing where a copy costs the whole body.
        payload: Vec::from(payload),
        properties,
        partition_key,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_header_becomes_the_partition_key() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_pulsar_message(outgoing, None);
        assert_eq!(message.partition_key.as_deref(), Some("user-42"));
        assert_eq!(
            message.properties.get("x-tenant").map(String::as_str),
            Some("acme")
        );
        assert!(!message.properties.contains_key(PARTITION_KEY_HEADER));
    }

    #[test]
    fn the_call_site_key_wins_over_the_header() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-7");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_pulsar_message(outgoing, Some("user-42"));
        assert_eq!(message.partition_key.as_deref(), Some("user-42"));
        assert!(!message.properties.contains_key(PARTITION_KEY_HEADER));
    }

    /// The boundary is exclusive: a delay equal to the timeout would race the consumer's own
    /// redelivery, and the call refuses rather than picking a winner.
    #[test]
    fn a_delay_is_bounded_by_the_ack_timeout() {
        let timeout = Duration::from_secs(30);
        assert!(within_ack_timeout(Duration::from_secs(29), Some(timeout), "orders").is_ok());
        assert!(within_ack_timeout(Duration::from_secs(300), None, "orders").is_ok());

        let refused = within_ack_timeout(timeout, Some(timeout), "orders")
            .expect_err("a delay equal to the timeout must be refused");
        let message = format!("{refused:?}");
        assert!(message.contains("ack_timeout"), "{message}");
    }

    /// The client's message owns its payload, so the publish hands the buffer the framework
    /// wrote over rather than copying it. Address equality is the proof: a copy lands elsewhere.
    #[test]
    fn the_client_message_takes_the_buffer_the_publish_wrote() {
        let buffer = BytesMut::from(&b"{\"id\":1}"[..]);
        let at = buffer.as_ptr();
        let outgoing = OutgoingMessage::produced("orders", buffer);

        let message = to_pulsar_message(outgoing, None);

        assert_eq!(
            message.payload.as_ptr(),
            at,
            "the payload must be the buffer the publish wrote, not a copy of it",
        );
    }

    #[test]
    fn a_publish_that_names_no_key_leaves_unkeyed() {
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice());

        let message = to_pulsar_message(outgoing, None);
        assert_eq!(message.partition_key, None);
    }
}
