//! [`PulsarMessage`] and the mapping between `RustStream` headers and message properties.
//!
//! Message properties carry headers directly - no envelope format is invented - and the
//! partition key rides the message's own `partition_key` in both directions.

use bytes::Bytes;
use pulsar::proto::MessageIdData;
use ruststream::{AckError, Headers, IncomingMessage, OutgoingMessage, Partitioned, Positioned};
use tokio::sync::{mpsc, oneshot};

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

/// A message delivered by a [`PulsarSubscriber`](crate::PulsarSubscriber).
///
/// `ack` acknowledges; `nack(requeue = true)` asks the broker to redeliver, which is what
/// drives the delivery count towards the subscription's dead-letter policy.
/// `nack(requeue = false)` acknowledges: Pulsar has no terminal reject verb - poison routing
/// belongs to the dead-letter policy, reached by repeated redelivery. The client queues
/// acknowledgements asynchronously, so `Ok` means "queued", not "broker confirmed".
pub struct PulsarMessage {
    payload: Bytes,
    headers: Headers,
    topic: String,
    id: MessageIdData,
    settle: SettleSender,
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
    pub(crate) fn new(message: &pulsar::consumer::Message<Vec<u8>>, settle: SettleSender) -> Self {
        let metadata = message.metadata();
        let mut headers = Headers::with_capacity(metadata.properties.len() + 1);
        for kv in &metadata.properties {
            headers.insert(kv.key.clone(), kv.value.clone());
        }
        if let Some(key) = &metadata.partition_key {
            headers.insert(PARTITION_KEY_HEADER, key.clone());
        }
        Self {
            payload: Bytes::copy_from_slice(&message.payload.data),
            headers,
            topic: message.topic.clone(),
            id: message.message_id().clone(),
            settle,
        }
    }

    /// The fully resolved topic this message arrived on (with its partition suffix when the
    /// topic is partitioned).
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
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

    fn headers(&self) -> &Headers {
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

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Builds the client message for an outgoing publish.
pub(crate) fn to_pulsar_message(msg: &OutgoingMessage<'_>) -> pulsar::producer::Message {
    let headers = msg.headers();
    let mut properties = std::collections::HashMap::with_capacity(headers.len());
    let mut partition_key = None;
    for (name, value) in headers.iter() {
        let text = String::from_utf8_lossy(value).into_owned();
        if name == PARTITION_KEY_HEADER {
            partition_key = Some(text);
        } else {
            properties.insert(name.to_owned(), text);
        }
    }
    pulsar::producer::Message {
        payload: msg.payload().to_vec(),
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
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_pulsar_message(&outgoing);
        assert_eq!(message.partition_key.as_deref(), Some("user-42"));
        assert_eq!(
            message.properties.get("x-tenant").map(String::as_str),
            Some("acme")
        );
        assert!(!message.properties.contains_key(PARTITION_KEY_HEADER));
    }
}
