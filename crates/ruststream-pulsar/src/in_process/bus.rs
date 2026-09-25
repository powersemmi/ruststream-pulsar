//! [`Bus`]: the in-process transport one connected broker and every handle paired off it share.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{BytesMut, HeaderMap, OutgoingMessage, RawMessage, Str};
use tokio::runtime::Handle;

use crate::error::PulsarError;
use crate::in_process::router::AddressRouter;
use crate::message::{PARTITION_KEY_HEADER, to_pulsar_message};
use crate::topic::PulsarTopic;

/// The in-process transport: the router with its retained log, the harness coordinator it
/// counts in-flight deliveries with, and the runtime the broker connected on, which a delayed
/// redelivery waits on.
#[derive(Debug)]
pub(crate) struct Bus {
    router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    runtime: Handle,
}

impl Bus {
    pub(crate) fn new(runtime: Handle) -> Arc<Self> {
        Arc::new(Self {
            router: AddressRouter::default(),
            coordinator: OnceLock::new(),
            runtime,
        })
    }

    /// The runtime the broker connected on: a task the transport starts on its own behalf runs
    /// there, whichever thread settles the delivery that asked for it.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }

    pub(crate) const fn router(&self) -> &AddressRouter {
        &self.router
    }

    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// Installs the harness coordinator. A second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// Stores a publish and delivers it, framed the way the live publisher frames it.
    ///
    /// The destination is qualified as the server qualifies it, and the message goes through the
    /// same conversion a producer sends: header values become the text properties Pulsar carries
    /// (a value that is not UTF-8 arrives with replacement characters, as it does from a
    /// server), and the partition key becomes the message's own key, which a delivery reports as
    /// the `partition-key` header.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Invalid`] for a destination that is no topic name, which the live
    /// publisher refuses before it opens a producer.
    pub(crate) fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        key: Option<&str>,
    ) -> Result<(), PulsarError> {
        let topic = PulsarTopic::parse(msg.name())?;
        let message = to_pulsar_message(msg, key);
        let headers = delivered_headers(message.properties, message.partition_key);
        self.router.publish(
            topic.as_str(),
            Bytes::from(message.payload),
            headers,
            self.coordinator(),
        );
        Ok(())
    }

    /// Every message stored on `name`, whichever spelling of the topic it names.
    pub(crate) fn published(&self, name: &str) -> Vec<RawMessage> {
        PulsarTopic::parse(name).map_or_else(
            |_| Vec::new(),
            |topic| self.router.published(topic.as_str()),
        )
    }

    /// Detaches every consumer, which ends their streams, and drops the log.
    pub(crate) fn close(&self) {
        self.router.clear();
    }
}

/// The headers a delivery of `properties` and `partition_key` reports, as the live subscriber
/// reads them off a message's metadata.
fn delivered_headers(
    properties: HashMap<String, String>,
    partition_key: Option<String>,
) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(properties.len() + 1);
    for (name, value) in properties {
        headers.insert(name, value);
    }
    if let Some(key) = partition_key {
        headers.insert(Str::from_static(PARTITION_KEY_HEADER), key);
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The log keeps the buffer the publish wrote, as the client does: the bytes it stored are the
    /// ones the framework encoded, at the same address.
    #[tokio::test]
    async fn a_publish_hands_the_buffer_to_the_log() {
        let bus = Bus::new(Handle::current());
        let buffer = BytesMut::from(&b"{\"id\":1}"[..]);
        let at = buffer.as_ptr();

        bus.publish(OutgoingMessage::produced("orders", buffer), None)
            .expect("a topic name is accepted");

        let logged = bus.published("orders");
        assert_eq!(
            logged
                .first()
                .expect("the publish reached the log")
                .payload()
                .as_ptr(),
            at,
            "the log must hold the buffer the publish wrote, not a copy of it",
        );
    }

    /// A destination the live publisher refuses is refused here, with the same error.
    #[tokio::test]
    async fn a_destination_that_is_no_topic_is_refused() {
        let bus = Bus::new(Handle::current());
        let refused = bus
            .publish(
                OutgoingMessage::produced("a/b", BytesMut::from(&b"{}"[..])),
                None,
            )
            .expect_err("a two-part name is no topic");
        assert!(matches!(refused, PulsarError::Invalid(_)), "{refused}");
    }

    /// Pulsar carries properties as text, so a header value that is not UTF-8 arrives the way a
    /// server delivers it, not as the bytes the publish wrote.
    #[tokio::test]
    async fn a_header_arrives_as_the_text_pulsar_carries() {
        let bus = Bus::new(Handle::current());
        let mut headers = HeaderMap::new();
        headers.insert("x-raw", vec![0xff, b'a']);
        bus.publish(
            OutgoingMessage::produced("orders", BytesMut::from(&b"{}"[..])).with_headers(headers),
            None,
        )
        .expect("a topic name is accepted");

        let logged = bus.published("persistent://public/default/orders");
        assert_eq!(
            logged[0].headers().get("x-raw"),
            Some("\u{fffd}a".as_bytes()),
        );
    }
}
