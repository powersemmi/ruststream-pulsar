//! End-to-end checks against a standalone Pulsar, gated behind `PULSAR_TEST_URL`.
//!
//! Start one with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_pulsar::{
    ConnectedPulsarBroker, DeadLetter, PARTITION_KEY_HEADER, PulsarBroker, PulsarSubscription,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(30);

fn test_url() -> Option<String> {
    match std::env::var("PULSAR_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!("PULSAR_TEST_URL is not set; skipping the live integration test");
            None
        }
    }
}

async fn connect(url: &str) -> ConnectedPulsarBroker {
    PulsarBroker::new(url)
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique names, so runs do not observe each other's leftovers (subscriptions and
/// their backlog persist broker-side).
fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_properties_and_partition_key() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"{\"id\":1}".as_slice()).with_headers(headers))
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    assert_eq!(message.partition_key(), Some(b"user-42".as_slice()));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_with_requeue_redelivers() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("requeue");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"again".as_slice()))
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    first.nack(true).await.expect("nack succeeds");

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(second.payload(), b"again");
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_letter_policy_routes_exhausted_messages() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("poison");
    let dlq = unique("dlq");
    // Subscribe the DLQ first so its delivery is retained for us.
    let mut dlq_subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&dlq, unique("dlq-sub")))
        .await
        .expect("dlq subscription opens");
    let mut subscriber = connected
        .subscribe_descriptor(
            PulsarSubscription::new(&topic, unique("sub"))
                .dead_letter(DeadLetter::new(&dlq).max_deliveries(2)),
        )
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"poison".as_slice()))
        .await
        .expect("publish succeeds");

    // Nack until the delivery count exceeds the policy and the broker reroutes.
    let mut stream = pin!(subscriber.stream());
    for _ in 0..4 {
        match tokio::time::timeout(RECV_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(message))) => message.nack(true).await.expect("nack succeeds"),
            Ok(Some(Err(err))) => panic!("subscription failed: {err}"),
            Ok(None) => panic!("subscription ended"),
            Err(_) => break, // no more redeliveries: the message moved to the DLQ
        }
    }

    let mut dlq_stream = pin!(dlq_subscriber.stream());
    let dead = tokio::time::timeout(RECV_TIMEOUT, dlq_stream.next())
        .await
        .expect("dead letter arrives")
        .expect("dlq stream is open")
        .expect("dead letter is ok");
    assert_eq!(dead.payload(), b"poison");
    dead.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}
