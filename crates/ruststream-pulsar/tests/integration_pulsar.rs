//! End-to-end checks against a standalone Pulsar, gated behind `PULSAR_TEST_URL`.
//!
//! Start one with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features -- --test-threads=1`.

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher, Seekable,
    Seeker, Subscriber,
};
use ruststream_pulsar::{
    ConnectedPulsarBroker, DeadLetter, PARTITION_KEY_HEADER, PulsarBroker, PulsarError,
    PulsarMessage, PulsarPosition, PulsarSubscription,
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
async fn seeking_to_earliest_replays_the_retained_backlog() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("replay");
    // The subscription exists before the first publish, so the broker retains all three for
    // it; draining them leaves a cursor at the tip, which is what the seek has to rewind.
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    for id in 1..=3u8 {
        publisher
            .publish(OutgoingMessage::new(&topic, [id].as_slice()))
            .await
            .expect("publish succeeds");
    }

    {
        let mut stream = pin!(subscriber.stream());
        for id in 1..=3u8 {
            let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            assert_eq!(message.payload(), [id]);
            message.ack().await.expect("ack succeeds");
        }
    }

    subscriber
        .seeker()
        .seek(PulsarPosition::earliest())
        .await
        .expect("seek to the beginning succeeds");

    let mut stream = pin!(subscriber.stream());
    for id in 1..=3u8 {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("replayed delivery arrives")
            .expect("stream is open")
            .expect("replayed delivery is ok");
        assert_eq!(message.payload(), [id]);
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A multi-topic subscription seeks per topic, a different client path from the single-topic
/// one, so the capability is pinned on both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeking_to_earliest_replays_every_topic_of_a_multi_topic_subscription() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let first = unique("multi-a");
    let second = unique("multi-b");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::topics(
            [first.as_str(), second.as_str()],
            unique("sub"),
        ))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    for topic in [&first, &second] {
        publisher
            .publish(OutgoingMessage::new(topic, topic.as_bytes()))
            .await
            .expect("publish succeeds");
    }
    let sent: BTreeSet<Vec<u8>> = [first.into_bytes(), second.into_bytes()].into();

    // Deliveries interleave across the topics, so the set is what carries the assertion.
    {
        let mut stream = pin!(subscriber.stream());
        assert_eq!(drain(&mut stream, sent.len()).await, sent);
    }

    subscriber
        .seeker()
        .seek(PulsarPosition::earliest())
        .await
        .expect("seek to the beginning succeeds");

    let mut stream = pin!(subscriber.stream());
    assert_eq!(drain(&mut stream, sent.len()).await, sent);

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Takes `count` deliveries, acknowledging each, and returns their payloads.
async fn drain(
    stream: &mut (impl StreamExt<Item = Result<PulsarMessage, PulsarError>> + Unpin),
    count: usize,
) -> BTreeSet<Vec<u8>> {
    let mut payloads = BTreeSet::new();
    for _ in 0..count {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        payloads.insert(message.payload().to_vec());
        message.ack().await.expect("ack succeeds");
    }
    payloads
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
