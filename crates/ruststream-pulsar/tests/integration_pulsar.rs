//! End-to-end checks against a standalone Pulsar, gated behind `PULSAR_TEST_URL`.
//!
//! Start one with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features -- --test-threads=1`.

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, DeclareRetryError, HeaderMap, IncomingMessage, Outgoing,
    OutgoingMessage, Publisher, RetryDeclaration, Seekable, Seeker, Serialized, Subscribe,
    Subscriber, SubscriptionSource, nonzero,
};
use ruststream_pulsar::{
    ConnectedPulsarBroker, PARTITION_KEY_HEADER, PulsarBroker, PulsarError, PulsarMessage,
    PulsarPosition, PulsarPublishSteps, PulsarSubscription,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(30);

/// The wait a deferred redelivery asks for. Long enough that half of it is a real gap on a live
/// broker, short enough to keep the suite quick.
const NACK_DELAY: Duration = Duration::from_secs(4);

/// The payload the builder-driven publishes carry: these tests assert on the transport, not on
/// a model, so the bytes name themselves as the wire form and no codec runs on them.
#[derive(Outgoing, Serialized)]
struct Record(&'static [u8]);

/// The broker to run the live checks against, or `None` to skip them.
///
/// Skipping quietly is what keeps these usable on a laptop with no stand running. It is also
/// what would let a renamed variable or a dropped `env:` block turn the whole live job green
/// without running anything, so CI sets `RUSTSTREAM_REQUIRE_LIVE` and the skip becomes a
/// failure there.
fn test_url() -> Option<String> {
    match std::env::var("PULSAR_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live suites must run, but \
                 PULSAR_TEST_URL is missing or empty",
            );
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

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&topic, b"{\"id\":1}".as_slice()).with_headers(headers),
            None,
        )
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

/// The setting has to reach the server as the message's own key, not as one more property: that
/// key is what keyed routing places by and what `KeyShared` orders by.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_partition_key_step_reaches_the_server() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("keyed");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .message(&Record(b"{\"id\":1}"))
        .to(topic.as_str())
        .partition_key("user-42")
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

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
        .publish(OutgoingMessage::new(&topic, b"again".as_slice()), None)
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

/// The one seek test that needs the wire beyond the framework's own `capabilities::seeking`
/// suite (which `conformance_pulsar.rs` runs against this broker): a multi-topic subscription
/// seeks per topic, a different client path the suite's single-name source never reaches.
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
            .publish(OutgoingMessage::new(topic, topic.as_bytes()), None)
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

/// The wait a `retry_after` outcome asks for, against a real consumer.
///
/// Pulsar's negative acknowledgement carries no delay, so the crate holds the delivery
/// unacknowledged and sends the negative acknowledgement when the delay is over. The proof is on
/// both sides of the deadline: nothing comes back while the wait runs, and the redelivery arrives
/// once it is done. The subscription names no `ack_timeout`, so the only timer in play is ours.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_nack_is_redelivered_after_the_delay() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("deferred");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"defer".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the first delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    assert_eq!(first.payload(), b"defer");
    first
        .nack_after(NACK_DELAY)
        .await
        .expect("a delay shorter than any consumer timer is accepted");

    // Well inside the wait: a redelivery here would mean the delay was dropped.
    assert!(
        tokio::time::timeout(NACK_DELAY / 2, stream.next())
            .await
            .is_err(),
        "the delivery came back before the delay was over",
    );

    let again = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delayed redelivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    assert_eq!(again.payload(), b"defer");
    again.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A delay the consumer's own timer would cut short is refused before anything waits, so a
/// service never holds a message past the point where the broker takes it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delay_past_the_ack_timeout_is_refused() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("bounded");
    let mut subscriber = connected
        .subscribe_descriptor(
            PulsarSubscription::new(&topic, unique("sub")).ack_timeout(NACK_DELAY),
        )
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"bounded".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");

    let refused = delivery
        .nack_after(NACK_DELAY * 2)
        .await
        .expect_err("a delay past the ack timeout must be refused");
    assert!(
        format!("{refused:?}").contains("ack_timeout"),
        "{refused:?}"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The registration's declaration reaching the real consumer.
///
/// The mount site's `max_attempts(..)` and `dead_letter(..)` arrive at the descriptor through
/// `declare_retry`, and this is the live proof that the client turns them into the topology it
/// says it does: a message nacked past the limit turns up on the declared topic. The call is
/// spelled out because the descriptor is a source for two brokers, so the bare method names no
/// impl.
fn declaring(
    subscription: PulsarSubscription,
    max_attempts: u32,
    dead_letter: String,
) -> PulsarSubscription {
    let declaration = RetryDeclaration::new()
        .with_max_attempts(max_attempts.try_into().expect("a non-zero cap"))
        .with_dead_letter(dead_letter);
    <PulsarSubscription as SubscriptionSource<ConnectedPulsarBroker>>::declare_retry(
        subscription,
        &declaration,
    )
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
        .subscribe_descriptor(declaring(
            PulsarSubscription::new(&topic, unique("sub")),
            2,
            dlq.clone(),
        ))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"poison".as_slice()), None)
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

/// The same declaration, made over a bare topic name.
///
/// A registration mounted as `#[subscriber("orders")]` has no descriptor to carry the
/// declaration, so the broker takes it in and builds the consumer for that name from it. This is
/// the live proof that the consumer it opens carries the policy, and that half a declaration is
/// refused before anything subscribes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declaration_over_a_bare_name_routes_exhausted_messages() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("named-poison");
    let dlq = unique("named-dlq");
    // Subscribe the DLQ first so its delivery is retained for us.
    let mut dlq_subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&dlq, unique("dlq-sub")))
        .await
        .expect("dlq subscription opens");

    let refused = connected
        .declare_retry(
            &topic,
            &RetryDeclaration::new().with_max_attempts(nonzero!(2u32)),
        )
        .expect_err("a cap with no destination is not a policy the client can apply");
    assert!(
        matches!(refused, DeclareRetryError::Broker(_)),
        "{refused:?}"
    );

    let declaration = RetryDeclaration::new()
        .with_max_attempts(nonzero!(2u32))
        .with_dead_letter(dlq.clone());
    connected
        .declare_retry(&topic, &declaration)
        .expect("the broker takes a whole declaration");
    let mut subscriber = connected
        .subscribe(&topic)
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"poison".as_slice()), None)
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
