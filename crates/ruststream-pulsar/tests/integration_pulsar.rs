//! End-to-end checks against a standalone Pulsar, gated behind `PULSAR_TEST_URL`.
//!
//! Start one with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features -- --test-threads=1`.

mod live;

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, DeclareRetryError, HeaderMap, IncomingMessage,
    Outgoing, OutgoingMessage, Publisher, RetryDeclaration, Seekable, Seeker, Serialized,
    Subscribe, Subscriber, SubscriptionSource, nonzero,
};
use ruststream_pulsar::{
    ConnectedPulsarBroker, PARTITION_KEY_HEADER, PulsarBroker, PulsarError, PulsarMessage,
    PulsarPosition, PulsarPublishSteps, PulsarSubscriber, PulsarSubscription, SubscriptionType,
};

use crate::live::{RECV_TIMEOUT, admin, connect, test_url, unique};

/// The wait a deferred redelivery asks for. Long enough that half of it is a real gap on a live
/// broker, short enough to keep the suite quick.
const NACK_DELAY: Duration = Duration::from_secs(4);

/// How long a subscription that should be quiet is watched before the claim is taken as proved.
///
/// Every redelivery below follows a negative acknowledgement or a timer this suite drives, so
/// this bounds a claim about nothing happening rather than a race with the broker.
const QUIET: Duration = Duration::from_secs(5);

/// The retry cap the dead-letter suites declare: two deliveries, then the declared topic.
const CAP: u32 = 2;

/// The payload the builder-driven publishes carry: these tests assert on the transport, not on
/// a model, so the bytes name themselves as the wire form and no codec runs on them.
#[derive(Outgoing, Serialized)]
struct Record(&'static [u8]);

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

/// Hands `cap` deliveries back for redelivery and returns how many the subscription made, one
/// more than the cap included.
///
/// It reads one delivery past the cap on purpose: the client hands a spent message to the
/// dead-letter topic instead of to this stream, so a subscription that goes quiet there is the
/// cap doing its work and a delivery that arrives anyway is the cap not being applied.
async fn spend(subscriber: &mut PulsarSubscriber, cap: usize) -> usize {
    let mut stream = pin!(subscriber.stream());
    let mut deliveries = 0;
    for attempt in 0..=cap {
        // A redelivery follows a negative acknowledgement at once, so the wait past the cap is
        // the quiet one and stays short.
        let wait = if attempt < cap { RECV_TIMEOUT } else { QUIET };
        match tokio::time::timeout(wait, stream.next()).await {
            Ok(Some(Ok(message))) => {
                deliveries += 1;
                message.nack(true).await.expect("nack succeeds");
            }
            Ok(Some(Err(err))) => panic!("subscription failed: {err}"),
            Ok(None) => panic!("subscription ended"),
            Err(_) => break,
        }
    }
    deliveries
}

/// The cap counts the deliveries the registration asked for, and the spent one leaves the
/// subscription for the topic the declaration named.
///
/// The number is the claim: a client that counted its own redeliveries differently, or a broker
/// that counted none, would still put the message in the dead-letter topic eventually or never,
/// and only the count says which of the two is happening.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cap_is_spent_at_the_declared_number_of_deliveries() {
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
            CAP,
            dlq.clone(),
        ))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"poison".as_slice()), None)
        .await
        .expect("publish succeeds");

    assert_eq!(
        spend(&mut subscriber, CAP as usize).await,
        CAP as usize,
        "the handler must see the declared number of deliveries and not one more",
    );

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

/// The other subscription type the broker counts redeliveries on, since the cap is that count
/// read against a limit and a `KeyShared` consumer reaches it through a different dispatcher.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_shared_subscription_spends_the_same_cap() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("keyed-poison");
    let dlq = unique("keyed-dlq");
    let mut dlq_subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&dlq, unique("dlq-sub")))
        .await
        .expect("dlq subscription opens");
    let mut subscriber = connected
        .subscribe_descriptor(declaring(
            PulsarSubscription::new(&topic, unique("sub"))
                .subscription_type(SubscriptionType::KeyShared),
            CAP,
            dlq.clone(),
        ))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .message(&Record(b"poison"))
        .to(topic.as_str())
        .partition_key("user-42")
        .publish()
        .await
        .expect("publish succeeds");

    assert_eq!(
        spend(&mut subscriber, CAP as usize).await,
        CAP as usize,
        "the handler must see the declared number of deliveries and not one more",
    );

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
    let connected = PulsarBroker::new(&url)
        .default_subscription(unique("by-name"))
        .connect()
        .await
        .expect("broker connects");

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
        .with_max_attempts(CAP.try_into().expect("a non-zero cap"))
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

    assert_eq!(
        spend(&mut subscriber, CAP as usize).await,
        CAP as usize,
        "the handler must see the declared number of deliveries and not one more",
    );

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

/// A bare topic name joins the subscription the broker names as its default, and the server
/// reports that name and no other; a broker that names none refuses a bare name before anything
/// subscribes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_joins_the_default_subscription_the_broker_names() {
    let Some(url) = test_url() else { return };
    let topic = unique("by-name-default");
    let subscription = unique("orders-worker");

    let unnamed = PulsarBroker::new(&url)
        .connect()
        .await
        .expect("broker connects");
    let refused = unnamed
        .subscribe(&topic)
        .await
        .expect_err("a bare name without a default subscription must not open");
    let advice = refused.to_string();
    assert!(matches!(refused, PulsarError::Invalid(_)), "{advice}");
    assert!(
        advice.contains("PulsarBroker::default_subscription"),
        "{advice}"
    );
    unnamed.shutdown().await.expect("shutdown succeeds");

    let connected = PulsarBroker::new(&url)
        .default_subscription(&subscription)
        .connect()
        .await
        .expect("broker connects");
    let subscriber = connected
        .subscribe(&topic)
        .await
        .expect("subscription opens");
    assert_eq!(
        admin::subscription_names(&topic).await,
        BTreeSet::from([subscription]),
        "the server holds the cursor under the name the broker set, and under no other",
    );

    drop(subscriber);
    connected.shutdown().await.expect("shutdown succeeds");
}

/// Dropping a delivery settles it, and the broker agrees: Pulsar has no terminal reject, so
/// `nack(requeue = false)` acknowledges, and a subscription whose backlog and unacknowledged
/// count are both zero is the server saying the message will not come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_delivery_is_acknowledged() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("dropped");
    let subscription = unique("sub");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, &subscription))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"gone".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    delivery.nack(false).await.expect("dropping succeeds");

    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "a dropped delivery must not come back",
    );

    let stats = admin::subscription_stats(&topic, &subscription).await;
    assert_eq!(
        stats.get("msgBacklog").and_then(serde_json::Value::as_u64),
        Some(0),
        "the broker still holds the message: {stats}",
    );
    assert_eq!(
        stats
            .get("unackedMessages")
            .and_then(serde_json::Value::as_u64),
        Some(0),
        "the broker still counts the delivery unsettled: {stats}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// What the crate documents about the redelivery count: the client keeps the broker's count for
/// its own dead-letter decision and puts none on the message, so a handler that wants to know
/// how often a message has come round has nothing to read here.
///
/// The redelivery is what makes the claim worth pinning: a count that appeared on a second
/// delivery would make `redelivery_count` half-implemented rather than absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_carries_no_redelivery_count() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("counted");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"again".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the first delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    assert_eq!(first.redelivery_count(), None);
    first.nack(true).await.expect("nack succeeds");

    let again = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the redelivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    assert_eq!(
        again.redelivery_count(),
        None,
        "a redelivery must not start reporting a count the first delivery had none of",
    );
    again.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The descriptor's `ack_timeout` is the consumer's own redelivery timer, and this is the proof
/// it reaches the server's consumer rather than only bounding a delayed retry: a delivery nobody
/// settles comes back, and not before the timeout is over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsettled_delivery_comes_back_after_the_ack_timeout() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("unsettled");
    let mut subscriber = connected
        .subscribe_descriptor(
            PulsarSubscription::new(&topic, unique("sub")).ack_timeout(NACK_DELAY),
        )
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"unsettled".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    // Settled by nobody: the consumer's timer is the only thing that can bring it back.
    drop(delivery);

    assert!(
        tokio::time::timeout(NACK_DELAY / 2, stream.next())
            .await
            .is_err(),
        "the delivery came back before the acknowledgement timeout was over",
    );
    let again = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the timer redelivers")
        .expect("the subscription is open")
        .expect("the delivery is ok");
    assert_eq!(again.payload(), b"unsettled");
    again.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The setting's other edge: a publish that names no key leaves the message unkeyed, rather than
/// inventing one out of the payload or carrying the header spelling as a property.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_that_names_no_key_leaves_the_delivery_unkeyed() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("unkeyed");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .message(&Record(b"{\"id\":1}"))
        .to(topic.as_str())
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok");

    assert_eq!(delivery.partition_key(), None);
    assert_eq!(delivery.headers().get(PARTITION_KEY_HEADER), None);
    delivery.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The descriptor's `batch_wait` is the deadline that closes a partial batch, and it is the
/// descriptor's own value: a batch smaller than the size the registration named waits for it,
/// and arrives when it is over.
///
/// The client hands over one delivery at a time, so this is the framework's buffer over a live
/// consumer, opened at a size the run never fills.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_batch_closes_at_the_batch_wait_deadline() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("partial");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")).batch_wait(NACK_DELAY))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    for id in 0..2u8 {
        publisher
            .publish(OutgoingMessage::new(&topic, &[id]), None)
            .await
            .expect("publish succeeds");
    }

    let mut batches = pin!(subscriber.batches(nonzero!(8usize)));
    assert!(
        tokio::time::timeout(NACK_DELAY / 2, batches.next())
            .await
            .is_err(),
        "a partial batch closed before the deadline the descriptor named",
    );
    let batch = tokio::time::timeout(RECV_TIMEOUT, batches.next())
        .await
        .expect("the deadline closes the batch")
        .expect("the subscription is open")
        .expect("the batch is ok");
    assert_eq!(batch.len(), 2, "the batch must carry what had arrived");
    for message in batch {
        message.ack().await.expect("ack succeeds");
    }
}
