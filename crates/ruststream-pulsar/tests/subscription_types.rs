//! What the subscription type does to delivery, in process.
//!
//! A message reaches every subscription over its topic; within one subscription, the type
//! decides which consumer takes it. That is the rule a service writes tests about - two workers
//! on one subscription share the stream, they do not each run it - so the stand-in has to apply
//! it rather than fan out to everyone and let the test pass for the wrong reason.
//!
//! The subject here is the transport's own selection, so these drive `PulsarTestBroker` through
//! the broker traits; the last case is the service-level statement of the same thing, under
//! `TestApp`.
#![cfg(feature = "testing")]

use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{Broker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::{
    ConnectedPulsarTestBroker, PulsarTestBroker, PulsarTestSubscriber,
};
use ruststream_pulsar::{PARTITION_KEY_HEADER, PulsarError};
use serde::{Deserialize, Serialize};

/// How long a drain waits for a delivery that is not there. Every publish below lands in the
/// router before the drain starts, so this bounds the empty read rather than a race.
const QUIET: Duration = Duration::from_millis(100);

async fn connected() -> ConnectedPulsarTestBroker {
    PulsarTestBroker::new()
        .connect()
        .await
        .expect("the stand-in connects")
}

fn subscription(topic: &str, name: &str, sharing: SubscriptionType) -> PulsarSubscription {
    PulsarSubscription::new(topic, name).subscription_type(sharing)
}

async fn publish(broker: &ConnectedPulsarTestBroker, topic: &str, payload: &str) {
    broker
        .publisher()
        .publish(OutgoingMessage::new(topic, payload.as_bytes()), None)
        .await
        .expect("publish");
}

async fn publish_keyed(broker: &ConnectedPulsarTestBroker, topic: &str, key: &str, payload: &str) {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, key.to_owned());
    broker
        .publisher()
        .publish(
            OutgoingMessage::new(topic, payload.as_bytes()).with_headers(headers),
            None,
        )
        .await
        .expect("publish");
}

/// Everything queued for one consumer, acked on the way out.
async fn drain(subscriber: &mut PulsarTestSubscriber) -> Vec<String> {
    let mut stream = Box::pin(subscriber.stream());
    let mut seen = Vec::new();
    while let Ok(Some(delivery)) = tokio::time::timeout(QUIET, stream.next()).await {
        let msg = delivery.expect("delivery ok");
        seen.push(String::from_utf8_lossy(msg.payload()).into_owned());
        msg.ack().await.expect("ack");
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_subscription_hands_each_message_to_one_consumer() {
    let broker = connected().await;
    let shared = || subscription("orders", "workers", SubscriptionType::Shared);
    let mut first = broker
        .subscribe_descriptor(shared())
        .await
        .expect("the first consumer attaches");
    let mut second = broker
        .subscribe_descriptor(shared())
        .await
        .expect("a second consumer joins a shared subscription");

    for id in 1..=4 {
        publish(&broker, "orders", &format!("o{id}")).await;
    }

    // Every message went to exactly one consumer, in rotation.
    assert_eq!(drain(&mut first).await, ["o1", "o3"]);
    assert_eq!(drain(&mut second).await, ["o2", "o4"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_subscriptions_each_receive_the_whole_stream() {
    let broker = connected().await;
    let mut workers = broker
        .subscribe_descriptor(PulsarSubscription::new("orders", "workers"))
        .await
        .expect("subscribe");
    let mut audit = broker
        .subscribe_descriptor(PulsarSubscription::new("orders", "audit"))
        .await
        .expect("subscribe");

    publish(&broker, "orders", "o1").await;
    publish(&broker, "orders", "o2").await;

    // Sharing is within a subscription; two subscriptions over one topic each get a copy.
    assert_eq!(drain(&mut workers).await, ["o1", "o2"]);
    assert_eq!(drain(&mut audit).await, ["o1", "o2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exclusive_subscription_refuses_a_second_consumer() {
    let broker = connected().await;
    let exclusive = || subscription("orders", "solo", SubscriptionType::Exclusive);
    let _held = broker
        .subscribe_descriptor(exclusive())
        .await
        .expect("the first consumer takes the subscription");

    let err = broker
        .subscribe_descriptor(exclusive())
        .await
        .expect_err("a second consumer on an exclusive subscription must be refused");

    assert!(matches!(err, PulsarError::Subscribe { .. }));
    let reported = err.to_string();
    assert!(
        reported.contains("solo") && reported.contains("orders"),
        "the refusal must name the subscription and the topic, got: {reported}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failover_subscription_delivers_to_the_active_consumer() {
    let broker = connected().await;
    let failover = || subscription("orders", "failover", SubscriptionType::Failover);
    let mut active = broker
        .subscribe_descriptor(failover())
        .await
        .expect("the active consumer attaches");
    let mut standby = broker
        .subscribe_descriptor(failover())
        .await
        .expect("a standby attaches");

    publish(&broker, "orders", "o1").await;
    publish(&broker, "orders", "o2").await;

    assert_eq!(drain(&mut active).await, ["o1", "o2"]);
    assert!(
        drain(&mut standby).await.is_empty(),
        "a standby receives nothing while the active consumer holds the subscription",
    );

    drop(active);
    publish(&broker, "orders", "o3").await;

    assert_eq!(
        drain(&mut standby).await,
        ["o3"],
        "the standby takes over when the active consumer leaves",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_shared_subscription_keeps_a_key_on_one_consumer() {
    let broker = connected().await;
    let keyed = || subscription("orders", "keyed", SubscriptionType::KeyShared);
    let mut first = broker
        .subscribe_descriptor(keyed())
        .await
        .expect("subscribe");
    let mut second = broker
        .subscribe_descriptor(keyed())
        .await
        .expect("subscribe");

    for (key, payload) in [
        ("tenant-a", "m1"),
        ("tenant-b", "m2"),
        ("tenant-a", "m3"),
        ("tenant-b", "m4"),
    ] {
        publish_keyed(&broker, "orders", key, payload).await;
    }

    let one = drain(&mut first).await;
    let two = drain(&mut second).await;
    assert_eq!(one.len() + two.len(), 4, "every message reached a consumer");

    // Which consumer a key lands on is the stand-in's own hash and not a server's, so the claim
    // is the one a test can rest on either way: a key does not move between consumers.
    let together = |left: &str, right: &str| {
        one.iter().any(|seen| seen == left) == one.iter().any(|seen| seen == right)
    };
    assert!(together("m1", "m3"), "'tenant-a' must stay on one consumer");
    assert!(together("m2", "m4"), "'tenant-b' must stay on one consumer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeue_returns_to_the_subscription_not_to_the_consumer() {
    let broker = connected().await;
    let shared = || subscription("orders", "workers", SubscriptionType::Shared);
    let mut first = broker
        .subscribe_descriptor(shared())
        .await
        .expect("subscribe");
    let mut second = broker
        .subscribe_descriptor(shared())
        .await
        .expect("subscribe");

    publish(&broker, "orders", "o1").await;

    let mut stream = Box::pin(first.stream());
    let msg = tokio::time::timeout(QUIET, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(msg.payload(), b"o1");
    msg.nack(true).await.expect("requeue");
    drop(stream);

    assert!(
        drain(&mut first).await.is_empty(),
        "the retry goes back to the subscription, not to the consumer that gave up on it",
    );
    assert_eq!(drain(&mut second).await, ["o1"]);
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Task {
    id: u64,
}

#[subscriber(PulsarSubscription::new("tasks", "workers"))]
async fn worker_one(task: &Task) -> HandlerOutcome {
    let _ = task;
    HandlerOutcome::ack()
}

#[subscriber(PulsarSubscription::new("tasks", "workers"))]
async fn worker_two(task: &Task) -> HandlerOutcome {
    let _ = task;
    HandlerOutcome::ack()
}

/// The same rule where a service meets it: two handlers mounted on one shared subscription are
/// competing consumers, so a run of four messages is four handler calls between them - not four
/// each, which is what a stand-in that fans out to every consumer would report.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_handlers_split_the_stream_under_the_harness() {
    let app = RustStream::new(AppInfo::new("workers", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(worker_one);
            b.include(worker_two);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    for id in 1..=4 {
        tb.broker::<PulsarTestBroker>()
            .message(&Task { id })
            .to("tasks")
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<PulsarTestBroker>()
        .subscriber("tasks")
        .assert_called(4)
        .settled(HandlerOutcome::ack());
}
