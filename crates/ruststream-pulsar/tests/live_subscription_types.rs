//! What the subscription type does on a real broker.
//!
//! The type decides which consumer of a subscription takes a message, and the server decides it:
//! the in-process mode reproduces the rule so a service's tests run on it, but the rule itself
//! belongs to Pulsar. So each type is driven here against a server, and the
//! server is asked what it recorded for the subscription, which is the only way to tell a
//! descriptor that reached the consumer from one that was read and dropped.
//!
//! Start the stand with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features -- --test-threads=1`.

mod live;

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    RetryDeclaration, Subscriber, SubscriptionSource, nonzero,
};
use ruststream_pulsar::{
    ConnectedPulsarBroker, OperationRetries, PARTITION_KEY_HEADER, PulsarBroker, PulsarError,
    PulsarSubscriber, PulsarSubscription, SubscriptionType,
};

use crate::live::{RECV_TIMEOUT, admin, connect, test_url, unique};

/// How long a consumer that should have nothing is watched before the claim is taken as proved.
///
/// Every publish below is awaited to its receipt before a drain starts, so this bounds a claim
/// about an empty consumer rather than a race with the broker.
const QUIET: Duration = Duration::from_secs(3);

/// How long an attach that the server answered "busy" is watched, which is past the client's own
/// retry delay of five seconds: a shorter wait would only say the attach was slow.
const BUSY_WAIT: Duration = Duration::from_secs(7);

/// Everything queued for one consumer, acknowledged on the way out.
async fn drain(subscriber: &mut PulsarSubscriber) -> Vec<String> {
    let mut stream = pin!(subscriber.stream());
    let mut seen = Vec::new();
    while let Ok(Some(delivery)) = tokio::time::timeout(QUIET, stream.next()).await {
        let message = delivery.expect("delivery is ok");
        seen.push(String::from_utf8_lossy(message.payload()).into_owned());
        message.ack().await.expect("ack succeeds");
    }
    seen
}

async fn publish(connected: &ConnectedPulsarBroker, topic: &str, payload: &str) {
    connected
        .publisher()
        .publish(OutgoingMessage::new(topic, payload.as_bytes()), None)
        .await
        .expect("publish succeeds");
}

async fn publish_keyed(connected: &ConnectedPulsarBroker, topic: &str, key: &str, payload: &str) {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, key.to_owned());
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(topic, payload.as_bytes()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");
}

/// The server's own record of the subscription, so a descriptor's type cannot be read and
/// dropped on the way to the consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_reports_the_type_the_descriptor_named() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    // Pulsar's own spelling of the four, which is what the admin API answers with.
    for (sharing, reported) in [
        (SubscriptionType::Exclusive, "Exclusive"),
        (SubscriptionType::Shared, "Shared"),
        (SubscriptionType::Failover, "Failover"),
        (SubscriptionType::KeyShared, "Key_Shared"),
    ] {
        let topic = unique(&format!("typed-{reported}"));
        let subscription = unique("sub");
        let held = connected
            .subscribe_descriptor(
                PulsarSubscription::new(&topic, &subscription).subscription_type(sharing),
            )
            .await
            .expect("subscription opens");

        let stats = admin::subscription_stats(&topic, &subscription).await;
        assert_eq!(
            stats.get("type").and_then(serde_json::Value::as_str),
            Some(reported),
            "the server recorded another type for {sharing:?}: {stats}",
        );
        drop(held);
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// An exclusive subscription is one consumer's, and the second one waits for it.
///
/// The server answers the second attach with "consumer busy" and the client retries that answer
/// rather than surfacing it, so the call neither returns a subscriber nor an error while the
/// first consumer holds the subscription, and completes once it leaves. That is the behaviour a
/// service meets, and it is not the one the in-process mode has: there a second attach is
/// refused, which is where a test and a deployment part company.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_consumer_of_an_exclusive_subscription_waits_for_the_first() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("solo");
    let subscription = unique("sub");
    let exclusive = || {
        PulsarSubscription::new(&topic, &subscription)
            .subscription_type(SubscriptionType::Exclusive)
    };
    let held = connected
        .subscribe_descriptor(exclusive())
        .await
        .expect("the first consumer takes the subscription");

    // Past the client's retry delay, so the attach has been refused by the server once and put
    // back in the queue rather than merely being slow.
    let mut second = pin!(connected.subscribe_descriptor(exclusive()));
    assert!(
        tokio::time::timeout(BUSY_WAIT, &mut second).await.is_err(),
        "a second consumer must neither take an exclusive subscription nor be refused while the \
         first holds it",
    );

    drop(held);
    tokio::time::timeout(RECV_TIMEOUT, second)
        .await
        .expect("the waiting consumer takes the subscription once the holder leaves")
        .expect("subscription opens");
}

/// The bound on the client's retries is what turns that wait into an answer: with a few tries
/// named, the same second attach reports what the server said instead of queueing behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bounded_client_reports_the_exclusive_subscription_as_taken() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("taken");
    let subscription = unique("sub");
    let exclusive = || {
        PulsarSubscription::new(&topic, &subscription)
            .subscription_type(SubscriptionType::Exclusive)
    };
    let _held = connected
        .subscribe_descriptor(exclusive())
        .await
        .expect("the first consumer takes the subscription");

    // A connection of its own: the bound belongs to the client, and the holder above keeps the
    // unbounded one this suite uses everywhere else.
    let bounded = PulsarBroker::new(&url)
        .operation_retries(
            OperationRetries::attempts(nonzero!(2u32)).delay(Duration::from_millis(200)),
        )
        .connect()
        .await
        .expect("broker connects");

    let refused = tokio::time::timeout(RECV_TIMEOUT, bounded.subscribe_descriptor(exclusive()))
        .await
        .expect("a bounded client answers instead of waiting")
        .expect_err("the subscription is taken, so the attach must report it");
    assert!(
        matches!(refused, PulsarError::Subscribe { .. }),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains(&topic),
        "the refusal must name the topic, got: {refused}",
    );

    bounded.shutdown().await.expect("shutdown succeeds");
    connected.shutdown().await.expect("shutdown succeeds");
}

/// Competing consumers: a message goes to one of them, and two subscriptions over one topic each
/// get their own copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_subscription_hands_each_message_to_one_consumer() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("competing");
    let subscription = unique("workers");
    let shared = || PulsarSubscription::new(&topic, &subscription);
    let mut first = connected
        .subscribe_descriptor(shared())
        .await
        .expect("the first consumer attaches");
    let mut second = connected
        .subscribe_descriptor(shared())
        .await
        .expect("a second consumer joins a shared subscription");
    let mut watcher = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("audit")))
        .await
        .expect("a second subscription attaches");

    let sent: Vec<String> = (1..=4).map(|id| format!("o{id}")).collect();
    for payload in &sent {
        publish(&connected, &topic, payload).await;
    }

    let mut shared_out = drain(&mut first).await;
    shared_out.extend(drain(&mut second).await);
    shared_out.sort();
    assert_eq!(
        shared_out, sent,
        "every message must reach exactly one consumer of the subscription",
    );
    assert_eq!(
        drain(&mut watcher).await,
        sent,
        "sharing is within a subscription: a second one over the topic reads the whole stream",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Per-key ordering rests on a key staying with one consumer, which is the claim the server's
/// hash ranges make and the in-process mode can only approximate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_shared_subscription_keeps_a_key_on_one_consumer() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("keyed-sharing");
    let subscription = unique("keyed");
    let keyed = || {
        PulsarSubscription::new(&topic, &subscription)
            .subscription_type(SubscriptionType::KeyShared)
    };
    let mut first = connected
        .subscribe_descriptor(keyed())
        .await
        .expect("the first consumer attaches");
    let mut second = connected
        .subscribe_descriptor(keyed())
        .await
        .expect("a second consumer joins");

    for (key, payload) in [
        ("tenant-a", "m1"),
        ("tenant-b", "m2"),
        ("tenant-a", "m3"),
        ("tenant-b", "m4"),
    ] {
        publish_keyed(&connected, &topic, key, payload).await;
    }

    let one = drain(&mut first).await;
    let two = drain(&mut second).await;
    assert_eq!(one.len() + two.len(), 4, "every message reached a consumer");

    // Which consumer takes a key is the server's hash ranges, so the claim is the one a service
    // rests on either way: a key does not move between consumers.
    let together = |left: &str, right: &str| {
        one.iter().any(|seen| seen == left) == one.iter().any(|seen| seen == right)
    };
    assert!(
        together("m1", "m3"),
        "'tenant-a' was split between consumers: {one:?} and {two:?}",
    );
    assert!(
        together("m2", "m4"),
        "'tenant-b' was split between consumers: {one:?} and {two:?}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// One active consumer with a hot standby: the standby reads nothing while the active consumer
/// holds the subscription, and takes over when it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failover_subscription_promotes_the_standby() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("failover");
    let subscription = unique("sub");
    let failover = || {
        PulsarSubscription::new(&topic, &subscription).subscription_type(SubscriptionType::Failover)
    };
    let mut first = connected
        .subscribe_descriptor(failover())
        .await
        .expect("the first consumer attaches");
    let mut second = connected
        .subscribe_descriptor(failover())
        .await
        .expect("a standby attaches");

    publish(&connected, &topic, "o1").await;
    publish(&connected, &topic, "o2").await;

    // Which of the two the server made active is its own choice, so the claim is that one of
    // them holds the whole stream and the other holds none of it.
    let from_first = drain(&mut first).await;
    let from_second = drain(&mut second).await;
    let (active, mut standby) = if from_first.is_empty() {
        (second, first)
    } else {
        (first, second)
    };
    assert_eq!(
        if from_first.is_empty() {
            &from_second
        } else {
            &from_first
        },
        &["o1".to_owned(), "o2".to_owned()],
        "one consumer must hold the whole stream while it is the active one",
    );
    assert!(
        from_first.is_empty() || from_second.is_empty(),
        "a standby reads nothing while the active consumer holds the subscription: \
         {from_first:?} and {from_second:?}",
    );

    drop(active);
    // The server promotes the standby once the active consumer's close reaches it, so the take
    // over is polled rather than assumed to have happened by the next publish.
    let mut promoted = Vec::new();
    for attempt in 0..10 {
        publish(&connected, &topic, &format!("after-{attempt}")).await;
        promoted = drain(&mut standby).await;
        if !promoted.is_empty() {
            break;
        }
    }
    assert!(
        !promoted.is_empty(),
        "the standby was never promoted after the active consumer left",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A cap the broker would never count is refused where it is declared, before a consumer is
/// opened for it: the server holds no subscription afterwards, so the refusal is a startup one
/// and not a consumer left behind under a policy that does nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cap_on_a_single_active_subscription_never_opens_a_consumer() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let topic = unique("uncounted");
    let subscription = unique("sub");
    publish(&connected, &topic, "o1").await;

    let declaration = RetryDeclaration::new()
        .with_max_attempts(nonzero!(2u32))
        .with_dead_letter(unique("uncounted-dlq"));
    let capped = <PulsarSubscription as SubscriptionSource<ConnectedPulsarBroker>>::declare_retry(
        PulsarSubscription::new(&topic, &subscription)
            .subscription_type(SubscriptionType::Failover),
        &declaration,
    );

    let refused = connected
        .subscribe_descriptor(capped)
        .await
        .expect_err("a cap the broker never counts must be refused");
    assert!(matches!(refused, PulsarError::Invalid(_)), "{refused:?}");
    assert!(
        format!("{refused}").contains("Failover"),
        "the refusal must name the type that carries no count: {refused}",
    );
    assert!(
        !admin::subscription_names(&topic)
            .await
            .contains(&subscription),
        "a consumer was opened for a policy the broker cannot apply",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
