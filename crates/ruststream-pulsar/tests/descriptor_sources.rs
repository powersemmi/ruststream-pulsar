//! The routes file a service ships, mounted on the in-process stand-in.
//!
//! `PulsarSubscription` is a subscription source for `PulsarTestBroker` as well as for the real
//! broker, and `PulsarPublish` pairs against both, so a `TestApp` run mounts the very
//! `#[subscriber(PulsarSubscription::..)]` and the very `.out(Reply, Publish)` a production
//! routes file writes: no bare-topic rewrite of the declaration, no test-only descriptor or
//! policy, nothing changed at the include site. Each addressing form the descriptor offers
//! (one topic, a list, a pattern) gets a run here, because each is a different route through
//! the stand-in's router.
//!
//! What a Pulsar server does with the rest of a descriptor is not simulated and is covered by
//! the live suite in `integration_pulsar.rs`.
#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Deserialize, Outgoing, Serialize)]
struct Order {
    id: u64,
}

fn order(id: u64) -> Order {
    Order { id }
}

// --8<-- [start:descriptor]
/// The declaration a service ships, settings and all: the subscription type, the dead-letter
/// policy and the ack timeout describe work the Pulsar server does, and the stand-in ignores
/// them, so the same handler mounts on either broker.
#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
        .dead_letter(DeadLetter::new("orders-dlq").max_deliveries(5))
        .ack_timeout(Duration::from_secs(30))
)]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}
// --8<-- [end:descriptor]

/// One subscription over a fixed list of topics.
#[subscriber(PulsarSubscription::topics(["orders-eu", "orders-us"], "regional"))]
async fn handle_regional(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// One subscription over every topic whose name matches, whenever that topic appears.
#[subscriber(PulsarSubscription::pattern("audit-.*", "audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
struct Receipt {
    id: u64,
}

// --8<-- [start:reply]
/// A replying handler on the production descriptor. Its reply leaves through the policy named at
/// the mount site below, in the spelling a routes file uses against a real broker.
#[subscriber(PulsarSubscription::new("payments", "workers"), publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}
// --8<-- [end:reply]

/// The same shape with nothing named at the mount site: the reply rides the broker's default
/// publish policy, which on the stand-in is that same `PulsarPublish`.
#[subscriber(
    PulsarSubscription::new("payments-default", "workers"),
    publish("receipts-default")
)]
async fn confirm_by_default(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_production_descriptor_mounts_on_the_stand_in() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(handle);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&order(1))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    // The subscription is reported under its single topic, as it is against a real broker.
    tb.broker::<PulsarTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&order(1))
        .settled(HandlerOutcome::ack());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_topic_descriptor_covers_every_topic_it_lists() {
    let app = RustStream::new(AppInfo::new("regional", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(handle_regional);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    for (topic, id) in [("orders-eu", 1), ("orders-us", 2)] {
        tb.broker::<PulsarTestBroker>()
            .message(&order(id))
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }
    // A topic the list does not name belongs to another subscription.
    tb.broker::<PulsarTestBroker>()
        .message(&order(3))
        .to("orders-apac")
        .publish()
        .await
        .expect("publish");

    // A list has no single topic to name the subscription by, so it is reported under the
    // subscription name - the same name a real broker holds it under.
    assert_eq!(
        tb.broker::<PulsarTestBroker>()
            .subscriber("regional")
            .assert_called(2)
            .settled(HandlerOutcome::ack())
            .received::<Order>(),
        vec![order(1), order(2)],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_descriptor_follows_topics_that_appear_later() {
    let app =
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(PulsarTestBroker::new(), |b| {
            b.include(audit);
        });
    let tb = TestApp::start(app).await.expect("start harness");

    // Neither topic existed when the subscription opened; a pattern subscription picks up both.
    for (topic, id) in [("audit-eu", 1), ("audit-us", 2)] {
        tb.broker::<PulsarTestBroker>()
            .message(&order(id))
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }
    tb.broker::<PulsarTestBroker>()
        .message(&order(3))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    assert_eq!(
        tb.broker::<PulsarTestBroker>()
            .subscriber("audit")
            .assert_called(2)
            .settled(HandlerOutcome::ack())
            .received::<Order>(),
        vec![order(1), order(2)],
        "the pattern selects its topics and nothing else",
    );
}

// --8<-- [start:reply_mount]
/// The publishing half, in the production spelling: the mount site names the policy the broker
/// prelude exports, and the reply is asserted on the stand-in's publish log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_production_routes_file_publishes_its_reply_on_the_stand_in() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish);
            // No `.out(..)`: the reply takes the broker's default policy, which is `Publish` too.
            b.include(confirm_by_default);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&order(1))
        .to("payments")
        .publish()
        .await
        .expect("publish");
    tb.broker::<PulsarTestBroker>()
        .message(&order(2))
        .to("payments-default")
        .publish()
        .await
        .expect("publish");

    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 1 });
    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts-default")
        .assert_called_once()
        .with(&Receipt { id: 2 });
}
// --8<-- [end:reply_mount]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_topic_subscription_opens_on_the_backlog_of_every_topic() {
    let broker = PulsarTestBroker::new();
    let ingress = broker.publisher();
    for (topic, id) in [("orders-eu", 1), ("orders-us", 2), ("orders-eu", 3)] {
        ingress
            .message(&order(id))
            .to(topic)
            .publish()
            .await
            .expect("the stand-in publishes before connect");
    }

    let app = RustStream::new(AppInfo::new("regional", "0.1.0")).with_broker(broker, |b| {
        b.include(handle_regional.start_at(PulsarPosition::earliest()));
    });
    let tb = TestApp::start(app).await.expect("start harness");
    tb.settle().await.expect("the opening replay settles");

    // The clause opened the subscription at the beginning of each topic's log, and the two
    // replays reached the handler as one stream.
    let mut ids: Vec<u64> = tb
        .broker::<PulsarTestBroker>()
        .subscriber("regional")
        .assert_called(3)
        .settled(HandlerOutcome::ack())
        .received::<Order>()
        .into_iter()
        .map(|order| order.id)
        .collect();
    // Entries written within the same millisecond carry the same publish time, so the merge
    // across the two logs is free to interleave them; which topic came first is not the claim.
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
}
