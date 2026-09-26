//! The routes file a service ships, run in process by the test harness.
//!
//! `TestApp` runs the production app with `PulsarBroker` connected in process, so a test mounts
//! the very `#[subscriber(PulsarSubscription::..)]` and the very `out_reply(Publish)` a
//! production routes file writes: no bare-topic rewrite of the declaration, no test-only
//! descriptor or policy, nothing changed at the include site. Each addressing form the
//! descriptor offers (one topic, a list, a pattern) gets a run here, because each reads its
//! topics differently.
//!
//! What a Pulsar server does with the rest of a descriptor is covered by the live suite in
//! `integration_pulsar.rs`.
#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "pulsar://localhost:6650";

/// How often the client lists the namespace again for a pattern subscription.
const PATTERN_REFRESH: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq, Deserialize, Outgoing, Serialize)]
struct Order {
    id: u64,
}

fn order(id: u64) -> Order {
    Order { id }
}

// --8<-- [start:descriptor]
/// The declaration a service ships, settings and all: the subscription type decides which
/// consumer takes a message in process as it does on a server, and the ack timeout bounds a
/// delayed retry in both.
#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
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

/// One subscription over every topic of the namespace whose name matches.
#[subscriber(PulsarSubscription::pattern("audit-.*", "audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[derive(Debug, PartialEq, Eq, Deserialize, Outgoing, Serialize)]
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
/// publish policy, which is that same `PulsarPublish`.
#[subscriber(
    PulsarSubscription::new("payments-default", "workers"),
    publish("receipts-default")
)]
async fn confirm_by_default(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_production_descriptor_mounts_in_process() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(PulsarBroker::new(URL), |b| {
            b.include(handle);
        });
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&order(1))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    // The subscription is reported under its single topic, as it is against a real broker.
    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&order(1))
        .settled(HandlerOutcome::ack());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_topic_descriptor_covers_every_topic_it_lists() {
    let app = RustStream::new(AppInfo::new("regional", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(handle_regional);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    for (topic, id) in [("orders-eu", 1), ("orders-us", 2)] {
        tb.broker::<PulsarBroker>()
            .message(&order(id))
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }
    // A topic the list does not name belongs to another subscription.
    tb.broker::<PulsarBroker>()
        .message(&order(3))
        .to("orders-apac")
        .publish()
        .await
        .expect("publish");

    // A list has no single topic to name the subscription by, so it is reported under the
    // subscription name - the same name a real broker holds it under.
    assert_eq!(
        tb.broker::<PulsarBroker>()
            .subscriber("regional")
            .assert_called(2)
            .settled(HandlerOutcome::ack())
            .received::<Order>(),
        vec![order(1), order(2)],
    );
}

/// A pattern resolves against the namespace listing: a topic created after the subscription
/// opened is read from the client's next listing, and what reached it before then is not
/// delivered, because the subscription the client opens on it starts at its tip.
#[tokio::test(start_paused = true)]
async fn a_pattern_descriptor_reads_a_new_topic_from_the_next_listing() {
    let app =
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(PulsarBroker::new(URL), |b| {
            b.include(audit);
        });
    let tb = TestApp::start(app).await.expect("start harness");

    // Neither topic existed when the subscription opened.
    for (topic, id) in [("audit-eu", 1), ("orders", 2)] {
        tb.broker::<PulsarBroker>()
            .message(&order(id))
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }
    tb.broker::<PulsarBroker>()
        .subscriber("audit")
        .assert_not_called();

    tb.advance(PATTERN_REFRESH)
        .await
        .expect("the client lists the namespace again");
    for (topic, id) in [("audit-eu", 3), ("orders", 4)] {
        tb.broker::<PulsarBroker>()
            .message(&order(id))
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    assert_eq!(
        tb.broker::<PulsarBroker>()
            .subscriber("audit")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .received::<Order>(),
        vec![order(3)],
        "the pattern reads its listed topics and nothing else",
    );
}

// --8<-- [start:reply_mount]
/// The publishing half, in the production spelling: the mount site names the policy the broker
/// prelude exports, and the reply is asserted on the broker's publish log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_production_routes_file_publishes_its_reply() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(confirm).out_reply(Publish);
            // No reply position: the reply takes the broker's default policy, `Publish` too.
            b.include(confirm_by_default);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&order(1))
        .to("payments")
        .publish()
        .await
        .expect("publish");
    tb.broker::<PulsarBroker>()
        .message(&order(2))
        .to("payments-default")
        .publish()
        .await
        .expect("publish");

    tb.broker::<PulsarBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 1 });
    tb.broker::<PulsarBroker>()
        .published::<Receipt>("receipts-default")
        .assert_called_once()
        .with(&Receipt { id: 2 });
}
// --8<-- [end:reply_mount]
