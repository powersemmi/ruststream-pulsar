//! What a registration declares about its retries, and what Pulsar does with it.
//!
//! The declaration is one pair of steps at the mount site: `max_attempts(..)` caps how many times
//! a message is handed to the handler, `dead_letter(..)` names where it goes afterwards. On
//! Pulsar both become the consumer's own dead-letter policy, so the client counts the
//! redeliveries and produces the spent message to that topic. Nothing is published by the
//! service, which is why `out_retry(..)` does not compile over a Pulsar subscription at all.
//!
//! A registration mounted by a bare topic name declares the same pair. It has no descriptor to
//! carry the declaration, so the broker takes it in and builds the consumer for that name from
//! it.
//!
//! Half a declaration is not a policy the client can apply, so a registration that writes one
//! step and not the other refuses to start rather than running with a cap nobody enforces.
#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "pulsar://localhost:6650";

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Never settles successfully, so the only thing that ends the run is the declared cap.
#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

#[subscriber(PulsarSubscription::topics(["orders-eu", "orders-us"], "regional"))]
async fn reconcile_regional(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

#[subscriber(PulsarSubscription::pattern("audit-.*", "audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

/// The cap counts deliveries to the handler, and the spent message turns up on the declared
/// topic rather than circling the subscription for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declared_cap_ends_at_the_dead_letter_topic() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(reconcile)
                .max_attempts(nonzero!(3))
                .dead_letter("orders-dlq");
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called(3);
    tb.broker::<PulsarBroker>()
        .published::<Order>("orders-dlq")
        .assert_called_once()
        .with(&Order { id: 1 });
}

/// The policy belongs to the consumer, so it covers every topic a multi-topic subscription
/// reads, and the spent message leaves through the one declared topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_topic_subscription_declares_one_destination() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(reconcile_regional)
                .max_attempts(nonzero!(2))
                .dead_letter("regional-dlq");
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&Order { id: 2 })
        .to("orders-us")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<PulsarBroker>()
        .subscriber("regional")
        .assert_called(2);
    tb.broker::<PulsarBroker>()
        .published::<Order>("regional-dlq")
        .assert_called_once();
}

/// A pattern subscription is a consumer too, so it carries the same policy: the copy path is a
/// property of the client rather than of how the subscription found its topics. The destination
/// is named outside the pattern on purpose - a dead-letter topic the pattern itself selects
/// feeds the spent message straight back to the handler, here and on a server alike.
///
/// The topic appears after the subscription opened, so the client reads it from its next listing
/// of the namespace; the paused clock is what lets the test reach that listing.
#[tokio::test(start_paused = true)]
async fn a_pattern_subscription_carries_the_same_policy() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(audit)
                .max_attempts(nonzero!(2))
                .dead_letter("dlq-audit");
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    // The first publish creates the topic; the pattern reads it once the client lists it again.
    tb.broker::<PulsarBroker>()
        .message(&Order { id: 0 })
        .to("audit-eu")
        .publish()
        .await
        .expect("publish");
    tb.advance(Duration::from_secs(30))
        .await
        .expect("the client lists the namespace again");
    tb.broker::<PulsarBroker>()
        .message(&Order { id: 3 })
        .to("audit-eu")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<PulsarBroker>()
        .subscriber("audit")
        .assert_called(2);
    tb.broker::<PulsarBroker>()
        .published::<Order>("dlq-audit")
        .assert_called_once();
}

/// Never settles, and mounted by a bare topic name: the declaration has no descriptor to travel
/// in, so what applies it is whatever the broker made of it.
#[subscriber("orders")]
async fn reconcile_by_name(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

/// A bare name is the shorter spelling of the same subscription, so the cap and the destination
/// declared on it reach the consumer the way a descriptor's do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_carries_its_declaration_to_the_consumer() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL).default_subscription("workers"),
        |b| {
            b.include(reconcile_by_name)
                .max_attempts(nonzero!(5))
                .dead_letter("orders-dead");
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&Order { id: 11 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called(5);
    tb.broker::<PulsarBroker>()
        .published::<Order>("orders-dead")
        .assert_called_once()
        .with(&Order { id: 11 });
}

/// The refusal reaches a bare name too, and it names the topic, because that is all a
/// registration mounted this way says about itself.
#[tokio::test]
async fn a_bare_name_refuses_half_a_declaration() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL).default_subscription("workers"),
        |b| {
            b.include(reconcile_by_name).max_attempts(nonzero!(5));
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a half declaration must not start");
    let message = failed.to_string();
    assert!(message.contains("topic 'orders'"), "{message}");
    assert!(message.contains("dead_letter"), "{message}");
}

/// A cap with nowhere to send the spent message is not a Pulsar policy, and the refusal says
/// which step is missing.
#[tokio::test]
async fn a_cap_without_a_destination_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(reconcile).max_attempts(nonzero!(3));
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a half declaration must not start");
    let message = failed.to_string();
    assert!(message.contains("workers"), "{message}");
    assert!(message.contains("dead_letter"), "{message}");
}

/// The mirror image: a destination with no cap never fires, so it is refused as well.
#[tokio::test]
async fn a_destination_without_a_cap_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(reconcile).dead_letter("orders-dlq");
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a half declaration must not start");
    let message = failed.to_string();
    assert!(message.contains("workers"), "{message}");
    assert!(message.contains("max_attempts"), "{message}");
}

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Receipt {
    id: u64,
}

#[subscriber(PulsarSubscription::new("payments", "workers"), publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The declaration composes with the rest of the mount chain: a replying registration names its
/// reply policy and its cap in one statement, which is the spelling the README and the guide
/// show.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declaration_composes_with_a_reply_position() {
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(confirm)
                .out_reply(Publish)
                .max_attempts(nonzero!(5))
                .dead_letter("payments-dlq");
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&Order { id: 7 })
        .to("payments")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<PulsarBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
}
