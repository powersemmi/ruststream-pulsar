//! Where a deferred `retry_after` copy is published on Pulsar.
//!
//! Pulsar has no per-message delivery delay on the consumer side, so the framework honours
//! `retry_after` by publishing the message again once the delay is over, and the subscription
//! says where. A topic is its own address: a publish to it reaches whoever reads it, so a
//! subscription over one topic answers with that topic and a bare `#[subscriber("orders")]`
//! answers with the name it was given.
//!
//! A topic list and a pattern stay silent. Either could name a topic the subscription reads, but
//! the copy would then arrive on a different topic from the one the original came off, and a
//! handler that branches on the delivery's topic would take the wrong branch. The application
//! refuses to start instead, naming the subscription.
#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::{Outcome, TestApp};
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;
use serde::{Deserialize, Serialize};

const RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Defers the first delivery of every message and acks the copy that comes back. The framework
/// counts the attempts in a header, so the copy is told apart from the original.
fn defer_once(headers: &HeaderMap) -> HandlerOutcome {
    let attempt = headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn reconcile(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order;
    defer_once(ctx.headers())
}

#[subscriber("payments")]
async fn settle(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order;
    defer_once(ctx.headers())
}

#[subscriber(PulsarSubscription::topics(["orders-eu", "orders-us"], "regional"))]
async fn reconcile_regional(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// One topic: the deferred copy goes back to it, so the handler sees the message again after the
/// delay and not before.
#[tokio::test(start_paused = true)]
async fn a_descriptor_over_one_topic_addresses_its_own_retries() {
    let broker = PulsarTestBroker::new();
    let retries = broker.publisher();
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(broker, |b| {
        b.retry_via(retries);
        b.include(reconcile);
    });
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<PulsarTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // The delay is real: nothing comes back before it elapses.
    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<PulsarTestBroker>()
        .subscriber("orders")
        .assert_called_once();

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    assert_eq!(
        tb.broker::<PulsarTestBroker>()
            .subscriber("orders")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

/// A bare topic name is an address too, so `#[subscriber("payments")]` composes with a retry
/// publisher without a descriptor.
#[tokio::test(start_paused = true)]
async fn a_topic_name_subscriber_addresses_its_own_retries() {
    let broker = PulsarTestBroker::new();
    let retries = broker.publisher();
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(broker, |b| {
        b.retry_via(retries);
        b.include(settle);
    });
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 2 })
        .to("payments")
        .publish()
        .await
        .expect("publish");

    tb.advance(RETRY_DELAY).await.expect("settle");
    assert_eq!(
        tb.broker::<PulsarTestBroker>()
            .subscriber("payments")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

/// A subscription over several topics has no single address, so a scope that defers through a
/// publisher refuses to start rather than relocating the retried message.
#[tokio::test]
async fn a_multi_topic_descriptor_refuses_to_defer() {
    let broker = PulsarTestBroker::new();
    let retries = broker.publisher();
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(broker, |b| {
        b.retry_via(retries);
        b.include(reconcile_regional);
    });

    let failed = TestApp::start(app)
        .await
        .expect_err("a subscription that cannot address its retries must not start");
    let message = failed.to_string();
    assert!(message.contains("regional"), "{message}");
    assert!(message.contains("PulsarSubscription"), "{message}");
    assert!(message.contains("retry_via"), "{message}");
}

/// Without a retry publisher the same subscription starts: `retry_after` degrades to an
/// immediate requeue, which is what Pulsar's own nack does anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_topic_descriptor_starts_when_the_scope_defers_nothing() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(reconcile_regional);
        },
    );

    let tb = TestApp::start(app).await.expect("start harness");
    tb.shutdown().await.expect("graceful shutdown");
}
