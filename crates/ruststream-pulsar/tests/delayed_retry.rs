//! A `retry_after(delay)` outcome on Pulsar: the wait is this process's, the redelivery is the
//! broker's.
//!
//! Pulsar's negative acknowledgement carries no delay, so the crate holds the delivery
//! unacknowledged until the delay is over and only then negatively acknowledges it. Nothing is
//! republished, so the redelivery is an ordinary one and the consumer's dead-letter policy counts
//! it like any other: a handler that keeps deferring still ends at the declared topic.
//!
//! The bound is the subscription's `ack_timeout`. A consumer redelivers an unacknowledged message
//! once that timeout elapses, so a delay that is not shorter than it would bring the message back
//! early, and the call refuses it instead.
//!
//! The last pair runs one test body twice: in process under `just test`, and against the stand
//! under `just test-brokers`.
#![cfg(feature = "testing")]

mod live;

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

use crate::live::{test_url, unique};

const DELAY: Duration = Duration::from_secs(2);

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "pulsar://localhost:6650";

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Defers every delivery, so what ends the run is the declared cap rather than the handler.
#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

/// A subscription whose consumer redelivers on its own sooner than the handler asks to wait.
#[subscriber(
    PulsarSubscription::new("slow", "workers").ack_timeout(Duration::from_secs(1))
)]
async fn reconcile_slowly(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

/// The delay is real: the message comes back when it is over, and not a moment sooner.
#[tokio::test(start_paused = true)]
async fn a_delayed_retry_comes_back_after_the_delay_and_not_before() {
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
    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called_once();

    tb.advance(DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called_once();

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called(2);
}

/// The wait changes nothing about who applies the cap: the redelivery is the broker's, so the
/// consumer's dead-letter policy still counts it and still lands the spent delivery.
#[tokio::test(start_paused = true)]
async fn a_deferred_delivery_still_reaches_the_dead_letter_topic() {
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
        .message(&Order { id: 2 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    // Three deliveries, so three waits: the third redelivery is the one the cap stops.
    for _ in 0..3 {
        tb.advance(DELAY).await.expect("settle");
    }

    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called(3);
    tb.broker::<PulsarBroker>()
        .published::<Order>("orders-dlq")
        .assert_called_once()
        .with(&Order { id: 2 });
}

/// A delay the consumer's own timer would cut short is refused, so the message is never held for
/// longer than the broker would wait: nothing comes back, and the handler is not called again.
#[tokio::test(start_paused = true)]
async fn a_delay_past_the_ack_timeout_is_refused() {
    let app = RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(
        PulsarBroker::new(URL),
        |b| {
            b.include(reconcile_slowly);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarBroker>()
        .message(&Order { id: 3 })
        .to("slow")
        .publish()
        .await
        .expect("publish");
    tb.advance(DELAY * 3).await.expect("settle");

    tb.broker::<PulsarBroker>()
        .subscriber("slow")
        .assert_called_once();
}

// --- One test body, in process and against a live broker. ---

/// Long enough to tell a redelivery the delay released from one that came back at once, short
/// enough for a live run.
const WAITED: Duration = Duration::from_secs(1);

/// The topic the two-mode test runs on. Each live run joins a subscription of its own, which
/// starts at the tip, so an earlier run's backlog is not this one's.
const DEFERRED: &str = "orders.deferred";

/// Asks for every delivery back after [`WAITED`].
#[subscriber("orders.deferred")]
async fn defer(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(WAITED)
}

/// The app `main` runs, on the broker it is handed: the production builder in either mode.
fn deferring_app(broker: PulsarBroker) -> RustStream {
    RustStream::new(AppInfo::new("retries", "0.1.0")).with_broker(broker, |b| {
        b.include(defer);
    })
}

/// The body both modes run: the first delivery asks to come back, the delay passes, and the
/// broker hands the same message over again without anything being published a second time.
async fn a_deferred_delivery_comes_back(tb: TestApp<()>) {
    tb.broker::<PulsarBroker>()
        .message(&Order { id: 42 })
        .to(DEFERRED)
        .publish()
        .await
        .expect("publish drives the first delivery to its settlement");
    tb.broker::<PulsarBroker>()
        .subscriber(DEFERRED)
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(WAITED));

    tb.advance(WAITED)
        .await
        .expect("the delay passes and the broker redelivers");

    tb.broker::<PulsarBroker>()
        .subscriber(DEFERRED)
        .assert_called(2)
        .with(&Order { id: 42 });
    tb.broker::<PulsarBroker>()
        .published::<Order>(DEFERRED)
        .assert_called_once()
        .with(&Order { id: 42 });

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_deferred_delivery_comes_back_in_process() {
    let broker = PulsarBroker::new(URL).default_subscription("workers");
    let tb = TestApp::start(deferring_app(broker)).await.expect("start");
    a_deferred_delivery_comes_back(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_delivery_comes_back_live() {
    let Some(url) = test_url() else { return };
    let broker = PulsarBroker::new(url).default_subscription(unique("deferred"));
    let tb = TestApp::start_live(deferring_app(broker))
        .await
        .expect("start against the stand");
    a_deferred_delivery_comes_back(tb).await;
}
