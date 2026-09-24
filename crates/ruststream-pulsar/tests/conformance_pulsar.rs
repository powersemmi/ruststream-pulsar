//! Conformance: every suite this crate's capabilities justify, run twice over the same
//! production broker.
//!
//! The in-process legs connect `PulsarBroker` the way the test harness does, through
//! `harness::InProcessBroker` for the suites that call `connect`, and hold that transport to the
//! contract the server is held to - routing, lifecycle, seeking, batches - because a service's
//! tests run on it. The server legs, gated behind `PULSAR_TEST_URL`, are what says the
//! in-process mode is not merely agreeing with itself.
//!
//! Request-reply and transactions have no legs here: the crate implements neither capability, so
//! neither suite applies to either transport.
//!
//! Start a broker with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features`.

#![cfg(feature = "testing")]

mod live;

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream_pulsar::{PulsarBroker, PulsarSubscription};

use crate::live::{test_url, unique};

/// The address the service's broker is built with; the in-process legs dial nothing.
const URL: &str = "pulsar://localhost:6650";

/// The production broker, connected in process by the suites that call `connect`.
fn in_process() -> InProcessBroker<PulsarBroker> {
    InProcessBroker::new(PulsarBroker::new(URL).default_subscription("conformance"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(|| PulsarBroker::new(URL).default_subscription("conformance")).await;
}

/// The lifecycle ladder in process, through the crate's own descriptor: synchronous
/// construction, the consuming transition, a subscription, a delivery it acks, the consuming
/// `shutdown`, and the publisher that aliased the connection erroring afterwards rather than
/// swallowing the message. The live leg below runs the same suite, which is what says the two
/// agree.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| PulsarSubscription::new(name, "conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same ladder over the bare-name form, which resolves through `Subscribe` and the broker's
/// default subscription rather than through the descriptor.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_lifecycle_by_name() {
    harness::lifecycle(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The in-process transport retains a log, so the framework's own seeking suite is what says its
/// repositioning matches the contract - the same suite the live broker runs below.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_seeking_suite() {
    capabilities::seeking(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// Pulsar's client hands over one delivery at a time, so this crate's batches are assembled on
/// the client. The suite is what says the assembly honours the contract: it opens the
/// subscription at a size smaller than the run and fails a batch that comes back longer.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_batches_suite() {
    capabilities::batches(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_lifecycle() {
    let Some(url) = test_url() else { return };
    // A subscription persists broker-side and keeps its backlog; a per-run name keeps reruns
    // from inheriting the previous run's messages.
    let sub = format!("lifecycle-{}", std::process::id());
    harness::lifecycle(
        || PulsarBroker::new(url.clone()).default_subscription(unique("conformance")),
        |name| PulsarSubscription::new(name, sub.clone()),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_seeking_suite() {
    let Some(url) = test_url() else { return };
    let sub = format!("seeking-{}", std::process::id());
    capabilities::seeking(
        || PulsarBroker::new(url.clone()).default_subscription(unique("conformance")),
        |name| PulsarSubscription::new(name, sub.clone()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same batch contract against a live consumer: the buffer sits over real deliveries here,
/// with the client's own flow control underneath it.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_batches_suite() {
    let Some(url) = test_url() else { return };
    let sub = format!("batches-{}", std::process::id());
    capabilities::batches(
        || PulsarBroker::new(url.clone()).default_subscription(unique("conformance")),
        |name| PulsarSubscription::new(name, sub.clone()),
        |connected| connected.publisher(),
    )
    .await;
}
