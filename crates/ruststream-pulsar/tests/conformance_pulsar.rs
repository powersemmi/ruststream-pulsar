//! Conformance: every suite this crate's capabilities justify, run twice.
//!
//! The in-process legs hold the stand-in to the same contract as the server - routing,
//! lifecycle, seeking, batches - because a service is unit-tested against it and a stand-in that
//! quietly disagrees with the contract is worse than no stand-in. The server legs, gated behind
//! `PULSAR_TEST_URL`, are what says the stand-in is not merely agreeing with itself.
//!
//! Request-reply and transactions have no legs here: the crate implements neither capability, so
//! neither suite applies to either broker.
//!
//! Start a broker with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_pulsar::testing::PulsarTestBroker;
use ruststream_pulsar::{PulsarBroker, PulsarSubscription};

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
            eprintln!("PULSAR_TEST_URL is not set; skipping the live conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_test_broker_passes_conformance_suite() {
    harness::run_suite(PulsarTestBroker::new).await;
}

/// The lifecycle ladder against the stand-in, through the crate's own descriptor: synchronous
/// construction, the consuming `connect`, a subscription, a delivery it acks, the consuming
/// `shutdown`, and the publisher that aliased the connection erroring afterwards rather than
/// swallowing the message. A service is unit-tested against this broker, so the contract it
/// claims to follow has to hold here and not only against a server; the live leg below runs the
/// same suite, which is what says the two agree.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_test_broker_passes_lifecycle() {
    harness::lifecycle(
        PulsarTestBroker::new,
        |name| PulsarSubscription::new(name, "conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

/// The stand-in retains a log, so the framework's own seeking suite is what says its
/// repositioning matches the contract - the same suite the live broker runs below. Without it a
/// service unit-tested on the stand-in could be relying on a seek that only looked right.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_test_broker_passes_seeking_suite() {
    capabilities::seeking(
        PulsarTestBroker::new,
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
async fn pulsar_test_broker_passes_batches_suite() {
    capabilities::batches(
        PulsarTestBroker::new,
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
        || PulsarBroker::new(url.clone()),
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
        || PulsarBroker::new(url.clone()),
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
        || PulsarBroker::new(url.clone()),
        |name| PulsarSubscription::new(name, sub.clone()),
        |connected| connected.publisher(),
    )
    .await;
}
