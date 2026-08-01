//! Conformance: the routing suite against the in-process transport, and the lifecycle check
//! against a standalone broker (gated behind `PULSAR_TEST_URL`).
//!
//! Start one with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::conformance::{capabilities, harness};
use ruststream_pulsar::testing::PulsarTestBroker;
use ruststream_pulsar::{PulsarBroker, PulsarSubscription};

fn test_url() -> Option<String> {
    match std::env::var("PULSAR_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!("PULSAR_TEST_URL is not set; skipping the live conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_test_broker_passes_conformance_suite() {
    harness::run_suite(PulsarTestBroker::new).await;
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
