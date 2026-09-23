//! What every live suite shares: the stand it runs against, and the broker's own view of it.
//!
//! Start the stand with `just brokers-up`, or run the whole set with `just test-brokers`.

// One module, several suites: each includes it and calls the part its own assertions need.
#![allow(dead_code)]

use std::time::Duration;

use ruststream::Broker;
use ruststream_pulsar::{ConnectedPulsarBroker, PulsarBroker};

pub(crate) mod admin;

/// How long a delivery may take before the suite calls it lost.
pub(crate) const RECV_TIMEOUT: Duration = Duration::from_secs(30);

/// The broker to run the live checks against, or `None` to skip them.
///
/// Skipping quietly is what keeps these usable on a laptop with no stand running. It is also
/// what would let a renamed variable or a dropped `env:` block turn the whole live job green
/// without running anything, so CI sets `RUSTSTREAM_REQUIRE_LIVE` and the skip becomes a
/// failure there.
///
/// # Panics
///
/// Panics when `RUSTSTREAM_REQUIRE_LIVE` is set and no URL is.
pub(crate) fn test_url() -> Option<String> {
    match std::env::var("PULSAR_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live suites must run, but \
                 PULSAR_TEST_URL is missing or empty",
            );
            eprintln!("PULSAR_TEST_URL is not set; skipping the live suite");
            None
        }
    }
}

/// Connects to the stand.
///
/// # Panics
///
/// Panics when the stand does not answer.
pub(crate) async fn connect(url: &str) -> ConnectedPulsarBroker {
    PulsarBroker::new(url)
        .default_subscription(unique("by-name"))
        .connect()
        .await
        .expect("broker connects")
}

/// Per-run unique names, so runs do not observe each other's leftovers: a subscription and its
/// backlog are broker-side state that outlives the test that made them.
#[must_use]
pub(crate) fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}
