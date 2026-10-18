//! One test body, two modes: in process under `TestApp::start`, and against a running Pulsar under
//! `TestApp::start_live`. The live test skips without `PULSAR_TEST_URL`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example pulsar_testing_live --features testing
//! just brokers-up
//! PULSAR_TEST_URL=pulsar://127.0.0.1:6650 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example pulsar_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live-payments")]
struct Payment {
    amount: u64,
}

#[subscriber("testing-live-payments")]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The service on the server at `url`, reading in the subscription `subscription`.
fn app(url: &str, subscription: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PulsarBroker::new(url).default_subscription(subscription),
        |b| {
            b.include(accept);
        },
    )
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("pulsar://localhost:6650", "payments-svc")
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;
    use std::process;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Payment, app};

    /// The stand's URL from `PULSAR_TEST_URL`, or `None` to skip the live test.
    /// `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure.
    fn stand() -> Option<String> {
        match env::var("PULSAR_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                    "RUSTSTREAM_REQUIRE_LIVE is set, but PULSAR_TEST_URL is missing or empty",
                );
                None
            }
        }
    }

    // --8<-- [start:both]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<PulsarBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("testing-live-payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app("pulsar://localhost:6650", "payments-svc")).await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = stand() else {
            return Ok(());
        };
        // A subscription of this run's own: it starts at the end of the topic, past what
        // earlier runs published, and its cursor outlives the test on the server.
        let subscription = format!("payments-{}", process::id());
        accepts_a_payment(TestApp::start_live(app(&url, &subscription)).await?).await
    }
    // --8<-- [end:both]
}
