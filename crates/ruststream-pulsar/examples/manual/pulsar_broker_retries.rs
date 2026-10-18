//! The service of `examples/pulsar_broker_retries.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pulsar_broker_retries --features testing
//! just brokers-up
//! PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --example manual_pulsar_broker_retries --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

struct Record;

impl Handle<Entry> for Record {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(entry.id, "entry recorded");
        ready(Ok(()))
    }
}

/// One writer at a time: the ledger is appended in order.
fn ledger(topic: impl Into<String>) -> PulsarSubscription {
    PulsarSubscription::new(topic, "ledger-writer").subscription_type(SubscriptionType::Exclusive)
}

fn service(broker: PulsarBroker, ledger: PulsarSubscription) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("ledger", Record).map_source(|_| ledger).build());
    })
}

// --8<-- [start:broker]
/// The cluster at `url`; an operation the server keeps refusing fails after three tries.
fn broker(url: &str) -> PulsarBroker {
    PulsarBroker::new(url)
        .operation_retries(OperationRetries::attempts(nonzero!(3u32)).delay(Duration::from_secs(1)))
}

fn app() -> impl App<State = ()> {
    service(broker("pulsar://localhost:6650"), ledger("ledger"))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Entry {
    id: u64,
}

// `#[outgoing(name = "ledger")]` by hand: an entry names the topic it is published to.
impl OutgoingDestination for Entry {
    type Form = FixedName;
    const DESTINATION: &'static str = "ledger";
}

impl MessageHeaders for Entry {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;
    use std::process;
    use std::time::{Duration, Instant};

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PulsarError;
    use ruststream_pulsar::prelude::*;
    use tokio::time::timeout;

    use super::{Entry, app, broker, ledger, service};

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

    /// The `PulsarError` behind `err`, if any.
    fn pulsar_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a PulsarError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(pulsar) = err.downcast_ref::<PulsarError>() {
                return Some(pulsar);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn a_bounded_broker_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Entry { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("ledger")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_taken_exclusive_subscription_fails_startup_after_three_tries()
    -> Result<(), Box<dyn Error>> {
        let Some(url) = stand() else {
            return Ok(());
        };
        // A topic of this run's own: it and its subscription outlive the test on the server.
        let topic = format!("manual-broker-retries-ledger-{}", process::id());
        let writer = || service(broker(&url), ledger(topic.clone()));

        let holder = TestApp::start_live(writer()).await?;
        let started = Instant::now();
        // Unbounded, the client would wait for the holder to leave and startup would never end.
        let second = timeout(Duration::from_secs(30), TestApp::start_live(writer()))
            .await
            .map_err(|_| "startup still waits for the exclusive subscription")?
            .expect_err("a second consumer took the exclusive subscription");

        assert!(
            matches!(pulsar_error(&second), Some(PulsarError::Subscribe { .. })),
            "{second}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "startup gave up after {:?}",
            started.elapsed()
        );
        holder.shutdown().await?;
        Ok(())
    }
}
