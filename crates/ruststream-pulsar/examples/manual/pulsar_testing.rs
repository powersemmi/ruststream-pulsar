//! The service of `examples/pulsar_testing.rs` without the macros: the handler is a named type and
//! `main` is written by hand. The tests are the same.
//!
//! ```text
//! cargo run --example manual_pulsar_testing --features testing -- run
//! cargo test --example manual_pulsar_testing --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

/// How long a payment over the review limit waits before it is read again.
const REVIEW: Duration = Duration::from_secs(60);

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "payments")]` by hand: a payment names the topic it is published to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        if payment.amount > 10_000 {
            tracing::info!(payment.amount, "payment held for review");
            return ready(Err(HandlerOutcome::retry_after(REVIEW)));
        }
        ready(Ok(()))
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            let payments = PulsarSubscription::new("payments", "payments-svc");
            b.include(
                subscriber("payments", Accept)
                    .map_source(|_| payments)
                    .build(),
            );
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Payment, REVIEW, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn an_empty_payment_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Payment { amount: 0 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment { amount: 0 })
            .settled(HandlerOutcome::drop());
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:advance]
    #[tokio::test(start_paused = true)]
    async fn a_held_payment_is_read_again_after_the_review_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Payment { amount: 50_000 })
            .publish()
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(REVIEW));

        // The clock is paused: the delay passes when the test says so, not in real time.
        tb.advance(REVIEW).await?;
        tb.broker::<PulsarBroker>()
            .subscriber("payments")
            .assert_called(2);
        Ok(())
    }
    // --8<-- [end:advance]
}
