//! Testing a service in process: the production app, `PulsarBroker` included, run by `TestApp`
//! with no Pulsar server. A publish through the harness returns once every handler it woke has
//! settled, so the assertions read finished state.
//!
//! ```text
//! cargo run --example pulsar_testing --features testing -- run
//! cargo test --example pulsar_testing --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

/// How long a payment over the review limit waits before it is read again.
const REVIEW: Duration = Duration::from_secs(60);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    amount: u64,
}

#[subscriber(PulsarSubscription::new("payments", "payments-svc"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    if payment.amount > 10_000 {
        tracing::info!(payment.amount, "payment held for review");
        return HandlerOutcome::retry_after(REVIEW);
    }
    HandlerOutcome::ack()
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(accept);
        },
    )
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
