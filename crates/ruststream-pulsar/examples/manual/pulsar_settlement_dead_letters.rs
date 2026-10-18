//! The service of `examples/pulsar_settlement_dead_letters.rs` written without `#[subscriber]`:
//! the same two steps at the mount site declare the consumer's dead-letter policy.
//!
//! ```text
//! cargo test --example manual_pulsar_settlement_dead_letters --features testing
//! cargo run --example manual_pulsar_settlement_dead_letters --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    currency: String,
}

/// Books each payment; a currency the ledger does not know fails every attempt.
struct Book;

impl Handle<Payment> for Book {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.currency == "EUR" {
            tracing::info!(payment.id, "booked");
            return ready(Ok(()));
        }
        tracing::warn!(payment.id, currency = %payment.currency, "unknown currency");
        ready(Err(HandlerOutcome::retry()))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("bookkeeping", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            let payments = PulsarSubscription::new("payments", "bookkeeping");
            // Three deliveries in all; then the client moves the payment to `payments-dlq`.
            b.include(
                subscriber("payments", Book)
                    .map_source(|_| payments)
                    .build(),
            )
            .max_attempts(nonzero!(3))
            .dead_letter("payments-dlq");
        },
    )
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Payment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payment_that_keeps_failing_ends_in_the_dead_letter_topic()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 1,
            currency: "XTS".to_owned(),
        };
        tb.broker::<PulsarBroker>()
            .message(&payment)
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("payments")
            .assert_called(3);
        tb.broker::<PulsarBroker>()
            .published::<Payment>("payments-dlq")
            .assert_called_once()
            .with(&payment);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_booked_payment_stays_out_of_it() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Payment {
                id: 2,
                currency: "EUR".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<PulsarBroker>()
            .published::<Payment>("payments-dlq")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
