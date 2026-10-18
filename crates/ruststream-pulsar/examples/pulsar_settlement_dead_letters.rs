//! Dead letters on Pulsar: `max_attempts` with `dead_letter` becomes the consumer's own
//! dead-letter policy, so the client moves a message that used up its deliveries to the declared
//! topic, and the service publishes nothing.
//!
//! ```text
//! cargo test --example pulsar_settlement_dead_letters --features testing
//! cargo run --example pulsar_settlement_dead_letters -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    currency: String,
}

/// Books each payment; a currency the ledger does not know fails every attempt.
#[subscriber(PulsarSubscription::new("payments", "bookkeeping"))]
async fn book(payment: &Payment) -> HandlerOutcome {
    if payment.currency == "EUR" {
        tracing::info!(payment.id, "booked");
        return HandlerOutcome::ack();
    }
    tracing::warn!(payment.id, currency = %payment.currency, "unknown currency");
    HandlerOutcome::retry()
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("bookkeeping", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            // Three deliveries in all; then the client moves the payment to `payments-dlq`.
            b.include(book)
                .max_attempts(nonzero!(3))
                .dead_letter("payments-dlq");
        },
    )
}
// --8<-- [end:app]

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
