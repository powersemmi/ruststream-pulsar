//! The service the Testing guide tests, written without the macros: card payments keyed by
//! account, a receipt for each, and a review topic for the payments the bank keeps holding.
//!
//! ```text
//! cargo test --example manual_guide_testing --features testing
//! PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --example manual_guide_testing --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::PARTITION_KEY_HEADER;
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;

/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "card-payments")]
struct Payment {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "card-receipts")]
struct Receipt {
    payment: u64,
    account: String,
}

/// Charges a payment to the account its partition key names.
struct Charge;

impl Handle<Payment, Receipt> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        let Some(account) = ctx.headers().get_str(PARTITION_KEY_HEADER) else {
            tracing::warn!(payment.id, "payment without an account dropped");
            return ready(Err(HandlerOutcome::drop()));
        };
        if payment.amount_cents > INSTANT_LIMIT_CENTS {
            tracing::info!(payment.id, "payment held by the bank");
            return ready(Err(HandlerOutcome::retry_after(HOLD)));
        }
        tracing::info!(payment.id, account, "payment charged");
        ready(Ok(Receipt {
            payment: payment.id,
            account: account.to_owned(),
        }))
    }
}

/// The app on the broker it is handed: `main` passes the production one, a test its own.
fn service(broker: PulsarBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("card-payments", Charge).reply().build())
            .max_attempts(nonzero!(3u32))
            .dead_letter("card-payments-review");
    })
}

fn app() -> RustStream {
    service(PulsarBroker::new("pulsar://localhost:6650").default_subscription("payments"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;
    use std::process;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_pulsar::PARTITION_KEY_HEADER;
    use ruststream_pulsar::prelude::*;

    use super::{HOLD, Payment, Receipt, service};

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

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_keyed_payment_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let broker = PulsarBroker::new("pulsar://localhost:6650").default_subscription("payments");
        let tb = TestApp::start(service(broker)).await?;
        a_receipt_names_the_account_of_the_key(tb).await
    }

    async fn a_receipt_names_the_account_of_the_key(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, "acct-7");
        tb.broker::<PulsarBroker>()
            .message(&Payment {
                id: 1,
                amount_cents: 2_500,
            })
            .with_headers(key)
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<PulsarBroker>()
            .published::<Receipt>("card-receipts")
            .assert_called_once()
            .with(&Receipt {
                payment: 1,
                account: "acct-7".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:keyless]
    #[tokio::test]
    async fn a_payment_without_an_account_is_dropped() -> Result<(), Box<dyn Error>> {
        let broker = PulsarBroker::new("pulsar://localhost:6650").default_subscription("payments");
        let tb = TestApp::start(service(broker)).await?;

        tb.broker::<PulsarBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 2_500,
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<PulsarBroker>()
            .published::<Receipt>("card-receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:keyless]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_payment_the_bank_keeps_holding_goes_to_review() -> Result<(), Box<dyn Error>> {
        let broker = PulsarBroker::new("pulsar://localhost:6650").default_subscription("payments");
        let tb = TestApp::start(service(broker)).await?;
        let held = Payment {
            id: 3,
            amount_cents: 250_000,
        };
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, "acct-7");

        tb.broker::<PulsarBroker>()
            .message(&held)
            .with_headers(key)
            .publish()
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // Each delivery waits `HOLD`, then the broker hands the same message over again.
        tb.advance(HOLD).await?;
        tb.broker::<PulsarBroker>()
            .subscriber("card-payments")
            .assert_called(2);

        // The third delivery is the last one the mount site allows: after its wait, the client
        // moves the payment to the review topic.
        tb.advance(HOLD).await?;
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<PulsarBroker>()
                .subscriber("card-payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<PulsarBroker>()
            .published::<Payment>("card-payments-review")
            .assert_called_once()
            .with(&held);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_keyed_payment_gets_a_receipt_on_a_server() -> Result<(), Box<dyn Error>> {
        let Some(url) = stand() else {
            return Ok(());
        };
        let subscription = format!("payments-{}", process::id());
        let broker = PulsarBroker::new(url).default_subscription(subscription);
        let tb = TestApp::start_live(service(broker)).await?;
        a_receipt_names_the_account_of_the_key(tb).await
    }
    // --8<-- [end:live]
}
