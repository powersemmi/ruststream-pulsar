//! The service of `examples/pulsar_publishing_partition_key.rs` written without `#[subscriber]`:
//! the body bounds its slot on `PulsarPublishOptions`, which is what puts the `partition_key`
//! step in reach.
//!
//! ```text
//! cargo test --example manual_pulsar_publishing_partition_key --features testing
//! cargo run --example manual_pulsar_publishing_partition_key --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    user: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Ledger;

// --8<-- [start:handler]
/// Records a receipt per order, keyed by the user who placed it, so one user's receipts stay
/// in order.
struct Record;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Record
where
    Entry: OutEntry<Ledger, Wire: Publisher<Options = PulsarPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        outs.get(Ledger)
            .message(&Receipt { order_id: order.id })
            .partition_key(format!("user-{}", order.user))
            .publish()
            .await
            .map_err(|err| {
                tracing::warn!(order.id, %err, "the receipt was not recorded");
                HandlerOutcome::retry()
            })
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            let orders = PulsarSubscription::new("orders", "ledger");
            let record = subscriber("orders", Record).map_source(|_| orders);
            b.include(record.build()).out(Ledger, Publish).build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PARTITION_KEY_HEADER;
    use ruststream_pulsar::prelude::*;

    use super::{Ledger, Order, Receipt, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_receipt_is_keyed_by_its_user() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7, user: 42 })
            .publish()
            .await?;

        tb.out::<Ledger>()
            .assert_called_once()
            .with_options(&PulsarPublishOptions {
                partition_key: Some("user-42".to_owned()),
            });
        tb.broker::<PulsarBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 })
            .with_header(PARTITION_KEY_HEADER, "user-42");
        tb.shutdown().await?;
        Ok(())
    }
}
