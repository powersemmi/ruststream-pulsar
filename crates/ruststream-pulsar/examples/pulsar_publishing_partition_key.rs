//! The `partition_key` step on Pulsar: a handler body keys one publish of a slot, and the message
//! still leaves through that slot, in its codec.
//!
//! ```text
//! cargo test --example pulsar_publishing_partition_key --features testing
//! cargo run --example pulsar_publishing_partition_key -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    user: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Ledger;

// --8<-- [start:handler]
/// Records a receipt per order, keyed by the user who placed it, so one user's receipts stay
/// in order. A body that names a per-message setting imports this broker's prelude and states
/// the settings type on its slot.
#[subscriber(PulsarSubscription::new("orders", "ledger"))]
async fn record(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = PulsarPublishOptions>, Ledger>,
) -> HandlerOutcome {
    let sent = ledger
        .message(&Receipt { order_id: order.id })
        .partition_key(format!("user-{}", order.user))
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(order.id, %err, "the receipt was not recorded");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(record).out(Ledger, Publish).build();
        },
    )
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
