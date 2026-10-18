//! The publish policy on Pulsar: a reply leaves through a producer on its destination topic, and
//! the publish returns once the broker has stored the message.
//!
//! ```text
//! cargo test --example pulsar_publishing_publish --features testing
//! cargo run --example pulsar_publishing_publish -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:reply]
/// The receipt of an order; the type names the topic it goes to.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// Confirms each order with a receipt.
#[subscriber(PulsarSubscription::new("orders", "confirmations"), reply)]
async fn confirm(order: &Order) -> Receipt {
    tracing::info!(order.id, "order confirmed");
    Receipt { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(confirm).out_reply(Publish);
        },
    )
}
// --8<-- [end:reply]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_receipt_lands_on_the_topic_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
