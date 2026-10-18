//! Message keys on Pulsar: the partition-key header of a published message becomes the message
//! key, which places it on a partition and orders a `KeyShared` subscription.
//!
//! ```text
//! cargo test --example pulsar_publishing_keys --features testing
//! cargo run --example pulsar_publishing_keys -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:keys]
/// The header a shipment carries: `partition-key`, which becomes the message key.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ShipmentKey {
    #[serde(rename = "partition-key")]
    customer: String,
}

/// Every shipment of one customer carries one key, so the partition and the consumer that
/// take it see that customer's shipments in order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments", headers = ShipmentKey)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

#[subscriber(PulsarSubscription::new("orders", "shipping"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let key = ShipmentKey {
        customer: order.customer.clone(),
    };
    let shipment = Shipment { order_id: order.id };
    if shipments
        .message(&shipment)
        .with_headers(&key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:keys]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(ship).out(Shipments, Publish).build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PARTITION_KEY_HEADER;
    use ruststream_pulsar::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shipment_is_keyed_by_its_customer() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order {
                id: 7,
                customer: "c-42".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header(PARTITION_KEY_HEADER, "c-42");
        tb.shutdown().await?;
        Ok(())
    }
}
