//! The shipping service of the Subscription types and ordering guide: a `Shared` subscription
//! that packs orders and keys each shipment event by its order, a `KeyShared` subscription that
//! tracks shipments in order per key, an `Exclusive` ledger and a `Failover` invoicer.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_subscription_types_and_ordering --features testing -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Payment {
    order: u64,
    amount_cents: u64,
}

// --8<-- [start:shared]
/// One step in the life of a shipment.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order: u64,
    status: String,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// Packs an order. Every instance of the service shares the subscription `packing`.
#[subscriber(PulsarSubscription::new("orders", "packing"))]
async fn pack(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = PulsarPublishOptions>, Shipments>,
) -> HandlerOutcome {
    let packed = Shipment {
        order: order.id,
        status: "packed".to_owned(),
    };
    // The order's id keys the event, so every event of one order keeps its place in line.
    let sent = shipments
        .message(&packed)
        .partition_key(order.id.to_string())
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, "order packed");
    HandlerOutcome::ack()
}
// --8<-- [end:shared]

// --8<-- [start:key_shared]
/// Follows each shipment. Events of one order arrive in the order they were published.
#[subscriber(
    PulsarSubscription::new("shipments", "tracking")
        .subscription_type(SubscriptionType::KeyShared),
    workers(8, by_key)
)]
async fn track(event: &Shipment) -> HandlerOutcome {
    tracing::info!(event.order, event.status, "shipment tracked");
    HandlerOutcome::ack()
}
// --8<-- [end:key_shared]

// --8<-- [start:exclusive]
/// Books payments one after another, in the order they were taken.
#[subscriber(
    PulsarSubscription::new("payments", "ledger").subscription_type(SubscriptionType::Exclusive)
)]
async fn book(payment: &Payment) -> HandlerOutcome {
    tracing::info!(payment.order, payment.amount_cents, "payment booked");
    HandlerOutcome::ack()
}
// --8<-- [end:exclusive]

// --8<-- [start:failover]
/// Invoices orders on one instance at a time; the others stand by.
#[subscriber(
    PulsarSubscription::new("orders", "invoicing").subscription_type(SubscriptionType::Failover)
)]
async fn invoice(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order invoiced");
    HandlerOutcome::ack()
}
// --8<-- [end:failover]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // A second ledger consumer reports the refusal after three tries instead of waiting.
    let retries = OperationRetries::attempts(nonzero!(3u32));
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650").operation_retries(retries),
        |b| {
            b.include(pack).out(Shipments, Publish).build();
            b.include(track);
            b.include(book);
            b.include(invoice);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PARTITION_KEY_HEADER;
    use ruststream_pulsar::prelude::*;
    use serde::Serialize;

    use super::{Order, Payment, Shipment, Shipments, app};

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Placed {
        id: u64,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "payments")]
    struct Paid {
        order: u64,
        amount_cents: u64,
    }

    fn event(order: u64, status: &str) -> Shipment {
        Shipment {
            order,
            status: status.to_owned(),
        }
    }

    #[tokio::test]
    async fn a_packed_order_is_keyed_by_its_id() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Placed { id: 1 })
            .publish()
            .await?;

        // `packing` and `invoicing` each read the order.
        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called(2)
            .with(&Order { id: 1 });
        tb.out::<Shipments>()
            .assert_called_once()
            .with_options(&PulsarPublishOptions {
                partition_key: Some("1".to_owned()),
            });
        // The key reaches the tracking subscription with the event.
        tb.broker::<PulsarBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&event(1, "packed"))
            .with_header(PARTITION_KEY_HEADER, "1");
        tb.broker::<PulsarBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .with(&event(1, "packed"));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_events_of_one_order_are_tracked_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let run = [
            event(2, "packed"),
            event(3, "packed"),
            event(2, "shipped"),
            event(3, "shipped"),
            event(2, "delivered"),
        ];
        for step in &run {
            let mut key = HeaderMap::new();
            key.insert(PARTITION_KEY_HEADER, step.order.to_string());
            tb.broker::<PulsarBroker>()
                .message(step)
                .with_headers(key)
                .publish()
                .await?;
        }

        let tracked = tb
            .broker::<PulsarBroker>()
            .subscriber("shipments")
            .assert_called(5)
            .received::<Shipment>();
        let of_order = |order: u64| -> Vec<&str> {
            tracked
                .iter()
                .filter(|step| step.order == order)
                .map(|step| step.status.as_str())
                .collect()
        };
        assert_eq!(of_order(2), ["packed", "shipped", "delivered"]);
        assert_eq!(of_order(3), ["packed", "shipped"]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_ledger_books_each_payment() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Paid {
                order: 4,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment {
                order: 4,
                amount_cents: 1250,
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
