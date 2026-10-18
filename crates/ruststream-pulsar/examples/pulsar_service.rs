//! A minimal Pulsar service: a shared subscription with a retry cap and a dead-letter topic.
//!
//! ```text
//! just brokers-up
//! cargo run --example pulsar_service --features testing -- run
//! cargo test --example pulsar_service --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use std::time::Duration;

use ruststream_pulsar::prelude::*;

#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
        .ack_timeout(Duration::from_secs(30))
)]
async fn handle(order: &Order) -> HandlerOutcome {
    if order.items == 0 {
        // Comes back, up to five deliveries in all, then moves to `orders-dlq`.
        tracing::warn!(subscription = "workers", order.id, "order has no items yet");
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, order.items, "order received");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(handle)
                .max_attempts(nonzero!(5))
                .dead_letter("orders-dlq");
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_with_items_is_acknowledged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7, items: 2 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7, items: 2 })
            .settled(HandlerOutcome::ack());
        tb.broker::<PulsarBroker>()
            .published::<Order>("orders-dlq")
            .assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn an_order_retried_five_times_moves_to_the_dead_letter_topic()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 8, items: 0 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called(5);
        tb.broker::<PulsarBroker>()
            .published::<Order>("orders-dlq")
            .assert_called_once()
            .with(&Order { id: 8, items: 0 });
        Ok(())
    }
}
