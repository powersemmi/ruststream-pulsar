//! The service of `examples/pulsar_service.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! just brokers-up
//! cargo run --example manual_pulsar_service --features testing -- run
//! cargo test --example manual_pulsar_service --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use std::time::Duration;

use ruststream_pulsar::prelude::*;

struct HandleOrder;

impl Handle<Order> for HandleOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if order.items == 0 {
            // Comes back, up to five deliveries in all, then moves to `orders-dlq`.
            tracing::warn!(subscription = "workers", order.id, "order has no items yet");
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(order.id, order.items, "order received");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            // The shared subscription `workers` on `orders`.
            b.include(
                subscriber(
                    PulsarSubscription::new("orders", "workers")
                        .subscription_type(SubscriptionType::Shared)
                        .ack_timeout(Duration::from_secs(30)),
                    HandleOrder,
                )
                .build(),
            )
            .max_attempts(nonzero!(5))
            .dead_letter("orders-dlq");
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    items: u32,
}

// `#[outgoing(name = "orders")]` by hand: an order names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
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
