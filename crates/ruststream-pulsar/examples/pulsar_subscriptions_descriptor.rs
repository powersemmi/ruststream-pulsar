//! `PulsarSubscription::new`: a named durable subscription on one topic. Two subscriptions on one
//! topic each read the whole topic. Each scenario is a test.
//!
//! ```text
//! cargo test --example pulsar_subscriptions_descriptor --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "acme/sales/orders")]
struct Order {
    id: u64,
}

// --8<-- [start:descriptor]
/// Bills every order of the `acme/sales` namespace; the subscription `billing` is its cursor.
#[subscriber(PulsarSubscription::new("acme/sales/orders", "billing"))]
async fn bill(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order billed");
    HandlerOutcome::ack()
}

/// Ships the same orders on a cursor of its own.
#[subscriber(PulsarSubscription::new("acme/sales/orders", "shipping"))]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order shipped");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(bill);
        b.include(ship);
    })
}
// --8<-- [end:descriptor]

/// A namespace with no tenant: Pulsar accepts a bare topic or `tenant/namespace/topic`.
#[subscriber(PulsarSubscription::new("sales/orders", "billing"))]
async fn misnamed(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "never reached");
    HandlerOutcome::ack()
}

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn each_subscription_reads_every_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in [1, 2] {
            tb.broker::<PulsarBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        // Both subscriptions report under their one topic: two orders, two cursors, four calls.
        let mut seen: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("acme/sales/orders")
            .assert_called(4)
            .settled(HandlerOutcome::ack())
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, [1, 1, 2, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn a_topic_name_pulsar_refuses_stops_the_service() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PulsarBroker::new("pulsar://localhost:6650"),
            |b| {
                b.include(misnamed);
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("a two-part topic name is no Pulsar topic");
        assert!(refused.to_string().contains("sales/orders"), "{refused}");
    }
}
