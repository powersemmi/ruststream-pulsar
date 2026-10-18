//! A subscription by topic name: `#[subscriber("orders")]` reads the topic `orders` under the
//! broker's default subscription. Each scenario is a test.
//!
//! ```text
//! cargo test --example pulsar_subscriptions_name --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:name]
/// Reads the topic `orders`; the subscription comes from the broker.
#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650").default_subscription("orders-svc");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:name]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_topic_is_read_under_the_default_subscription() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_service_with_no_default_subscription_does_not_start() {
        let unnamed = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PulsarBroker::new("pulsar://localhost:6650"),
            |b| {
                b.include(place);
            },
        );

        let refused = TestApp::start(unnamed)
            .await
            .expect_err("Pulsar reads a topic only through a named subscription");
        assert!(
            refused.to_string().contains("default_subscription"),
            "{refused}"
        );
    }
}
