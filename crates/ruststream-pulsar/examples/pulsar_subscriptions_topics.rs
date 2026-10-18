//! `PulsarSubscription::topics`: one subscription over a fixed list of topics. Each scenario is a
//! test.
//!
//! ```text
//! cargo test --example pulsar_subscriptions_topics --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    region: String,
}

// --8<-- [start:descriptor]
/// Reads the European and the American orders under one subscription, `regional`.
#[subscriber(PulsarSubscription::topics(["orders-eu", "orders-us"], "regional"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, region = %order.region, "order fulfilled");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    })
}
// --8<-- [end:descriptor]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    fn order(id: u64, region: &str) -> Order {
        Order {
            id,
            region: region.to_owned(),
        }
    }

    #[tokio::test]
    async fn every_listed_topic_reaches_the_subscription() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (topic, order) in [
            ("orders-eu", order(1, "eu")),
            ("orders-us", order(2, "us")),
            ("orders-apac", order(3, "apac")),
        ] {
            tb.broker::<PulsarBroker>()
                .message(&order)
                .to(topic)
                .publish()
                .await?;
        }

        // A list has no single topic, so the subscription reports under its own name.
        assert_eq!(
            tb.broker::<PulsarBroker>()
                .subscriber("regional")
                .assert_called(2)
                .settled(HandlerOutcome::ack())
                .received::<Order>(),
            [order(1, "eu"), order(2, "us")],
        );
        Ok(())
    }
}
