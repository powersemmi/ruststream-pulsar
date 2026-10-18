//! The service of `examples/pulsar_subscriptions_topics.rs` written without the macros: a topic
//! list is the source a registration maps its name to.
//!
//! ```text
//! cargo test --example manual_pulsar_subscriptions_topics --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
    region: String,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the topic.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:descriptor]
/// Fulfils the European and the American orders.
struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, region = %order.region, "order fulfilled");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        // Two topics under one subscription, `regional`, which is also its reported name.
        let regional = PulsarSubscription::topics(["orders-eu", "orders-us"], "regional");
        b.include(
            subscriber("regional", Fulfil)
                .build()
                .map_source(|_| regional),
        );
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
