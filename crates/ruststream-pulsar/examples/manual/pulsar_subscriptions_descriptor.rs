//! The service of `examples/pulsar_subscriptions_descriptor.rs` written without the macros: a
//! `PulsarSubscription` is the source a registration maps its name to.
//!
//! ```text
//! cargo test --example manual_pulsar_subscriptions_descriptor --features testing
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
}

// `#[outgoing(name = "acme/sales/orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "acme/sales/orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:descriptor]
/// Bills every order of the `acme/sales` namespace.
struct Bill;

impl Handle<Order> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order billed");
        ready(Ok(()))
    }
}

/// Ships the same orders.
struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Each subscription is a cursor of its own: `billing` and `shipping` read every order.
        b.include(
            subscriber(
                PulsarSubscription::new("acme/sales/orders", "billing"),
                Bill,
            )
            .build(),
        );
        b.include(
            subscriber(
                PulsarSubscription::new("acme/sales/orders", "shipping"),
                Ship,
            )
            .build(),
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
        // A namespace with no tenant: Pulsar accepts a bare topic or `tenant/namespace/topic`.
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PulsarBroker::new("pulsar://localhost:6650"),
            |b| {
                b.include(
                    subscriber(PulsarSubscription::new("sales/orders", "billing"), Bill).build(),
                );
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("a two-part topic name is no Pulsar topic");
        assert!(refused.to_string().contains("sales/orders"), "{refused}");
    }
}
