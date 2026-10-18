//! The service of `examples/pulsar_subscriptions_topic.rs` written without the macros: the
//! handler is a named type and the slot a marker type.
//!
//! ```text
//! cargo test --example manual_pulsar_subscriptions_topic --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    tenant: String,
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the topic.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:route]
/// The tenants' own order topics.
struct TenantOrders;

impl OutSlot for TenantOrders {
    const NAME: &'static str = "TenantOrders";
    type Destination = Reads;
}

impl PublishedThrough<TenantOrders> for Order {}

/// Fans orders out to their tenant's topic. The tenant comes off the wire, so its topic is
/// parsed, and an order naming a tenant Pulsar would refuse is dropped here.
struct Route;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Route
where
    Entry: OutEntry<TenantOrders, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(topic) = PulsarTopic::parse(&format!("{}/orders/created", order.tenant)) else {
            tracing::warn!(order.id, tenant = %order.tenant, "no such tenant topic");
            return Err(HandlerOutcome::drop());
        };
        let tenants = outs.get(TenantOrders);
        match tenants.message(order).to(topic.as_str()).publish().await {
            Ok(()) => Ok(()),
            Err(_) => Err(HandlerOutcome::retry()),
        }
    }
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("router", "0.1.0")).with_broker(broker, |b| {
        let orders = PulsarSubscription::new("orders", "router");
        b.include(subscriber("orders", Route).build().map_source(|_| orders))
            .out(TenantOrders, Publish)
            .build();
    })
}
// --8<-- [end:route]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    fn order(tenant: &str, id: u64) -> Order {
        Order {
            tenant: tenant.to_owned(),
            id,
        }
    }

    #[tokio::test]
    async fn an_order_reaches_its_tenants_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&order("acme", 1))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Order>("persistent://acme/orders/created")
            .assert_called_once()
            .with(&order("acme", 1));
        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_tenant_pulsar_refuses_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&order("acme corp", 2))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.out::<TenantOrders>().assert_not_called();
        Ok(())
    }
}
