//! Headers on Pulsar: each header travels as a message property of the same name, so a consumer
//! in any language reads it as a plain Pulsar property.
//!
//! ```text
//! cargo test --example pulsar_publishing_headers --features testing
//! cargo run --example pulsar_publishing_headers -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:headers]
/// The header contract of an invoice: the tenant it belongs to travels as the `tenant` property.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Tenant {
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices", headers = Tenant)]
struct Invoice {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Invoice)]
struct Invoices;

/// Issues the invoice of each order under its tenant.
#[subscriber(PulsarSubscription::new("orders", "billing"))]
async fn issue(order: &Order, Out(invoices): Out<impl Publisher, Invoices>) -> HandlerOutcome {
    let tenant = Tenant {
        tenant: order.tenant.clone(),
    };
    let sent = invoices
        .message(&Invoice { order_id: order.id })
        .with_headers(&tenant)
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(order.id, %err, "the invoice was not issued");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:headers]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(issue).out(Invoices, Publish).build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Invoice, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_invoice_carries_its_tenant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order {
                id: 3,
                tenant: "acme".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice { order_id: 3 })
            .with_header("tenant", "acme");
        tb.shutdown().await?;
        Ok(())
    }
}
