//! The service of `examples/pulsar_publishing_headers.rs` written without `#[subscriber]`: the
//! body declares its publishing slot in the injections arena and sets the header contract there.
//!
//! ```text
//! cargo test --example manual_pulsar_publishing_headers --features testing
//! cargo run --example manual_pulsar_publishing_headers --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:headers]
/// The header contract of an invoice: the tenant it belongs to travels as the `tenant` property.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Tenant {
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "invoices", headers = Tenant)]
struct Invoice {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Invoice)]
struct Invoices;

/// Issues the invoice of each order under its tenant.
struct Issue;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Issue
where
    Entry: OutEntry<Invoices, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let tenant = Tenant {
            tenant: order.tenant.clone(),
        };
        outs.get(Invoices)
            .message(&Invoice { order_id: order.id })
            .with_headers(&tenant)
            .publish()
            .await
            .map_err(|err| {
                tracing::warn!(order.id, %err, "the invoice was not issued");
                HandlerOutcome::retry()
            })
    }
}
// --8<-- [end:headers]

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            let issue = subscriber(PulsarSubscription::new("orders", "billing"), Issue);
            b.include(issue.build()).out(Invoices, Publish).build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
