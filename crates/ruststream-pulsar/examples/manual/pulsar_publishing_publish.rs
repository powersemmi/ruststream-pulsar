//! The service of `examples/pulsar_publishing_publish.rs` written without `#[subscriber]`: the
//! reply body implements `Handle`, and the mount site pairs the same publish policy.
//!
//! ```text
//! cargo test --example manual_pulsar_publishing_publish --features testing
//! cargo run --example manual_pulsar_publishing_publish --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:reply]
/// The receipt of an order; the type names the topic it goes to.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// Confirms each order with a receipt.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Receipt { order_id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            let confirm = subscriber(PulsarSubscription::new("orders", "confirmations"), Confirm);
            b.include(confirm.reply().build()).out_reply(Publish);
        },
    )
}
// --8<-- [end:reply]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_receipt_lands_on_the_topic_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
