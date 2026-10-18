//! The service of `examples/pulsar_broker_shutdown.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pulsar_broker_shutdown --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(Shipment { order_id: order.id }))
    }
}

// --8<-- [start:app]
/// The service on `broker`. Its replies open a producer on `shipments`, which shutdown closes.
fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        let orders = PulsarSubscription::new("orders", "shipping");
        b.include(
            subscriber("orders", Ship)
                .map_source(|_| orders)
                .reply()
                .build(),
        )
        .out_reply(Publish);
    })
}

fn app() -> impl App<State = ()> {
    service(PulsarBroker::new("pulsar://localhost:6650"))
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
}

// `#[outgoing(name = "orders")]` by hand: an order names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

// `#[outgoing(name = "shipments")]` by hand: a shipment names the topic it is published to.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PulsarError;
    use ruststream_pulsar::prelude::*;

    use super::{Order, Shipment, app, service};

    /// The `PulsarError` behind `err`, if any.
    fn pulsar_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a PulsarError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(pulsar) = err.downcast_ref::<PulsarError>() {
                return Some(pulsar);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn a_shipped_order_then_a_clean_shutdown() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_early_publisher_reports_not_connected_after_shutdown() -> Result<(), Box<dyn Error>>
    {
        let broker = PulsarBroker::new("pulsar://localhost:6650");
        let early = broker.publisher();
        let tb = TestApp::start(service(broker)).await?;

        early.message(&Shipment { order_id: 1 }).publish().await?;
        tb.broker::<PulsarBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once();
        tb.shutdown().await?;

        let late = early
            .message(&Shipment { order_id: 2 })
            .publish()
            .await
            .expect_err("a publish after shutdown succeeded");
        assert!(
            matches!(pulsar_error(&late), Some(PulsarError::NotConnected)),
            "{late}"
        );
        Ok(())
    }
}
