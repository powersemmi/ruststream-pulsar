//! Shutdown: the runtime closes the producers the handlers published through, and a publisher
//! taken off the broker earlier reports `PulsarError::NotConnected` afterwards instead of
//! publishing to a closed connection.
//!
//! ```text
//! cargo test --example pulsar_broker_shutdown --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[subscriber(PulsarSubscription::new("orders", "shipping"), reply)]
async fn ship(order: &Order) -> Shipment {
    tracing::info!(order.id, "order shipped");
    Shipment { order_id: order.id }
}

// --8<-- [start:app]
/// The service on `broker`. Its replies open a producer on `shipments`, which shutdown closes.
fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        b.include(ship).out_reply(Publish);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(PulsarBroker::new("pulsar://localhost:6650"))
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
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
