//! `PulsarError` where a service meets it: a publish the broker refuses, and a startup that fails.
//!
//! ```text
//! cargo test --example pulsar_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// --8<-- [start:handler]
#[subscriber(PulsarSubscription::new("orders", "shipping"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let shipment = Shipment {
        order_id: order.id,
        labels: (1..=order.parcels)
            .map(|parcel| format!("{}-{parcel:06}", order.id))
            .collect(),
    };
    if let Err(err) = shipments.message(&shipment).publish().await {
        // The error names the topic and the reason.
        tracing::warn!(subscription = "shipping", order.id, error = %err, "shipment not published");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        b.include(ship)
            .out(Shipments, Publish)
            .max_attempts(nonzero!(3))
            .dead_letter("orders-dlq")
            .build();
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(PulsarBroker::new("pulsar://localhost:6650"))
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    parcels: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
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
    async fn a_shipment_the_broker_takes_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7, parcels: 1 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<PulsarBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment {
                order_id: 7,
                labels: vec!["7-000001".to_owned()],
            });
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_shipment_retries_the_order_until_the_cap() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // Six hundred thousand labels encode to more than the five-mebibyte payload limit.
        let order = Order {
            id: 8,
            parcels: 600_000,
        };
        tb.broker::<PulsarBroker>()
            .message(&order)
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called(3);
        tb.broker::<PulsarBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        tb.broker::<PulsarBroker>()
            .published::<Order>("orders-dlq")
            .assert_called_once()
            .with(&order);
        Ok(())
    }

    #[tokio::test]
    async fn a_url_the_client_cannot_parse_fails_startup_with_connect() {
        let failed = TestApp::start(service(PulsarBroker::new("localhost:6650")))
            .await
            .expect_err("startup took a URL with no pulsar scheme");

        assert!(
            matches!(pulsar_error(&failed), Some(PulsarError::Connect(_))),
            "{failed}"
        );
    }
}
