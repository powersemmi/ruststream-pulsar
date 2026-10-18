//! Settling a Pulsar delivery: `ack` acknowledges the message, `retry` negatively acknowledges it
//! and the subscription delivers it again, `drop` acknowledges it too, since Pulsar has no
//! terminal reject.
//!
//! ```text
//! cargo test --example pulsar_settlement_ack --features testing
//! cargo run --example pulsar_settlement_ack -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    quantity: u32,
}

// A stand-in for the warehouse client: the first reservation after startup finds the stock
// count still loading, every later one succeeds. Wired once at startup, injected through `State`.
#[derive(Clone, Default)]
struct Warehouse {
    loaded: Arc<AtomicBool>,
}

impl Warehouse {
    async fn reserve(&self, order: &Order) -> Result<(), String> {
        tokio::task::yield_now().await;
        if self.loaded.swap(true, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(format!("stock count still loading for order {}", order.id))
        }
    }
}

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    warehouse: Warehouse,
}

// --8<-- [start:handler]
/// Reserves the stock of each order. `ack` acknowledges the message; `retry` negatively
/// acknowledges it and the subscription delivers it again; `drop` acknowledges it as well, so an
/// order of nothing is never seen again.
#[subscriber(PulsarSubscription::new("orders", "fulfilment"))]
async fn fulfil(order: &Order, State(warehouse): State<Warehouse>) -> HandlerOutcome {
    if order.quantity == 0 {
        tracing::warn!(order.id, "an order of nothing is dropped");
        return HandlerOutcome::drop();
    }
    match warehouse.reserve(order).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(order.id, %err, "reservation failed, redelivering");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                warehouse: Warehouse::default(),
            })
        })
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            b.include(fulfil);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_pulsar::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_order_is_delivered_again_and_acknowledged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order { id: 1, quantity: 2 };
        tb.broker::<PulsarBroker>()
            .message(&order)
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called(2)
            .with(&order);
        assert_eq!(
            tb.broker::<PulsarBroker>().subscriber("orders").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_order_is_not_delivered_again() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 2, quantity: 0 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
