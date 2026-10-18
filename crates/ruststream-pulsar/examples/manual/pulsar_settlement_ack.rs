//! The service of `examples/pulsar_settlement_ack.rs` written without `#[subscriber]`: the body
//! implements `Handle`, and its `Err(outcome)` settles the delivery on the broker.
//!
//! ```text
//! cargo test --example manual_pulsar_settlement_ack --features testing
//! cargo run --example manual_pulsar_settlement_ack --features testing -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    quantity: u32,
}

// A stand-in for the warehouse client: the first reservation after startup finds the stock
// count still loading, every later one succeeds. Wired once at startup, read through
// `Context::state`.
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

/// What the handlers share, built once at startup.
struct AppState {
    warehouse: Warehouse,
}

// --8<-- [start:handler]
/// Reserves the stock of each order. `Ok` acknowledges the message; `Err(retry)` negatively
/// acknowledges it and the subscription delivers it again; `Err(drop)` acknowledges it as well,
/// so an order of nothing is never seen again.
struct Fulfil;

impl Handle<Order, (), (), (), AppState> for Fulfil {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if order.quantity == 0 {
            tracing::warn!(order.id, "an order of nothing is dropped");
            return Err(HandlerOutcome::drop());
        }
        ctx.state().warehouse.reserve(order).await.map_err(|err| {
            tracing::warn!(order.id, %err, "reservation failed, redelivering");
            HandlerOutcome::retry()
        })
    }
}
// --8<-- [end:handler]

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                warehouse: Warehouse::default(),
            })
        })
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            b.include(subscriber(PulsarSubscription::new("orders", "fulfilment"), Fulfil).build());
        })
}

fn main() -> ExitCode {
    run_main(app)
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
