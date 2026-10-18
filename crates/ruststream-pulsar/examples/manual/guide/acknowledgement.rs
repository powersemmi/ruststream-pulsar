//! The inventory service of the Acknowledgement guide, written without the macros: each handler
//! is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is
//! written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_acknowledgement --features testing -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::Deserialize;

/// Why the stock service turned a reservation down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The stock service did not answer.
    Unavailable,
    /// No item carries this SKU; another try gets the same answer.
    UnknownSku,
}

/// Stands in for the stock service's client.
#[derive(Debug, Default)]
struct Stock {
    /// How many more calls find the service unavailable.
    unavailable_for: Mutex<u32>,
    /// The counts recorded so far, one entry per batch.
    recorded: Mutex<Vec<usize>>,
}

impl Stock {
    async fn reserve(&self, reservation: &Reservation) -> Result<(), Refusal> {
        tokio::task::yield_now().await;
        if reservation.sku.is_empty() {
            return Err(Refusal::UnknownSku);
        }
        let unavailable = {
            let mut left = self
                .unavailable_for
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let unavailable = *left > 0;
            if unavailable {
                *left -= 1;
            }
            unavailable
        };
        if unavailable {
            Err(Refusal::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn record(&self, counts: &[Count]) {
        tokio::task::yield_now().await;
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(counts.len());
    }
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone)]
struct Clients {
    stock: Arc<Stock>,
}

// --8<-- [start:reserve]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Reservation {
    order: u64,
    sku: String,
    quantity: u32,
}

/// A reservation left unsettled for a minute comes back.
fn reservations() -> PulsarSubscription {
    PulsarSubscription::new("reservations", "inventory").ack_timeout(Duration::from_secs(60))
}

/// Reserves stock for an order.
struct Reserve;

impl Handle<Reservation, (), (), (), Clients> for Reserve {
    async fn handle(
        &self,
        reservation: &Reservation,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), HandlerOutcome> {
        match ctx.state().stock.reserve(reservation).await {
            Ok(()) => {
                tracing::info!(reservation.order, reservation.quantity, "stock reserved");
                Ok(())
            }
            // No answer: the subscription hands the reservation out again at once.
            Err(Refusal::Unavailable) => Err(HandlerOutcome::retry()),
            Err(Refusal::UnknownSku) => {
                tracing::warn!(reservation.order, "unknown SKU, reservation dropped");
                Err(HandlerOutcome::drop())
            }
        }
    }
}
// --8<-- [end:reserve]

// --8<-- [start:counts]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Count {
    sku: String,
    on_hand: u32,
}

/// A batch that is not full closes 250 ms after its first count.
fn stock_counts() -> PulsarSubscription {
    PulsarSubscription::new("stock-counts", "inventory").batch_wait(Duration::from_millis(250))
}

/// Records the stock counts of a warehouse scan; `Ok` settles the whole batch.
struct RecordCounts;

impl Handle<[Count], (), (), (), Clients> for RecordCounts {
    async fn handle(
        &self,
        counts: &[Count],
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        ctx.state().stock.record(counts).await;
        tracing::info!(counts = counts.len(), "stock counts recorded");
        Ok(())
    }
}
// --8<-- [end:counts]

// --8<-- [start:app]
fn app() -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("inventory", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            b.include(subscriber(reservations(), Reserve).build());
            b.include(
                subscriber(stock_counts(), RecordCounts)
                    .batch(nonzero!(64))
                    .build(),
            );
        })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex, PoisonError};

    use futures::future::join_all;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_pulsar::prelude::*;
    use serde::Serialize;

    use super::{Clients, Reservation, Stock, app};

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "reservations")]
    struct Reserve {
        order: u64,
        sku: &'static str,
        quantity: u32,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "stock-counts")]
    struct Counted {
        sku: &'static str,
        on_hand: u32,
    }

    fn stock_unavailable_for(calls: u32) -> Arc<Stock> {
        Arc::new(Stock {
            unavailable_for: Mutex::new(calls),
            ..Stock::default()
        })
    }

    fn clients(stock: &Arc<Stock>) -> Clients {
        Clients {
            stock: Arc::clone(stock),
        }
    }

    #[tokio::test]
    async fn an_unanswered_reservation_comes_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| clients(&stock_unavailable_for(1))).await?;

        tb.broker::<PulsarBroker>()
            .message(&Reserve {
                order: 1,
                sku: "lamp",
                quantity: 2,
            })
            .publish()
            .await?;

        // The first delivery is negatively acknowledged, and the second one is reserved.
        let broker = tb.broker::<PulsarBroker>();
        let reserved = broker.subscriber("reservations").assert_called(2);
        assert_eq!(reserved.outcomes(), [Outcome::Nack, Outcome::Ack]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_sku_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| clients(&stock_unavailable_for(0))).await?;

        tb.broker::<PulsarBroker>()
            .message(&Reserve {
                order: 2,
                sku: "",
                quantity: 1,
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("reservations")
            .assert_called_once()
            .with(&Reservation {
                order: 2,
                sku: String::new(),
                quantity: 1,
            })
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn counts_that_arrive_together_are_recorded_in_one_call() -> Result<(), Box<dyn Error>> {
        let stock = stock_unavailable_for(0);
        let tb = TestApp::with_state(app(), |_| clients(&stock)).await?;

        let counts = [
            Counted {
                sku: "lamp",
                on_hand: 4,
            },
            Counted {
                sku: "desk",
                on_hand: 1,
            },
            Counted {
                sku: "chair",
                on_hand: 9,
            },
        ];
        let broker = tb.broker::<PulsarBroker>();
        for published in join_all(counts.iter().map(|count| broker.message(count).publish())).await
        {
            published?;
        }

        tb.broker::<PulsarBroker>()
            .subscriber("stock-counts")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        let recorded = stock
            .recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert_eq!(recorded, [3]);
        tb.shutdown().await?;
        Ok(())
    }
}
