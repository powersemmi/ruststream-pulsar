//! The inventory service of the Acknowledgement guide: reservations settled by their outcome,
//! under an acknowledgement timeout, and stock counts settled a batch at a time.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_acknowledgement --features testing -- run
//! ```

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

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
#[derive(Debug, Default, Clone, FromRef)]
struct Clients {
    stock: Arc<Stock>,
}

// --8<-- [start:reserve]
#[derive(Debug, PartialEq, Deserialize)]
struct Reservation {
    order: u64,
    sku: String,
    quantity: u32,
}

/// Reserves stock for an order. A reservation left unsettled for a minute comes back.
#[subscriber(
    PulsarSubscription::new("reservations", "inventory").ack_timeout(Duration::from_secs(60))
)]
async fn reserve(reservation: &Reservation, State(stock): State<Arc<Stock>>) -> HandlerOutcome {
    match stock.reserve(reservation).await {
        Ok(()) => {
            tracing::info!(reservation.order, reservation.quantity, "stock reserved");
            HandlerOutcome::ack()
        }
        // No answer: the subscription hands the reservation out again at once.
        Err(Refusal::Unavailable) => HandlerOutcome::retry(),
        Err(Refusal::UnknownSku) => {
            tracing::warn!(reservation.order, "unknown SKU, reservation dropped");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:reserve]

// --8<-- [start:counts]
#[derive(Debug, PartialEq, Deserialize)]
struct Count {
    sku: String,
    on_hand: u32,
}

/// Records the stock counts of a warehouse scan, many counts per call.
#[subscriber(
    PulsarSubscription::new("stock-counts", "inventory").batch_wait(Duration::from_millis(250))
)]
async fn record_counts(counts: &[Count], State(stock): State<Arc<Stock>>) -> HandlerOutcome {
    stock.record(counts).await;
    tracing::info!(counts = counts.len(), "stock counts recorded");
    HandlerOutcome::ack()
}
// --8<-- [end:counts]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("inventory", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            b.include(reserve);
            b.include(record_counts.batch(nonzero!(64)));
        })
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
