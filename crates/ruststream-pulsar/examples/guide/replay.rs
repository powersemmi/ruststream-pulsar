//! The search service of the Replay and seeking guide: a ledger projection rebuilt from the
//! retained log on every startup, and a search index that replays the log when the index is lost.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_replay --features testing -- run
//! ```

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the search cluster refused a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchError {
    /// The cluster lost its index; every document has to be indexed again.
    IndexLost,
}

/// Stands in for the search cluster's client.
#[derive(Debug, Default)]
struct Search {
    /// The order whose indexing finds the index lost, once.
    lost_at: Mutex<Option<u64>>,
}

impl Search {
    async fn index(&self, order: &Order) -> Result<(), SearchError> {
        tokio::task::yield_now().await;
        let lost = {
            let mut lost_at = self.lost_at.lock().unwrap_or_else(PoisonError::into_inner);
            let lost = *lost_at == Some(order.id);
            if lost {
                *lost_at = None;
            }
            lost
        };
        if lost {
            Err(SearchError::IndexLost)
        } else {
            Ok(())
        }
    }
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone, FromRef)]
struct Clients {
    search: Arc<Search>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "ledger")]
struct Entry {
    account: String,
    amount_cents: i64,
}

// --8<-- [start:start_at]
/// Rebuilds the account balances from the oldest retained entry on every startup.
#[subscriber(
    PulsarSubscription::new("ledger", "balances"),
    start_at(PulsarPosition::earliest())
)]
async fn project(entry: &Entry) -> HandlerOutcome {
    tracing::info!(entry.account, entry.amount_cents, "balance updated");
    HandlerOutcome::ack()
}
// --8<-- [end:start_at]

// --8<-- [start:seek]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Indexes each order; a lost index sends the subscription back to the oldest retained order.
#[subscriber(PulsarSubscription::new("orders", "search-index"))]
async fn index(
    order: &Order,
    Ctx(seeker): Ctx<SeekHandle>,
    State(search): State<Arc<Search>>,
) -> HandlerOutcome {
    match search.index(order).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(SearchError::IndexLost) => {
            tracing::warn!(order.id, "index lost, indexing every order again");
            if seeker.seek(PulsarPosition::earliest()).await.is_err() {
                return HandlerOutcome::retry();
            }
            HandlerOutcome::ack()
        }
    }
}
// --8<-- [end:seek]

// --8<-- [start:app]
fn service(broker: PulsarBroker) -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("search", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(broker, |b| {
            b.include(project);
            b.include(index);
        })
}

#[ruststream::app]
fn app() -> impl App<State = Clients> {
    service(PulsarBroker::new("pulsar://localhost:6650"))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Clients, Entry, Order, Search, service};

    fn search_lost_at(order: u64) -> Clients {
        Clients {
            search: Arc::new(Search {
                lost_at: Mutex::new(Some(order)),
            }),
        }
    }

    #[tokio::test]
    async fn a_lost_index_is_rebuilt_from_the_retained_log() -> Result<(), Box<dyn Error>> {
        let broker = PulsarBroker::new("pulsar://localhost:6650");
        // Taken before the app is built, so the whole run is queued before the first delivery.
        let ingress = broker.publisher();
        let tb = TestApp::with_state(service(broker), |_| search_lost_at(2)).await?;

        for id in 1..=3 {
            ingress.message(&Order { id }).publish().await?;
        }
        tb.settle().await?;

        // Order 3 was queued when the index was lost: the seek dropped it, and the replay
        // brought every order back from the start.
        let indexed: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called(5)
            .settled(HandlerOutcome::ack())
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(indexed, [1, 2, 1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_new_entry_updates_the_balances() -> Result<(), Box<dyn Error>> {
        let broker = PulsarBroker::new("pulsar://localhost:6650");
        let tb = TestApp::with_state(service(broker), |_| Clients::default()).await?;

        let entry = Entry {
            account: "acct-7".to_owned(),
            amount_cents: -1_250,
        };
        tb.broker::<PulsarBroker>()
            .message(&entry)
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("ledger")
            .assert_called_once()
            .with(&entry)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
