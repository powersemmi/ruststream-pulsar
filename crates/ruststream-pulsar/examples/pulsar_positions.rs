//! Positions on Pulsar: a handler reads where its delivery sits in the log through the `Position`
//! key, and a seek back to that position replays the message and everything after it.
//!
//! ```text
//! cargo test --example pulsar_positions --features testing
//! cargo run --example pulsar_positions -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct Entry {
    id: u64,
    closes_day: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.rebuild")]
struct Rebuild {
    reason: String,
}

// --8<-- [start:position]
/// Where the projection last closed a day: the ledger subscription's seeker and the position
/// of the entry that closed it.
#[derive(Clone, Default)]
struct Bookmark(Arc<Mutex<Option<(PulsarSeeker, PulsarPosition)>>>);

#[derive(FromRef)]
struct AppState {
    bookmark: Bookmark,
}

/// Projects each ledger entry and bookmarks the entry that closes a day.
#[subscriber(PulsarSubscription::new("ledger", "projection"))]
async fn project(
    entry: &Entry,
    Ctx(position): Ctx<Position>,
    Ctx(seeker): Ctx<SeekHandle>,
    State(bookmark): State<Bookmark>,
) -> HandlerOutcome {
    tracing::info!(entry.id, "projected");
    if entry.closes_day {
        *bookmark.0.lock().await = Some((seeker, position));
    }
    HandlerOutcome::ack()
}

/// Replays the ledger from the last closed day: the entry at the bookmark comes again, then
/// every entry after it, in order.
#[subscriber(PulsarSubscription::new("ledger.rebuild", "projection"))]
async fn rebuild(request: &Rebuild, State(bookmark): State<Bookmark>) -> HandlerOutcome {
    let Some((seeker, position)) = bookmark.0.lock().await.clone() else {
        return HandlerOutcome::ack();
    };
    tracing::info!(reason = %request.reason, "rebuilding from the last closed day");
    match seeker.seek(position).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(%err, "the ledger cannot be rewound");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:position]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("ledger", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                bookmark: Bookmark::default(),
            })
        })
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            b.include(project);
            b.include(rebuild);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Entry, Rebuild, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rebuild_replays_the_ledger_from_the_bookmark() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (id, closes_day) in [(1, false), (2, true), (3, false)] {
            tb.broker::<PulsarBroker>()
                .message(&Entry { id, closes_day })
                .publish()
                .await?;
        }
        tb.broker::<PulsarBroker>()
            .message(&Rebuild {
                reason: "a corrected rate".to_owned(),
            })
            .publish()
            .await?;

        let projected: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("ledger")
            .received::<Entry>()
            .iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(projected, [1, 2, 3, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }
}
