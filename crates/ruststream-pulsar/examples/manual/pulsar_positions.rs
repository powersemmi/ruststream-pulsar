//! The service of `examples/pulsar_positions.rs` written without `#[subscriber]`: a body reads
//! its position and the seek handle off its Pulsar context.
//!
//! ```text
//! cargo test --example manual_pulsar_positions --features testing
//! cargo run --example manual_pulsar_positions --features testing -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::Arc;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "ledger")]
struct Entry {
    id: u64,
    closes_day: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "ledger.rebuild")]
struct Rebuild {
    reason: String,
}

// --8<-- [start:position]
/// Where the projection last closed a day: the ledger subscription's seeker and the position
/// of the entry that closed it.
#[derive(Clone, Default)]
struct Bookmark(Arc<Mutex<Option<(PulsarSeeker, PulsarPosition)>>>);

struct AppState {
    bookmark: Bookmark,
}

/// Projects each ledger entry and bookmarks the entry that closes a day.
struct Project;

impl Handle<Entry, (), (), PulsarContext, AppState> for Project {
    async fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        ctx: &mut Context<'_, PulsarContext, AppState>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(entry.id, "projected");
        if entry.closes_day {
            let mark = (
                ctx.context(SeekHandle).clone(),
                ctx.context(Position).clone(),
            );
            *ctx.state().bookmark.0.lock().await = Some(mark);
        }
        Ok(())
    }
}

/// Replays the ledger from the last closed day: the entry at the bookmark comes again, then
/// every entry after it, in order.
struct Rewind;

impl Handle<Rebuild, (), (), PulsarContext, AppState> for Rewind {
    async fn handle(
        &self,
        request: &Rebuild,
        _outs: &(),
        ctx: &mut Context<'_, PulsarContext, AppState>,
    ) -> Result<(), HandlerOutcome> {
        let Some((seeker, position)) = ctx.state().bookmark.0.lock().await.clone() else {
            return Ok(());
        };
        tracing::info!(reason = %request.reason, "rebuilding from the last closed day");
        seeker.seek(position).await.map_err(|err| {
            tracing::warn!(%err, "the ledger cannot be rewound");
            HandlerOutcome::retry()
        })
    }
}

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("ledger", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                bookmark: Bookmark::default(),
            })
        })
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            let ledger = PulsarSubscription::new("ledger", "projection");
            let rebuilds = PulsarSubscription::new("ledger.rebuild", "projection");
            b.include(subscriber("ledger", Project).map_source(|_| ledger).build());
            b.include(
                subscriber("ledger.rebuild", Rewind)
                    .map_source(|_| rebuilds)
                    .build(),
            );
        })
}
// --8<-- [end:position]

fn main() -> ExitCode {
    run_main(app)
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
