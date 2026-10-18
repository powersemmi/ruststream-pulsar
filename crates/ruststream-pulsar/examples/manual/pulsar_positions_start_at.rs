//! The service of `examples/pulsar_positions_start_at.rs` written without `#[subscriber]`:
//! `.start_at(position)` is a step of the mount chain.
//!
//! ```text
//! PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --example manual_pulsar_positions_start_at --features testing
//! cargo run --example manual_pulsar_positions_start_at --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "ledger.audit")]
struct AuditEntry {
    id: u64,
}

// --8<-- [start:start_at]
/// Projects every audit entry it reads.
struct Replay;

impl Handle<AuditEntry> for Replay {
    fn handle(
        &self,
        entry: &AuditEntry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(entry.id, "projected");
        ready(Ok(()))
    }
}

/// The service on the Pulsar cluster `url` names.
fn service(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(PulsarBroker::new(url), |b| {
        // The projection is rebuilt from the oldest retained entry on every startup.
        b.include(
            subscriber(PulsarSubscription::new("ledger.audit", "audit"), Replay)
                .start_at(PulsarPosition::earliest())
                .build(),
        );
    })
}
// --8<-- [end:start_at]

fn app() -> impl App {
    service("pulsar://localhost:6650")
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
#[path = "../support/pulsar_flow_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{AuditEntry, live, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_startup_replays_the_retained_log() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::delete_topic(&url, "ledger.audit");

        // The first run reads the entry and acknowledges it.
        let first = TestApp::start_live(service(&url)).await?;
        first
            .broker::<PulsarBroker>()
            .message(&AuditEntry { id: 1 })
            .publish()
            .await?;
        first.shutdown().await?;

        let tb = TestApp::start_live(service(&url)).await?;
        tb.broker::<PulsarBroker>()
            .message(&AuditEntry { id: 2 })
            .publish()
            .await?;

        // The subscription had acknowledged the first entry, and the startup seek brings it back
        // ahead of the new one.
        let projected = tb
            .broker::<PulsarBroker>()
            .subscriber("ledger.audit")
            .received::<AuditEntry>();
        assert_eq!(projected.first(), Some(&AuditEntry { id: 1 }));
        tb.shutdown().await?;
        Ok(())
    }
}
