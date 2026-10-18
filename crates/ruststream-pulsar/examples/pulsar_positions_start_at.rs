//! `start_at` on Pulsar: the subscription is moved to a position on every startup, so a
//! projection rebuilt from the retained log reads it from the start each time the service boots.
//! The scenario runs against a broker, since it shows on entries an earlier run left behind.
//!
//! ```text
//! PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --example pulsar_positions_start_at --features testing
//! cargo run --example pulsar_positions_start_at -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.audit")]
struct AuditEntry {
    id: u64,
}

// --8<-- [start:start_at]
/// Rebuilds the audit projection from the oldest retained entry on every startup.
#[subscriber(
    PulsarSubscription::new("ledger.audit", "audit"),
    start_at(PulsarPosition::earliest())
)]
async fn replay(entry: &AuditEntry) -> HandlerOutcome {
    tracing::info!(entry.id, "projected");
    HandlerOutcome::ack()
}
// --8<-- [end:start_at]

/// The service on the Pulsar cluster `url` names.
fn service(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(PulsarBroker::new(url), |b| {
        b.include(replay);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service("pulsar://localhost:6650")
}

#[cfg(test)]
#[path = "support/pulsar_flow_live.rs"]
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
