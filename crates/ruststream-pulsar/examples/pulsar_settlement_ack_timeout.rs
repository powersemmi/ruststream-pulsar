//! `ack_timeout` on Pulsar: the broker redelivers a message left unacknowledged longer than the
//! timeout, and a `retry_after` delay has to stay shorter than it.
//!
//! ```text
//! cargo test --example pulsar_settlement_ack_timeout --features testing
//! cargo run --example pulsar_settlement_ack_timeout -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "exports")]
struct Export {
    id: u64,
}

// A stand-in for the archive client: the archive is down for the first upload after startup and
// up for every later one. Wired once at startup, injected through `State`.
#[derive(Clone, Default)]
struct Archive {
    up: Arc<AtomicBool>,
}

impl Archive {
    async fn upload(&self, export: &Export) -> Result<(), String> {
        tokio::task::yield_now().await;
        if self.up.swap(true, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(format!("the archive refused export {}", export.id))
        }
    }
}

#[derive(FromRef)]
struct AppState {
    archive: Archive,
}

// --8<-- [start:descriptor]
/// How long the archive gets before the next upload: shorter than the ack timeout, which would
/// bring the export back first.
const BACKOFF: Duration = Duration::from_secs(30);

/// Uploads each export. An export nobody settles within a minute, because the handler hangs or
/// the service stops, comes back from the broker.
#[subscriber(
    PulsarSubscription::new("exports", "archiver").ack_timeout(Duration::from_secs(60))
)]
async fn upload(export: &Export, State(archive): State<Archive>) -> HandlerOutcome {
    match archive.upload(export).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(export.id, %err, "upload failed, trying again later");
            HandlerOutcome::retry_after(BACKOFF)
        }
    }
}
// --8<-- [end:descriptor]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("archiver", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                archive: Archive::default(),
            })
        })
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            b.include(upload);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{BACKOFF, Export, app};

    #[tokio::test(start_paused = true)]
    async fn a_failed_upload_comes_back_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Export { id: 1 })
            .publish()
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("exports")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF.saturating_sub(Duration::from_millis(1)))
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("exports")
            .assert_called_once();

        tb.advance(Duration::from_millis(1)).await?;
        tb.broker::<PulsarBroker>()
            .subscriber("exports")
            .assert_called(2)
            .with(&Export { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
