//! The service of `examples/pulsar_settlement_ack_timeout.rs` written without `#[subscriber]`:
//! the descriptor with its ack timeout is the mount site's source.
//!
//! ```text
//! cargo test --example manual_pulsar_settlement_ack_timeout --features testing
//! cargo run --example manual_pulsar_settlement_ack_timeout --features testing -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "exports")]
struct Export {
    id: u64,
}

// A stand-in for the archive client: the archive is down for the first upload after startup and
// up for every later one. Wired once at startup, read through `Context::state`.
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

/// What the handler shares, built once at startup.
struct AppState {
    archive: Archive,
}

// --8<-- [start:descriptor]
/// How long the archive gets before the next upload: shorter than the ack timeout, which would
/// bring the export back first.
const BACKOFF: Duration = Duration::from_secs(30);

/// Uploads each export.
struct Upload;

impl Handle<Export, (), (), (), AppState> for Upload {
    async fn handle(
        &self,
        export: &Export,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        ctx.state().archive.upload(export).await.map_err(|err| {
            tracing::warn!(export.id, %err, "upload failed, trying again later");
            HandlerOutcome::retry_after(BACKOFF)
        })
    }
}

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("archiver", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                archive: Archive::default(),
            })
        })
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            // An export nobody settles within a minute, because the handler hangs or the service
            // stops, comes back from the broker.
            b.include(
                subscriber(
                    PulsarSubscription::new("exports", "archiver")
                        .ack_timeout(Duration::from_secs(60)),
                    Upload,
                )
                .build(),
            );
        })
}
// --8<-- [end:descriptor]

fn main() -> ExitCode {
    run_main(app)
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
