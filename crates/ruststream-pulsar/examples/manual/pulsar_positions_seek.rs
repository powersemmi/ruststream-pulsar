//! The service of `examples/pulsar_positions_seek.rs` written without `#[subscriber]`: the body
//! reads the seek handle off its Pulsar context.
//!
//! ```text
//! cargo test --example manual_pulsar_positions_seek --features testing
//! cargo run --example manual_pulsar_positions_seek --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// A job; the producer marks the first job of a run it knows to be unprocessable.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    poisoned: bool,
}

// --8<-- [start:seek]
/// Skips a poisoned run: the subscription jumps to the tip of the log, and the jobs queued
/// behind the marker never reach the handler.
struct Work;

impl Handle<Job, (), (), PulsarContext> for Work {
    async fn handle(
        &self,
        job: &Job,
        _outs: &(),
        ctx: &mut Context<'_, PulsarContext>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(job.id, "worked");
        if !job.poisoned {
            return Ok(());
        }
        ctx.context(SeekHandle)
            .seek(PulsarPosition::latest())
            .await
            .map_err(|err| {
                tracing::warn!(job.id, %err, "the poisoned run cannot be skipped");
                HandlerOutcome::retry()
            })
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            let jobs = PulsarSubscription::new("jobs", "workers");
            b.include(subscriber("jobs", Work).map_source(|_| jobs).build());
        },
    )
}
// --8<-- [end:seek]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn the_jobs_behind_the_marker_are_skipped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Job 2 marks a poisoned run; all four wait in the queue before the first is handled.
        let jobs: Vec<Job> = (1..=4)
            .map(|id| Job {
                id,
                poisoned: id == 2,
            })
            .collect();
        let broker = tb.broker::<PulsarBroker>();
        for published in join_all(jobs.iter().map(|job| broker.message(job).publish())).await {
            published?;
        }
        // The subscription is live at its new position.
        broker
            .message(&Job {
                id: 5,
                poisoned: false,
            })
            .publish()
            .await?;

        let worked: Vec<u64> = broker
            .subscriber("jobs")
            .received::<Job>()
            .iter()
            .map(|job| job.id)
            .collect();
        assert_eq!(worked, [1, 2, 5]);
        tb.shutdown().await?;
        Ok(())
    }
}
