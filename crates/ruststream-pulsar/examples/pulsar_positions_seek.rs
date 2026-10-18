//! Seeking from a handler on Pulsar: the `SeekHandle` key moves the whole subscription, and the
//! deliveries queued behind the seek are dropped.
//!
//! ```text
//! cargo test --example pulsar_positions_seek --features testing
//! cargo run --example pulsar_positions_seek -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// A job; the producer marks the first job of a run it knows to be unprocessable.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    poisoned: bool,
}

// --8<-- [start:seek]
/// Skips a poisoned run: the subscription jumps to the tip of the log, and the jobs queued
/// behind the marker never reach the handler.
#[subscriber(PulsarSubscription::new("jobs", "workers"))]
async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    tracing::info!(job.id, "worked");
    if !job.poisoned {
        return HandlerOutcome::ack();
    }
    match seeker.seek(PulsarPosition::latest()).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(job.id, %err, "the poisoned run cannot be skipped");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:seek]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(work);
        },
    )
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
