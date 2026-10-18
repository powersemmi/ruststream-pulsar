//! The services of `examples/pulsar_context.rs` written without the macros: a body that reads the
//! delivery's context names it as its context axis, `PulsarContext` for one delivery and
//! `PulsarBatchContext` for a batch.
//!
//! ```text
//! cargo test --example manual_pulsar_context --features testing
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::collections::HashSet;
use std::sync::Mutex;

use ruststream::schemars::{self, JsonSchema};
use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Job {
    id: u64,
    /// Set by the producer when a downstream system lost the job's result.
    redo: bool,
}

// `#[outgoing(name = "jobs")]` by hand: the type fixes its destination.
impl OutgoingDestination for Job {
    type Form = FixedName;

    const DESTINATION: &'static str = "jobs";
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

/// The jobs that already asked for a redo, so the redone job does not ask again.
#[derive(Debug, Default)]
struct Redos {
    asked: Mutex<HashSet<u64>>,
}

impl Redos {
    fn first_time(&self, job: &Job) -> bool {
        self.asked
            .lock()
            .is_ok_and(|mut asked| asked.insert(job.id))
    }
}

mod delivery {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Job, Redos};

    /// What the handler shares across deliveries.
    #[derive(Clone, Default)]
    struct AppState {
        redos: Arc<Redos>,
    }

    // --8<-- [start:context]
    /// Logs where each job sits; a job whose result was lost moves the subscription back to its
    /// own position, so the job is delivered again.
    struct Work;

    impl Handle<Job, (), (), PulsarContext, AppState> for Work {
        async fn handle(
            &self,
            job: &Job,
            _outs: &(),
            ctx: &mut Context<'_, PulsarContext, AppState>,
        ) -> Result<(), HandlerOutcome> {
            tracing::info!(job.id, at = ?ctx.context(Position), "job done");
            if job.redo && ctx.state().redos.first_time(job) {
                let at = ctx.context(Position).clone();
                if ctx.context(SeekHandle).seek(at).await.is_err() {
                    return Err(HandlerOutcome::retry());
                }
            }
            Ok(())
        }
    }
    // --8<-- [end:context]

    fn app() -> impl App<State = AppState> {
        RustStream::new(AppInfo::new("jobs", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
            .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
                b.include(subscriber(PulsarSubscription::new("jobs", "workers"), Work).build());
            })
    }

    #[tokio::test]
    async fn a_job_whose_result_was_lost_is_done_again() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, redo) in [(1, false), (2, true), (3, false)] {
            tb.broker::<PulsarBroker>()
                .message(&Job { id, redo })
                .publish()
                .await?;
        }

        let done: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("jobs")
            .received::<Job>()
            .into_iter()
            .map(|job| job.id)
            .collect();
        assert_eq!(
            done,
            [1, 2, 2, 3],
            "job 2 came back once, from its own position"
        );
        Ok(())
    }
}

mod batch {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::Arc;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Job, Redos};

    /// What the handler shares across batches.
    #[derive(Clone, Default)]
    struct AppState {
        redos: Arc<Redos>,
    }

    // --8<-- [start:batch]
    /// A batch with a job whose result was lost settles first; the seek then replays the
    /// retained log, so the next batch opens at the beginning.
    struct Work;

    impl Handle<[Job], (), (), PulsarBatchContext, AppState> for Work {
        async fn handle(
            &self,
            jobs: &[Job],
            _outs: &(),
            ctx: &mut Context<'_, PulsarBatchContext, AppState>,
        ) -> Result<(), Vec<HandlerOutcome>> {
            tracing::info!(count = jobs.len(), "jobs done");
            let redos = &ctx.state().redos;
            let redo = jobs.iter().any(|job| job.redo && redos.first_time(job));
            if redo
                && ctx
                    .context(SeekHandle)
                    .seek(PulsarPosition::earliest())
                    .await
                    .is_err()
            {
                return Err(jobs.iter().map(|_| HandlerOutcome::retry()).collect());
            }
            Ok(())
        }
    }
    // --8<-- [end:batch]

    fn app() -> impl App<State = AppState> {
        RustStream::new(AppInfo::new("jobs", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(AppState::default()))
            .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
                b.include(
                    subscriber(PulsarSubscription::new("jobs", "bulk"), Work)
                        .batch(nonzero!(16))
                        .build(),
                );
            })
    }

    #[tokio::test]
    async fn a_batch_asking_for_a_redo_replays_the_log_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let jobs = [(1, false), (2, true), (3, false)].map(|(id, redo)| Job { id, redo });
        let broker = tb.broker::<PulsarBroker>();
        for published in join_all(jobs.iter().map(|job| broker.message(job).publish())).await {
            published?;
        }

        let done: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("jobs")
            .batches::<Job>()
            .into_iter()
            .flatten()
            .map(|job| job.id)
            .collect();
        assert_eq!(done, [1, 2, 3, 1, 2, 3], "the log ran again, once");
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
