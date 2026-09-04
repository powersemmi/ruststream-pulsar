//! Repositioning from a handler, at the level a service writes it.
//!
//! The subject here is the application: a body reads the subscription's seeker off its delivery
//! context (or, for a batch, off the subscription-scoped one) and moves the subscription. So the
//! test is a `TestApp` run against the in-process stand-in, which retains a log and therefore
//! backs both halves of the crate's seek surface - the `start_at(..)` clause that opens the
//! subscription at the beginning of it, and the `SeekHandle` key the body reads.
//!
//! That the stand-in's repositioning matches the framework's contract is `conformance_pulsar.rs`,
//! which runs `capabilities::seeking` against it; that a real consumer seeks is the live suite in
//! `integration_pulsar.rs`.
#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;
use serde::{Deserialize, Serialize};

/// The producer's contract: an entry marked poisoned asks the consumer to abandon the rest of
/// the backlog and resume at the tip of the log.
#[derive(Debug, PartialEq, Eq, Deserialize, Outgoing, Serialize)]
struct Job {
    id: u64,
    poisoned: bool,
}

// --8<-- [start:delivery]
/// Skips forward past a poisoned region: everything queued behind the marker is dropped and the
/// subscription resumes at the tip. The seeker is a broker context field, so the handler binds
/// it as a parameter and needs nothing from the include site.
#[subscriber("jobs")]
async fn skip_poison(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if job.poisoned && seeker.seek(PulsarPosition::latest()).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:delivery]

// --8<-- [start:batch]
/// The batch counterpart. A batch has no single position, so its context carries the seeker alone
/// and the body reads it through a declared `Context` parameter.
#[subscriber("jobs.bulk")]
async fn skip_poison_batch(
    batch: &[Job],
    ctx: &mut Context<'_, PulsarBatchContext>,
) -> HandlerOutcome {
    if batch.iter().any(|job| job.poisoned)
        && ctx
            .context(SeekHandle)
            .seek(PulsarPosition::latest())
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

fn job(id: u64) -> Job {
    Job {
        id,
        poisoned: false,
    }
}

fn poison(id: u64) -> Job {
    Job { id, poisoned: true }
}

/// Fills the log before the service exists, so `start_at` has a backlog to open on.
async fn backlog(broker: &PulsarTestBroker, address: &str) {
    let ingress = broker.publisher();
    for job in [job(1), poison(2), job(3), job(4)] {
        ingress
            .message(&job)
            .to(address)
            .publish()
            .await
            .expect("the stand-in publishes before connect");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_repositions_its_own_subscription() {
    let broker = PulsarTestBroker::new();
    backlog(&broker, "jobs").await;

    let app = RustStream::new(AppInfo::new("seek", "0.1.0")).with_broker(broker, |b| {
        b.include(skip_poison.start_at(PulsarPosition::earliest()));
    });
    let tb = TestApp::start(app).await.expect("start harness");
    tb.settle().await.expect("the opening replay settles");

    // The subscription opened at the beginning of the log and stopped at the marker: jobs 3 and
    // 4 were queued behind it, and the seek to the tip dropped them.
    tb.broker::<PulsarTestBroker>()
        .subscriber("jobs")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    assert_eq!(
        tb.broker::<PulsarTestBroker>()
            .subscriber("jobs")
            .received::<Job>(),
        vec![job(1), poison(2)],
        "the deliveries queued behind the marker must not reach the handler",
    );

    // The subscription is live at its new position, not stranded there.
    tb.broker::<PulsarTestBroker>()
        .message(&job(5))
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.broker::<PulsarTestBroker>()
        .subscriber("jobs")
        .assert_called(3)
        .with(&job(5))
        .settled(HandlerOutcome::ack());
}

/// What is specific to the batch path: the body names the subscription-scoped context, reads the
/// seeker off it, and repositions the subscription it came from without stranding it.
///
/// The mount site names one number, the batch size, and nothing there says Pulsar assembles its
/// batches on the client rather than pulling them off the wire. The seeker reaches through that
/// buffer, so a batch subscription opens on a backlog exactly as a single-delivery one does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_repositions_the_subscription_it_came_from() {
    let broker = PulsarTestBroker::new();
    backlog(&broker, "jobs.bulk").await;

    let app = RustStream::new(AppInfo::new("seek-batch", "0.1.0")).with_broker(broker, |b| {
        b.include(
            skip_poison_batch
                .batch(nonzero!(2))
                .start_at(PulsarPosition::earliest()),
        );
    });
    let tb = TestApp::start(app).await.expect("start harness");
    tb.settle().await.expect("the opening replay settles");

    // One batch, closed by the size rather than by the deadline, and it carried the marker: the
    // seek to the tip dropped jobs 3 and 4 before a second batch could form.
    tb.broker::<PulsarTestBroker>()
        .subscriber("jobs.bulk")
        .assert_called_once()
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());
    assert_eq!(
        tb.broker::<PulsarTestBroker>()
            .subscriber("jobs.bulk")
            .received::<Job>(),
        vec![job(1), poison(2)],
        "the deliveries queued behind the marker must not reach the handler",
    );

    // The batch's seek moved the live subscription rather than breaking it: the next entry still
    // arrives, at the repositioned tip, as a batch of its own.
    tb.broker::<PulsarTestBroker>()
        .message(&job(5))
        .to("jobs.bulk")
        .publish()
        .await
        .expect("publish");
    tb.broker::<PulsarTestBroker>()
        .subscriber("jobs.bulk")
        .assert_called(2)
        .assert_batch_sizes(&[2, 1])
        .settled(HandlerOutcome::ack());
}
