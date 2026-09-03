//! The keyed seek contract: what the compiler pins, and what a live broker confirms.
//!
//! Repositioning is reached through the subscription's context keys, so half of the contract is
//! a typing property: a per-delivery body binds [`Position`] and [`SeekHandle`] off
//! `PulsarContext`, and a page body - Pulsar batches on the client, so through the framework's
//! own buffer - binds [`SeekHandle`] off `PulsarBatchContext` and nothing per-delivery.
//! Assembling the app proves it, since every one of those bounds is resolved at the mount site.
//!
//! The other half is that the handle a delivery carries really moves the subscription it came
//! from, which only a real broker can answer; that test is gated on `PULSAR_TEST_URL`, like the
//! rest of the live suite.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream::{ConnectedBroker, OutgoingMessage};
use ruststream_pulsar::prelude::*;
use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
    resume_at: Option<u64>,
}

// --8<-- [start:delivery]
/// Reads both keys as parameters: the delivery's position and the subscription's seeker.
#[subscriber(PulsarSubscription::new("jobs", "workers"))]
async fn skip_poison(
    job: &Job,
    Ctx(at): Ctx<Position>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    if let Some(resume_at) = job.resume_at {
        if seeker
            .seek(PulsarPosition::timestamp(resume_at))
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    } else {
        println!("job {} sits at {at:?}", job.id);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:delivery]

// --8<-- [start:page]
/// The page counterpart: the target rides the elements, the handle comes off the page context.
#[subscriber(PulsarSubscription::new("jobs.bulk", "workers"))]
async fn replay(page: &[Job], ctx: &mut Context<'_, PulsarBatchContext>) -> HandlerOutcome {
    let target = page.iter().find_map(|job| job.resume_at);
    if let Some(resume_at) = target
        && ctx
            .context(SeekHandle)
            .seek(PulsarPosition::timestamp(resume_at))
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:page]

#[test]
fn seeking_bodies_mount_on_a_pulsar_broker() {
    let app = RustStream::new(AppInfo::new("seek", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://127.0.0.1:6650"),
        |b| {
            b.include(skip_poison);
            b.include(replay.buffered(nonzero!(8), Duration::from_millis(10)));
        },
    );
    // Assembling is the assertion; nothing here dials a broker.
    let _ = app;
}

/// The marker payload the live handler rewinds on.
const REWIND: &[u8] = b"rewind";

/// Every payload the live handler saw, so the test can watch for the replay without polling.
/// Installed before the app is assembled; the handler is mounted only by the live test.
static SEEN: OnceLock<mpsc::UnboundedSender<Vec<u8>>> = OnceLock::new();

/// One rewind per run: the replay hands the marker back, and an ungated handler would seek
/// forever.
static REWOUND: AtomicBool = AtomicBool::new(false);

/// The live topic and subscription, named per process so reruns do not inherit a cursor or a
/// backlog. A subscription is durable broker-side state, so a fixed name would carry every
/// previous run's messages into this one.
fn live_topic() -> String {
    format!("it-ctxseek-{}", std::process::id())
}

fn live_subscription() -> String {
    format!("ctxseek-{}", std::process::id())
}

/// The payloads are not one model here, so they ride the byte lane.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

#[subscriber(PulsarSubscription::new(live_topic(), live_subscription()))]
async fn rewind_once(frame: &Frame<'_>, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    let _ = SEEN
        .get()
        .expect("the live test installs the sink before mounting")
        .send(frame.0.to_vec());
    if frame.0 == REWIND
        && !REWOUND.swap(true, Ordering::SeqCst)
        && seeker.seek(PulsarPosition::earliest()).await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn test_url() -> Option<String> {
    match std::env::var("PULSAR_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!("PULSAR_TEST_URL is not set; skipping the live seek-context test");
            None
        }
    }
}

/// The handle a delivery carries repositions the subscription that delivered it: the handler
/// rewinds on the marker, and the payload published before it is delivered a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_repositions_its_own_subscription() {
    let Some(url) = test_url() else { return };
    let topic = live_topic();
    let subscription = live_subscription();

    let connected = PulsarBroker::new(&url)
        .connect()
        .await
        .expect("broker connects");
    // The subscription has to exist before the publishes: without one the standalone broker
    // retains nothing, and there would be no backlog for the rewind to replay. Dropping the
    // subscriber closes the consumer; the subscription itself is durable broker-side state.
    drop(
        connected
            .subscribe_descriptor(PulsarSubscription::new(&topic, &subscription))
            .await
            .expect("subscription opens"),
    );
    let publisher = connected.publisher();
    for payload in [b"first".as_slice(), REWIND] {
        publisher
            .publish(OutgoingMessage::new(&topic, payload))
            .await
            .expect("publish succeeds");
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    SEEN.set(tx).expect("one live run per process");

    let running = RustStream::new(AppInfo::new("ctx-seek", "0.1.0"))
        .with_broker(PulsarBroker::new(&url), |b| {
            b.include(rewind_once);
        })
        .start()
        .await
        .expect("the app starts");

    let mut replays = 0;
    while replays < 2 {
        let payload = tokio::time::timeout(Duration::from_secs(60), rx.recv())
            .await
            .expect("the handler keeps receiving")
            .expect("the sink stays open");
        if payload == b"first" {
            replays += 1;
        }
    }

    running.shutdown().await.expect("shutdown succeeds");
    connected.shutdown().await.expect("shutdown succeeds");
}
