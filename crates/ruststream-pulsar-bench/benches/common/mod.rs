//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the fill, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, started through [`RustStream::start`], on
//! [`PulsarBroker`], this crate's production broker, connected to the stand the tests use
//! (`PULSAR_TEST_URL`, which `just bench-code` sets). The subscription is the crate's descriptor
//! with its defaults - a shared subscription - over a persistent topic of the run's own in the
//! default namespace, so the broker keeps what the fill publishes until the service takes it, and
//! a run never sees a message an earlier one left behind.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: the connect, the subscription and the first delivery.
//!
//! What a body measures is the start and the drain, in two regions, with the fill between them
//! and never counted: producing the messages is not what the scenario is about. The fill runs on a
//! thread of its own, with its own runtime and its own connection, through the crate's publisher,
//! which returns once the broker has stored a message. The service's runtime runs only inside
//! the regions, so every message is on the broker before the drain starts and none is taken while
//! the fill runs.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything on the service's thread inside the region is counted: the dispatcher, the
//! codec, this crate's code, and the `pulsar` client's work on that thread - its connection and
//! consumer tasks run on the service's runtime. Threads the client runs on its own are not
//! counted, and neither is the broker, which is another process. [`measure`] is the only frame
//! that carries its name, because a toggle on a name that also appears inside closure types
//! switches collection off again one frame deeper. DHAT is pointed at the same frame; the number
//! read is `Total blocks`, allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::fmt::Debug;
use std::hint::black_box;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream::{Broker, BytesMut, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_pulsar::PulsarBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

// A benchmark measures what ships, and the framework's harness feature changes the dispatch path:
// every delivery records what the handler saw. Nothing here enables it; this exists so that asking
// for it is a compile error rather than a number nobody can trust.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The variable that names the broker to measure against.
const URL: &str = "PULSAR_TEST_URL";

/// The subscription every scenario joins on [`topic`].
pub const SUBSCRIPTION: &str = "bench";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within a minute of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// Publishes the fill keeps in flight at once, so the stand is not waited on one receipt at a
/// time. The ceiling is the client's own outbound queue, a hundred frames per connection, which
/// refuses a send that would overflow it rather than waiting.
const FILL_WINDOW: usize = 64;

/// The topic of this run, one per process: a short name the broker resolves in the default
/// namespace, created on first use.
///
/// A handler names it in the descriptor of its own `#[subscriber(..)]` attribute, which takes a
/// constructor call.
pub fn topic() -> &'static str {
    static TOPIC: OnceLock<String> = OnceLock::new();
    TOPIC.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        format!("orders-{}-{nanos}", process::id())
    })
}

/// The broker the stand serves.
///
/// # Panics
///
/// When the variable is not set: a code-cost run needs the stand `just bench-code` starts.
fn url() -> String {
    env::var(URL).unwrap_or_else(|_| {
        panic!("{URL} names the broker to measure against; `just bench-code` sets it")
    })
}

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what a run allocates
/// once on top of that: the start, the first delivery, and what the client sets up once traffic
/// flows. Together they are the hard limit the longest run of the scenario (twice [`MESSAGES`]
/// deliveries) is held to, so the run fails when the path allocates more than it does today.
/// Both are floors the code is held to, taken as the highest count of three runs, so a number
/// that goes down is lowered here in the same change. The instruction limit is relative:
/// `just bench-code --save-baseline=main` records a baseline and
/// `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations are stated per `per` deliveries rather than per
/// delivery: a rate that is not a whole number per message, or a batch handler that allocates
/// per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        // The runner clears the environment; the stand's address is the one variable a run needs.
        .pass_through_env(URL)
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// Headroom over the floor, in tenths of a percent, and never less than one block.
///
/// A socket is in the loop, so how the deliveries arrive moves an allocation count by a block or
/// two between runs of one binary, and a limit at the floor itself would fail an unchanged tree
/// now and then. A tenth of a percent is above that movement and far below one allocation per
/// message, which the longest run multiplies by twice [`MESSAGES`].
const MARGIN_PER_MILLE: u64 = 1;

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`], plus the margin. The divisions round up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    let floor = cold + (steady * 2 * MESSAGES as u64).div_ceil(per);
    let margin = (floor * MARGIN_PER_MILLE).div_ceil(1_000);
    floor + if margin == 0 { 1 } else { margin }
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution, and the client's tasks on
/// the thread the service runs on.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started, and how many messages its run takes.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<PulsarBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, ready to be started by the body.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(PulsarBroker::new(url()), mount);
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Publishes `count` bodies on this run's topic and returns once the broker has stored them all.
///
/// Part of every setup, never of a measured region: it runs on a thread of its own, with a runtime
/// and a connection of its own, while the service's runtime is not running, so the deliveries are
/// on the broker before the body drains them and the service takes none of them early.
fn fill(count: usize) {
    let url = url();
    let topic = topic();
    thread::spawn(move || {
        runtime().block_on(async move {
            let connected = PulsarBroker::new(url)
                .connect()
                .await
                .expect("the fill connects to the stand");
            let publisher = connected.publisher();
            let body = json_body();
            let mut in_flight = FuturesUnordered::new();
            for _ in 0..count {
                if in_flight.len() == FILL_WINDOW {
                    receipt(in_flight.next().await);
                }
                let payload = BytesMut::from(body.as_slice());
                in_flight.push(publisher.publish(OutgoingMessage::produced(topic, payload), None));
            }
            while let Some(result) = in_flight.next().await {
                receipt(Some(result));
            }
            drop(in_flight);
            connected.shutdown().await.expect("the fill disconnects");
        });
    })
    .join()
    .expect("the fill runs to completion");
}

/// A publish of the fill that came back: the broker stored the message, or the run is void.
fn receipt<Error: Debug>(result: Option<Result<(), Error>>) {
    result
        .expect("a publish in flight")
        .expect("the broker stores the fill");
}

/// Starts the service, fills its topic, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries. The service stops outside both.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the topic was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    runtime
        .block_on(running.shutdown())
        .expect("the service stops");
    black_box(&latch);
}
