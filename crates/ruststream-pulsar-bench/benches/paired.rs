// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a broker fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `pulsar` client it wraps, and what the runtime costs on top.
//!
//! Every scenario runs three times over, as three loops that differ in what carries the messages:
//!
//! - **raw** - the client driven directly.
//! - **adapter** - this crate's own types: its broker, its subscription source, the [`Subscriber`]
//!   stream that source yields, its [`IncomingMessage`] and its ack, its [`Publisher`]. A loop
//!   here pulls from that stream, decodes, reads a field and settles. No handler, no app, no
//!   dispatch.
//! - **framework** - the service a user writes: `#[subscriber]`, the app, the runtime.
//!
//! Adapter against raw is what this crate's consumer and publisher cost over the client they wrap.
//! Framework against adapter is what the runtime costs on top, over this broker in particular.
//!
//! The procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! The consumer is attached first, a producer on a second connection then feeds it, and the window
//! runs from the first delivery to the end of the last settle. Connecting, subscribing and
//! creating the topic are startup cost and sit outside it. Every run owns a fresh topic and a
//! fresh subscription, and drops the topic afterwards, so a run never sees what the one before it
//! left behind and the stand does not grow through the afternoon.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! Rounds are interleaved - raw, adapter, framework, raw, adapter, framework - and the first is
//! discarded. Running one loop to the end and then the next would charge every drift of the
//! machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! The window ends where the last settle is issued rather than where the server records it, on
//! all three loops alike: Pulsar acknowledges by writing a command to the socket and waits for no
//! answer, so none of them can observe the server's side of it.
//!
//! Whether a row is one the transport paced is decided by arithmetic, not by a guess: a probe
//! outside the rounds times a ping-pong against the live broker, and the row is flagged when the
//! round trips a delivery costs already account for half of what a delivery took.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::io::{Read as _, Write as _};
use std::iter::repeat_n;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use pulsar::producer::SendFuture;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use ruststream::runtime::RunningApp;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource,
};
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::{ConnectedPulsarBroker, PulsarPublisher};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships, and the framework's harness feature changes the dispatch path:
// every delivery records what the handler saw and every handler call runs inside a task-local
// scope. Nothing here enables it; this exists so that asking for it is a compile error rather than
// a number nobody can trust. The benchmark lives in a package of its own for the same reason:
// `ruststream-pulsar`'s dev-dependencies enable that feature through the conformance harness, and
// a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw loop's rate.
const PROBE_MESSAGES: usize = 50_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count.
///
/// Pulsar writes every message to its ledger, so the count is also how much the stand has to
/// store and then reclaim; this bounds both the run and the disk under it.
const MAX_MESSAGES: usize = 2_000_000;
/// Rounds kept. One more is run and discarded.
const ROUNDS: usize = 11;
/// Worker threads every loop is driven on.
const WORKERS: usize = 4;

/// Requests the round-trip probe times, after as many again thrown away.
const PROBE_ROUND_TRIPS: u32 = 20_000;
/// Round trips a delivery costs the client, which is what decides `broker_bound`.
///
/// Pulsar's consumer pays none it waits for: it grants its receive permits in advance, refreshes
/// them once per half a receiver queue, and settles by writing one command to the socket with no
/// answer awaited. What a delivery does cost is the publish behind it, which the client holds open
/// until the broker has persisted the message and receipted it - one round trip per delivery,
/// pipelined across the feed's window. That receipt is the transport cost a delivery carries here,
/// so it is the one the flag is computed from.
const ROUND_TRIPS_PER_DELIVERY: f64 = 1.0;

/// How far the producer may run ahead of the consumer, in messages.
///
/// The backlog is the stand's memory and disk, and a run that let the producer finish first would
/// measure a drain rather than a delivery. Every loop is held to the same window.
const IN_FLIGHT: usize = 20_000;
/// How often the producer checks that ceiling.
const CHECK_EVERY: usize = 256;
/// How many publishes stay outstanding before the oldest is awaited.
///
/// Every loop awaits the broker's receipt for the messages it feeds, and every loop keeps this
/// many in flight while it does: awaiting each one in turn would make the feed a round trip per
/// message. The ceiling is the client's own outbound queue, a hundred frames per connection, which
/// refuses a send that would overflow it rather than waiting; at a round trip of tens of
/// microseconds this many in flight is already far more than either consumer can take.
const SEND_WINDOW: usize = 64;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(60);

/// The body size every loop publishes and decodes, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
///
/// Smaller than the 512 bytes the framework's other benchmarks use, and for the stand's sake: the
/// compose file runs Pulsar standalone with a direct-memory ceiling sized for the test suite, the
/// broker and its bookie buffer every message in that pool, and a run at this rate exhausts it and
/// takes the JVM down with it. Pulsar's throughput here is bound by entries rather than by bytes,
/// so a quarter of the body leaves the rates where they were and the stand standing.
const BODY_BYTES: usize = 128;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The HTTP port the stand serves its admin API on, beside the service port.
const ADMIN_PORT: u16 = 8080;
/// The namespace a bare topic name lives in, which is where a run creates its topic.
const NAMESPACE: &str = "public/default";

/// What every loop decodes a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// A name nothing else in this process or on the stand holds.
fn stamped(prefix: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{stamp}")
}

/// The names one run owns, and the subscription type it opens them with: nothing is shared with
/// the run before it.
#[derive(Clone, Debug)]
struct Names {
    topic: String,
    subscription: String,
    sharing: SubscriptionType,
}

impl Names {
    fn fresh(sharing: SubscriptionType) -> Self {
        Self {
            topic: stamped("ruststream-bench"),
            subscription: stamped("rs-bench"),
            sharing,
        }
    }

    /// The descriptor the adapter loop and the service both mount.
    fn descriptor(&self) -> PulsarSubscription {
        PulsarSubscription::new(&self.topic, &self.subscription).subscription_type(self.sharing)
    }

    /// The client's own spelling of the subscription type, which is what the raw loop asks the
    /// server for. This crate maps the same four the same way.
    const fn sub_type(&self) -> SubType {
        match self.sharing {
            SubscriptionType::Exclusive => SubType::Exclusive,
            SubscriptionType::Shared => SubType::Shared,
            SubscriptionType::Failover => SubType::Failover,
            SubscriptionType::KeyShared => SubType::KeyShared,
        }
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the framework opens is the one this run publishes to.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn install(names: &Names) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Every loop calls the same methods, so every loop pays for the signal. A delivery pays one
/// relaxed increment and two comparisons; the waiter is a single future for the whole run, woken
/// once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last settle.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, carrier: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{carrier}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What one loop of one round produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

/// The client's own builders return futures of some twenty kilobytes, so every one of them is
/// boxed where it is awaited. All of them are startup work, outside the window a run measures.
async fn connect(url: &str) -> Pulsar<TokioExecutor> {
    Box::pin(Pulsar::builder(url, TokioExecutor).build())
        .await
        .expect("the Pulsar broker accepts a connection")
}

async fn connect_broker(url: &str) -> ConnectedPulsarBroker {
    Box::pin(PulsarBroker::new(url).connect())
        .await
        .expect("the broker connects")
}

/// How far the feed has run ahead of the consumer, and what it waits for when that is too far.
async fn throttle(sent: usize, run: &Run) {
    if sent.is_multiple_of(CHECK_EVERY) {
        while sent.saturating_sub(run.handled()) > IN_FLIGHT {
            sleep(Duration::from_micros(200)).await;
        }
    }
}

/// Feeds the run through the client's own producer, which is what the raw loop consumes from.
///
/// The producer is left at the client's defaults, which is what this crate's publisher builds its
/// own with apart from a routing policy an unpartitioned topic never consults.
async fn publish_raw(client: &Pulsar<TokioExecutor>, topic: &str, messages: usize, run: &Run) {
    let mut producer = Box::pin(client.producer().with_topic(topic).build())
        .await
        .expect("the broker accepts the producer");
    let body = json_body(BODY_BYTES);
    let mut pending: VecDeque<SendFuture> = VecDeque::with_capacity(SEND_WINDOW);
    for sent in 0..messages {
        throttle(sent, run).await;
        // Not boxed, unlike every other call into the client: this one is per message, and the
        // heap allocation the lint asks for would cost the feed more than the stack it saves.
        #[allow(clippy::large_futures)]
        let receipt = producer
            .send_non_blocking(body.clone())
            .await
            .expect("the client accepts the publish");
        pending.push_back(receipt);
        if pending.len() >= SEND_WINDOW {
            pending
                .pop_front()
                .expect("the window is full")
                .await
                .expect("the broker receipts the publish");
        }
    }
    for receipt in pending {
        receipt.await.expect("the broker receipts the publish");
    }
}

/// Feeds the run through this crate's publisher, which is what the adapter loop and the service
/// consume from.
///
/// A publish resolves once the broker has receipted it, so the window keeps as many messages in
/// flight here as the raw feed keeps outstanding receipts.
async fn publish_through_crate(
    publisher: &PulsarPublisher,
    topic: &str,
    messages: usize,
    run: &Run,
) {
    let body = json_body(BODY_BYTES);
    let mut pending = FuturesUnordered::new();
    for sent in 0..messages {
        throttle(sent, run).await;
        pending.push(publisher.publish(OutgoingMessage::new(topic, &body), None));
        if pending.len() >= SEND_WINDOW {
            pending
                .next()
                .await
                .expect("the window is full")
                .expect("the broker receipts the publish");
        }
    }
    while let Some(done) = pending.next().await {
        done.expect("the broker receipts the publish");
    }
}

// ---------------------------------------------------------------------------------------------
// raw
// ---------------------------------------------------------------------------------------------

async fn raw(url: &str, names: &Names, messages: usize) -> Sample {
    let client = connect(url).await;
    // The consumer this crate opens for a descriptor with nothing else set on it: the
    // subscription, its type, and the topic as a one-element list.
    let mut consumer: Consumer<Vec<u8>, TokioExecutor> = Box::pin(
        client
            .consumer()
            .with_subscription(&names.subscription)
            .with_subscription_type(names.sub_type())
            .with_topics([&names.topic])
            .build(),
    )
    .await
    .expect("the server accepts the subscription");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            loop {
                let message = consumer
                    .next()
                    .await
                    .expect("the consumer stream keeps running")
                    .expect("the consumer delivers");
                let order: Order =
                    serde_json::from_slice(&message.payload.data).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                consumer.ack(&message).await.expect("the ack is written");
                if done {
                    break;
                }
            }
        }
    });

    let feed = connect(url).await;
    Box::pin(publish_raw(&feed, &names.topic, messages, &run)).await;
    drain(&run, "raw").await;
    consuming.await.expect("the consuming task ends");
    Sample {
        window: run.window(),
    }
}

// ---------------------------------------------------------------------------------------------
// adapter
// ---------------------------------------------------------------------------------------------

async fn adapter(url: &str, names: &Names, messages: usize) -> Sample {
    let broker = connect_broker(url).await;
    let mut subscriber = Box::pin(names.descriptor().subscribe(&broker))
        .await
        .expect("the server accepts the subscription");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut stream = pin!(subscriber.stream());
            loop {
                let message = stream
                    .next()
                    .await
                    .expect("the subscriber stream keeps running")
                    .expect("the subscriber delivers");
                let order: Order =
                    serde_json::from_slice(message.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                message.ack().await.expect("the ack is written");
                if done {
                    break;
                }
            }
        }
    });

    let feed = connect_broker(url).await;
    Box::pin(publish_through_crate(
        &feed.publisher(),
        &names.topic,
        messages,
        &run,
    ))
    .await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    let sample = Sample {
        window: run.window(),
    };
    feed.shutdown().await.expect("the feed shuts down");
    broker.shutdown().await.expect("the broker shuts down");
    sample
}

// ---------------------------------------------------------------------------------------------
// framework
// ---------------------------------------------------------------------------------------------

// The attribute takes a constructor path, so the descriptor is spelled out here rather than
// called through `Names::descriptor`; the two build the same thing.
#[subscriber(
    PulsarSubscription::new(installed().topic, installed().subscription)
        .subscription_type(installed().sharing)
)]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("pulsar-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(PulsarBroker::new(url), |b| {
            b.include(consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn framework(url: &str, names: &Names, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(names);
    let app = Box::pin(start(url, run.clone())).await;

    let feed = connect_broker(url).await;
    Box::pin(publish_through_crate(
        &feed.publisher(),
        &names.topic,
        messages,
        &run,
    ))
    .await;
    drain(&run, "framework").await;
    let sample = Sample {
        window: run.window(),
    };
    app.shutdown().await.expect("the service stops");
    feed.shutdown().await.expect("the feed shuts down");
    sample
}

// ---------------------------------------------------------------------------------------------
// The stand
// ---------------------------------------------------------------------------------------------

/// The admin address of the stand `url` names.
fn admin_address(url: &str) -> String {
    let rest = url
        .split_once("://")
        .map_or(url, |(_, rest)| rest)
        .split(',')
        .next()
        .unwrap_or_default();
    let host = rest.rsplit_once(':').map_or(rest, |(host, _)| host);
    format!("{host}:{ADMIN_PORT}")
}

/// Drops the topic a finished run owned, with its ledgers and its subscription.
///
/// Best effort, and deliberately so: the numbers are already taken when this runs, and a stand
/// that refuses the request costs disk rather than correctness. It speaks HTTP over a socket
/// because the benchmark measures a client and has no business growing a second one.
fn drop_topic(url: &str, topic: &str) {
    let address = admin_address(url);
    let Ok(socket) = address.parse() else { return };
    let Ok(mut stream) = TcpStream::connect_timeout(&socket, Duration::from_secs(10)) else {
        return;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let request = format!(
        "DELETE /admin/v2/persistent/{NAMESPACE}/{topic}?force=true HTTP/1.1\r\nHost: \
         {address}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return;
    }
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
}

/// One client-to-broker round trip, timed outside the rounds.
///
/// A topic lookup is the cheapest request this client makes whose answer it waits for, and the
/// broker answers it from the ownership it already holds, so what this measures is the transport
/// and not the broker's storage. A ping-pong would be cheaper still and is not usable: the client
/// keeps one resolver slot per connection for it and its own keep-alive writes to that slot every
/// minute, so a flood of pings races the library rather than the server. The published rows are
/// read against this number: a row whose delivery cost is already round trips is a row the
/// transport paced.
async fn round_trip(url: &str) -> Duration {
    let client = connect(url).await;
    let topic = stamped("ruststream-bench-lookup");
    // A lookup answers for a topic that exists, so the probe opens one and drops it again.
    let producer = Box::pin(client.producer().with_topic(&topic).build())
        .await
        .expect("the broker accepts the producer");
    drop(producer);

    // Boxed like every other call into the client, warm-up and timed loop alike: the allocation
    // is tens of nanoseconds against a round trip of tens of microseconds, and it is paid on both.
    for _ in 0..PROBE_ROUND_TRIPS {
        Box::pin(client.lookup_topic(&topic))
            .await
            .expect("the broker answers");
    }
    let start = Instant::now();
    for _ in 0..PROBE_ROUND_TRIPS {
        Box::pin(client.lookup_topic(&topic))
            .await
            .expect("the broker answers");
    }
    let elapsed = start.elapsed();

    let url = url.to_owned();
    tokio::task::spawn_blocking(move || drop_topic(&url, &topic))
        .await
        .expect("the cleanup request runs to completion");
    elapsed / PROBE_ROUND_TRIPS
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Exclusive,
    Shared,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::Exclusive => "Exclusive subscription, 128 B JSON, ack each",
            Self::Shared => "Shared subscription, 128 B JSON, ack each",
        }
    }

    const fn sharing(self) -> SubscriptionType {
        match self {
            Self::Exclusive => SubscriptionType::Exclusive,
            Self::Shared => SubscriptionType::Shared,
        }
    }

    fn names(self) -> Names {
        Names::fresh(self.sharing())
    }
}

#[derive(Clone, Copy, Debug)]
enum Carrier {
    Raw,
    Adapter,
    Framework,
}

/// One loop of one round, over a topic of its own, which is dropped once the sample is taken.
async fn once(carrier: Carrier, url: &str, names: &Names, messages: usize) -> Sample {
    let sample = match carrier {
        Carrier::Raw => raw(url, names, messages).await,
        Carrier::Adapter => adapter(url, names, messages).await,
        Carrier::Framework => framework(url, names, messages).await,
    };
    let (url, topic) = (url.to_owned(), names.topic.clone());
    tokio::task::spawn_blocking(move || drop_topic(&url, &topic))
        .await
        .expect("the cleanup request runs to completion");
    sample
}

/// Median, smallest and largest of the kept rounds.
#[derive(Clone, Copy, Debug)]
struct Stats {
    median: f64,
    min: f64,
    max: f64,
}

impl Stats {
    fn of(mut rates: Vec<f64>) -> Self {
        rates.sort_by(f64::total_cmp);
        let middle = rates.len() / 2;
        let median = if rates.len().is_multiple_of(2) {
            f64::midpoint(rates[middle - 1], rates[middle])
        } else {
            rates[middle]
        };
        Self {
            median,
            min: rates[0],
            max: rates[rates.len() - 1],
        }
    }

    const fn spread(self) -> f64 {
        self.max - self.min
    }
}

/// A difference smaller than the run-to-run spread of either side is a verdict, not a percentage.
const fn verdict(baseline: Stats, other: Stats) -> &'static str {
    if (baseline.median - other.median).abs() < baseline.spread().max(other.spread()) {
        "indistinguishable"
    } else {
        "measured"
    }
}

/// What `other` costs against `baseline`, as a percentage of the baseline.
const fn percent(baseline: Stats, other: Stats) -> f64 {
    (baseline.median - other.median) / baseline.median * 100.0
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    broker_bound: bool,
}

async fn measure(
    scenario: Scenario,
    url: &str,
    rounds: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = Box::pin(once(Carrier::Raw, url, &scenario.names(), PROBE_MESSAGES)).await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES)
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    for round in 0..=rounds {
        let raw = Box::pin(once(Carrier::Raw, url, &scenario.names(), messages)).await;
        let adapter = Box::pin(once(Carrier::Adapter, url, &scenario.names(), messages)).await;
        let framework = Box::pin(once(Carrier::Framework, url, &scenario.names(), messages)).await;
        if round == 0 {
            continue;
        }
        println!(
            "  round {round:>2}: raw {:>9.0}, adapter {:>9.0}, framework {:>9.0} msg/s",
            raw.rate(messages),
            adapter.rate(messages),
            framework.rate(messages)
        );
        raws.push(raw.rate(messages));
        adapters.push(adapter.rate(messages));
        frameworks.push(framework.rate(messages));
    }

    let raw = Stats::of(raws);
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter: Stats::of(adapters),
        framework: Stats::of(frameworks),
        // The transport's share of a delivery, against the delivery itself: a row whose round
        // trips already account for half of what a message took is a row the transport paced.
        broker_bound: ROUND_TRIPS_PER_DELIVERY * round_trip.as_secs_f64() >= 0.5 / raw.median,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    let mut out = format!(
        "{{\n  \"round_trip_micros\": {:.1},\n  \"scenarios\": [\n",
        round_trip.as_secs_f64() * 1e6
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"median\": {raw_median:.0}, \"min\": {raw_min:.0},",
                " \"max\": {raw_max:.0} }},\n",
                "      \"adapter\": {{ \"median\": {ad_median:.0}, \"min\": {ad_min:.0},",
                " \"max\": {ad_max:.0} }},\n",
                "      \"framework\": {{ \"median\": {fw_median:.0}, \"min\": {fw_min:.0},",
                " \"max\": {fw_max:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_median = row.raw.median,
            raw_min = row.raw.min,
            raw_max = row.raw.max,
            ad_median = row.adapter.median,
            ad_min = row.adapter.min,
            ad_max = row.adapter.max,
            fw_median = row.framework.median,
            fw_min = row.framework.min,
            fw_max = row.framework.max,
            adapter_overhead = percent(row.raw, row.adapter),
            adapter_verdict = verdict(row.raw, row.adapter),
            overhead = percent(row.raw, row.framework),
            verdict = verdict(row.raw, row.framework),
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a round count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with every kept round discarded.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let url = env::var("PULSAR_TEST_URL")
        .expect("PULSAR_TEST_URL names the broker to measure against; `just bench` sets it");
    let rounds = number("RUSTSTREAM_BENCH_ROUNDS", ROUNDS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    let round_trip = runtime.block_on(Box::pin(round_trip(&url)));
    println!("round trip: {:.1} us", round_trip.as_secs_f64() * 1e6);

    let measured: Vec<Measured> = [Scenario::Exclusive, Scenario::Shared]
        .into_iter()
        .map(|scenario| {
            runtime.block_on(Box::pin(measure(
                scenario, &url, rounds, seconds, round_trip,
            )))
        })
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%, {}), framework {:.0} ({:+.1}%, {}){}",
            row.scenario.name(),
            row.raw.median,
            row.adapter.median,
            percent(row.raw, row.adapter),
            verdict(row.raw, row.adapter),
            row.framework.median,
            percent(row.raw, row.framework),
            verdict(row.raw, row.framework),
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, round_trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
