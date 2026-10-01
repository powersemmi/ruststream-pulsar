//! Conformance: every suite this crate's capabilities justify, run over the same production
//! broker in process and against a server.
//!
//! The in-process legs connect `PulsarBroker` the way the test harness does, through
//! `harness::InProcessBroker` for the suites that call `connect`, and hold that transport to the
//! contract the server is held to - routing, lifecycle, retries, message shape, seeking, batches -
//! because a service's tests run on it. The server legs, gated behind `PULSAR_TEST_URL`, are what
//! says the in-process mode is not merely agreeing with itself, and the suites that compare the
//! two transports (settlements, the backlog, refusals) run there.
//!
//! Start a broker with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features`.

#![cfg(feature = "testing")]

mod live;

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::message_shape::OptionCases;
use ruststream::conformance::{capabilities, harness, lifecycle, message_shape, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{HeaderMap, IncomingMessage, Name, nonzero};
use ruststream_pulsar::{
    OperationRetries, PulsarBroker, PulsarMessage, PulsarPublish, PulsarPublishOptions,
    PulsarSubscription, SubscriptionType,
};

use crate::live::{test_url, unique};

/// The address the service's broker is built with; the in-process legs dial nothing.
const URL: &str = "pulsar://localhost:6650";

/// The largest payload a server accepts unless its deployment says otherwise, and the broker's
/// default limit.
const MAX_MESSAGE_SIZE: usize = 5 * 1024 * 1024;

/// How many deliveries `broker_moves` requeues before the client moves the message.
const ATTEMPTS: NonZeroU32 = nonzero!(3u32);

/// The production broker, as a service configures it.
fn production(url: &str) -> PulsarBroker {
    PulsarBroker::new(url).default_subscription(unique("conformance"))
}

/// The production broker, connected in process by the suites that call `connect`.
fn in_process() -> InProcessBroker<PulsarBroker> {
    InProcessBroker::new(production(URL))
}

/// The descriptor the suites subscribe through: one durable subscription per test and run, so a
/// rerun against one server does not inherit a backlog.
fn descriptor(subscription: &str) -> impl Fn(&str) -> PulsarSubscription + Clone + 'static {
    let subscription = unique(subscription);
    move |topic| PulsarSubscription::new(topic, subscription.clone())
}

/// How a keyed publish carries its key on this broker: the per-message option.
#[allow(clippy::unnecessary_wraps)]
fn keyed(key: &[u8], _headers: &mut HeaderMap) -> Option<PulsarPublishOptions> {
    Some(PulsarPublishOptions {
        partition_key: Some(String::from_utf8_lossy(key).into_owned()),
    })
}

/// The partition key cases: the policy names none, a call names one, and the next call is
/// unkeyed again.
fn key_cases() -> OptionCases<PulsarPublishOptions, Option<Vec<u8>>> {
    OptionCases::new(None).overrides(
        PulsarPublishOptions {
            partition_key: Some("user-42".to_owned()),
        },
        Some(b"user-42".to_vec()),
    )
}

/// What a delivery shows of the partition key it was published with.
fn observed_key(delivery: &PulsarMessage) -> Option<Vec<u8>> {
    delivery.partition_key().map(<[u8]>::to_vec)
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(|| production(URL)).await;
}

/// The lifecycle ladder in process, through the crate's own descriptor. The live leg below runs
/// the same suite, which is what says the two agree.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_lifecycle() {
    harness::lifecycle(in_process, descriptor("lifecycle"), |connected| {
        connected.publisher()
    })
    .await;
}

/// The same ladder over the bare-name form, which resolves through `Subscribe` and the broker's
/// default subscription rather than through the descriptor.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_lifecycle_by_name() {
    harness::lifecycle(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The client moves a spent delivery itself, so the cap and the dead-letter topic a registration
/// declares are applied in process too, on both addressing forms.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_moves_spent_deliveries() {
    retry::broker_moves(
        in_process,
        descriptor("moves"),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
    retry::broker_moves(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_keeps_key_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        descriptor("keyed"),
        |connected| connected.publisher(),
        keyed,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_resolves_publish_options() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance.options"),
        descriptor("options"),
        PulsarPublish,
        key_cases(),
        observed_key,
    )
    .await;
}

/// The in-process transport retains a log, so the framework's own seeking suite is what says its
/// repositioning matches the contract - the same suite the live broker runs below.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_seeking_suite() {
    capabilities::seeking(in_process, descriptor("seeking"), |connected| {
        connected.publisher()
    })
    .await;
}

/// Pulsar's client hands over one delivery at a time, so this crate's batches are assembled on
/// the client. The suite is what says the assembly honours the contract.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_batches_suite() {
    capabilities::batches(in_process, descriptor("batches"), |connected| {
        connected.publisher()
    })
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_batch_seeking_suite() {
    capabilities::batch_seeking(in_process, descriptor("batch-seeking"), |connected| {
        connected.publisher()
    })
    .await;
}

/// A service's URL may carry credentials, and the generated document is published and shared.
#[cfg(feature = "asyncapi")]
#[test]
fn the_document_carries_no_credentials() {
    harness::describes_without_credentials(
        &PulsarBroker::new("pulsar://admin:hunter2@broker:6650"),
        &PulsarSubscription::new("orders", "orders-worker"),
        "hunter2",
    );
    message_shape::publishes_without_credentials::<ruststream_pulsar::ConnectedPulsarBroker, _>(
        &PulsarPublish,
        "hunter2",
    );
    message_shape::describes_addresses_without_credentials(
        |addrs| {
            // The client takes one URL naming several hosts: the first address with its scheme,
            // then the others after a comma.
            let rest = addrs
                .iter()
                .skip(1)
                .map(|addr| addr.split_once("://").map_or(*addr, |(_, rest)| rest));
            let url: Vec<&str> = addrs.iter().take(1).copied().chain(rest).collect();
            PulsarBroker::new(url.join(","))
        },
        "pulsar",
    );
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_lifecycle() {
    let Some(url) = test_url() else { return };
    Box::pin(harness::lifecycle(
        move || production(&url),
        descriptor("lifecycle"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_lifecycle_by_name() {
    let Some(url) = test_url() else { return };
    Box::pin(harness::lifecycle(
        move || production(&url),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    ))
    .await;
}

/// A shutdown finishes the acknowledgement and the publish handed to it. A subscription the
/// server has not seen starts at the tip, so the observer is a subscription of its own, open
/// before the publish: each call names a new one.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_flushes_on_shutdown() {
    let Some(url) = test_url() else { return };
    let prefix = unique("flushes");
    let opened = AtomicUsize::new(0);
    Box::pin(lifecycle::shutdown_flushes(
        || production(&url),
        |topic| {
            let n = opened.fetch_add(1, Ordering::Relaxed);
            PulsarSubscription::new(topic, format!("{prefix}-{n}"))
        },
        |connected| connected.publisher(),
        Backlog::Missed,
    ))
    .await;
}

/// A settlement means on the server what it means in process: the two runs answer alike. The
/// server hands a delivery nobody settled back once its consumer closes.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_settles_as_it_does_in_process() {
    let Some(url) = test_url() else { return };
    Box::pin(settlement::matches_in_process(
        || production(&url),
        descriptor("settlement"),
        |connected| connected.publisher(),
        Duration::ZERO,
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_moves_spent_deliveries() {
    let Some(url) = test_url() else { return };
    Box::pin(retry::broker_moves(
        || production(&url),
        descriptor("moves"),
        |connected| connected.publisher(),
        ATTEMPTS,
    ))
    .await;
    Box::pin(retry::broker_moves(
        || production(&url),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        ATTEMPTS,
    ))
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_keeps_key_order() {
    let Some(url) = test_url() else { return };
    Box::pin(message_shape::keyed_order(
        || production(&url),
        &unique_subject("conformance.keyed"),
        descriptor("keyed"),
        |connected| connected.publisher(),
        keyed,
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_resolves_publish_options() {
    let Some(url) = test_url() else { return };
    Box::pin(message_shape::publish_options(
        || production(&url),
        &unique_subject("conformance.options"),
        descriptor("options"),
        PulsarPublish,
        key_cases(),
        observed_key,
    ))
    .await;
}

/// What a subscription opened after a publish receives is the same on both transports.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_backlog_matches_in_process() {
    let Some(url) = test_url() else { return };
    in_process::backlog_matches_server(|| production(&url), |connected| connected.publisher())
        .await;
}

/// The in-process transport refuses what the server refuses. The client waits for a held
/// exclusive subscription unless its tries are bounded, so the conflicting attach is probed on a
/// broker that gives up after one.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_refusals_match_in_process() {
    let Some(url) = test_url() else { return };
    let (topic, exclusive) = (unique_subject("conformance.exclusive"), unique("exclusive"));
    let held = || {
        PulsarSubscription::new(topic.clone(), exclusive.clone())
            .subscription_type(SubscriptionType::Exclusive)
    };
    Box::pin(in_process::refuses_like_the_server(
        || production(&url).operation_retries(OperationRetries::attempts(nonzero!(1u32))),
        |connected| connected.publisher(),
        [
            Refusal::PayloadOver {
                name: unique_subject("conformance.size"),
                limit: MAX_MESSAGE_SIZE,
            },
            Refusal::Publish {
                name: "not/a-topic".to_owned(),
            },
            Refusal::Subscription {
                source: PulsarSubscription::pattern("orders-(", unique("pattern")),
            },
            Refusal::Conflicting {
                open: held(),
                refused: held(),
            },
        ],
    ))
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_seeking_suite() {
    let Some(url) = test_url() else { return };
    Box::pin(capabilities::seeking(
        || production(&url),
        descriptor("seeking"),
        |connected| connected.publisher(),
    ))
    .await;
}

/// The same batch contract against a live consumer: the buffer sits over real deliveries here,
/// with the client's own flow control underneath it.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_batches_suite() {
    let Some(url) = test_url() else { return };
    Box::pin(capabilities::batches(
        || production(&url),
        descriptor("batches"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pulsar_broker_passes_batch_seeking_suite() {
    let Some(url) = test_url() else { return };
    Box::pin(capabilities::batch_seeking(
        || production(&url),
        descriptor("batch-seeking"),
        |connected| connected.publisher(),
    ))
    .await;
}
