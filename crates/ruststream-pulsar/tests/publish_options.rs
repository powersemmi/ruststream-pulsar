//! What one Pulsar publish is allowed to differ from the next in.
//!
//! Nothing, on this crate's surface: `PulsarPublisher` declares `Options = ()`, so the publish
//! builder grows no step here and every message a slot sends carries the policy the mount site
//! named. The partition key is the one per-message value the crate does carry, and it is a
//! header rather than an option: `Partitioned` is a framework contract every broker spells the
//! same way, and a service that switches brokers keeps its keyed routing.
#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_pulsar::PARTITION_KEY_HEADER;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::{PulsarTestBroker, PulsarTestPublisher};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Receipt {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Ledger;

/// The bound is the assertion the compiler makes: this broker's publisher has no per-message
/// settings, so a body written against `Options = ()` mounts on it. A broker that grew one
/// would fail this file before any test ran.
#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn record(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = ()>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(&Receipt { id: order.id })
        .to("receipts")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A slot publish carries the policy's own settings and nothing else, which is what the harness
/// reads back as "no options were set on this message".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_publish_carries_no_per_message_options() {
    let app = RustStream::new(AppInfo::new("options", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(record).out(Ledger, Publish).build();
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.out::<Ledger>()
        .assert_called_once()
        .assert_options_default();
    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
}

/// The per-message value this crate does carry travels as the header the framework defines, so
/// the publish reaches the broker keyed and a `KeyShared` subscription can order by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_partition_key_reaches_the_broker_as_a_header() {
    let app =
        RustStream::new(AppInfo::new("keyed", "0.1.0")).with_broker(PulsarTestBroker::new(), |b| {
            b.after_startup(Publish, async move |publisher: PulsarTestPublisher| {
                publisher
                    .with_partition_key("user-42")
                    .message(&Order { id: 1 })
                    .to("orders")
                    .publish()
                    .await
            });
        });
    let tb = TestApp::start(app).await.expect("start harness");
    tb.settle().await.expect("the startup publish settles");

    tb.broker::<PulsarTestBroker>()
        .published::<Order>("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .with_header(PARTITION_KEY_HEADER, "user-42");
}
