//! What one Pulsar publish is allowed to differ from the next in.
//!
//! One setting: the partition key, which keyed routing places the message by and `KeyShared`
//! subscriptions order by. The `partition_key` step names it for the message being assembled, so
//! the publish still leaves through the slot the mount site wired, with that slot's codec. A
//! publish that names no step carries the policy's own settings, and the message goes unkeyed
//! unless the call site wrote the `partition-key` header itself.
#![cfg(feature = "testing")]

use std::pin::pin;

use futures::StreamExt;
use ruststream::Subscriber;
use ruststream::codec::CborCodec;
use ruststream::testing::TestApp;
use ruststream_pulsar::PARTITION_KEY_HEADER;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;
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

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Notes;

// --8<-- [start:handler]
// A body that names a per-message setting imports this broker's prelude - the one exception to
// "a body imports the framework prelude alone" - and states the settings type on the slot.
#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn record(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = PulsarPublishOptions>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(&Receipt { id: order.id })
        .to("receipts")
        .partition_key(format!("user-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

/// The same publish with nothing named per message: the slot is bounded the same way, and what
/// reaches the broker is the policy's own settings.
#[subscriber(PulsarSubscription::new("orders.plain", "workers"))]
async fn note(
    order: &Order,
    Out(notes): Out<impl Publisher<Options = PulsarPublishOptions>, Notes>,
) -> HandlerOutcome {
    if notes
        .message(&Receipt { id: order.id })
        .to("receipts.plain")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The portable spelling: a body that writes its own headers keys the message through
/// `partition-key`, and every broker reads that name.
#[subscriber(PulsarSubscription::new("orders.tagged", "workers"))]
async fn tag(
    order: &Order,
    Out(notes): Out<impl Publisher<Options = PulsarPublishOptions>, Notes>,
) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, format!("user-{}", order.id));
    if notes
        .message(&Receipt { id: order.id })
        .with_headers(headers)
        .to("receipts.tagged")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Starts `app` on the harness and delivers one order to `topic`, the shape every test here
/// shares.
async fn delivered<Layers, Pipeline, Phase>(
    app: RustStream<Layers, (), Pipeline, Phase>,
    topic: &str,
) -> TestApp<()> {
    let tb = TestApp::start(app).await.expect("start harness");
    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 7 })
        .to(topic)
        .publish()
        .await
        .expect("publish");
    tb
}

/// The step's key is what the publish carried, and what reached the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_step_keys_the_message_the_slot_sends() {
    let app =
        RustStream::new(AppInfo::new("keyed", "0.1.0")).with_broker(PulsarTestBroker::new(), |b| {
            b.include(record).out(Ledger, Publish).build();
        });
    let tb = delivered(app, "orders").await;

    tb.out::<Ledger>()
        .assert_called_once()
        .with_options(&PulsarPublishOptions {
            partition_key: Some("user-7".to_owned()),
        });
    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 })
        .with_header(PARTITION_KEY_HEADER, "user-7");
}

/// A publish that names no step: the policy's settings are the whole answer, and this broker's
/// policy fixes no key, so the message leaves unkeyed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_with_no_step_leaves_the_message_unkeyed() {
    let app =
        RustStream::new(AppInfo::new("plain", "0.1.0")).with_broker(PulsarTestBroker::new(), |b| {
            b.include(note).out(Notes, Publish).build();
        });
    let tb = delivered(app, "orders.plain").await;

    tb.out::<Notes>()
        .assert_called_once()
        .assert_options_default();
    let receipts = tb
        .broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts.plain")
        .assert_called_once();
    assert_eq!(
        receipts.messages()[0].headers().get(PARTITION_KEY_HEADER),
        None,
        "a publish that named no key must not invent one",
    );
}

/// The header a call site writes itself still keys the message, so a service that spells the key
/// the portable way keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_site_header_keys_the_message_too() {
    let app = RustStream::new(AppInfo::new("tagged", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(tag).out(Notes, Publish).build();
        },
    );
    let tb = delivered(app, "orders.tagged").await;

    tb.out::<Notes>()
        .assert_called_once()
        .assert_options_default();
    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts.tagged")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "user-7");
}

/// The defect the step closes: a setting that wrapped the publisher took the publish off the
/// mount site's entry and lost the codec that entry named. A step is a position on the builder,
/// so a keyed publish still leaves in the slot's own format.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_publish_keeps_the_codec_the_mount_site_named() {
    let app = RustStream::new(AppInfo::new("keyed-codec", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(record)
                .out(Ledger, Publish)
                .codec(CborCodec)
                .build();
        },
    );
    let tb = delivered(app, "orders").await;

    tb.out::<Ledger>()
        .assert_called_once()
        .with_options(&PulsarPublishOptions {
            partition_key: Some("user-7".to_owned()),
        })
        .decoded_as::<Receipt>()
        .with_codec(&CborCodec, &Receipt { id: 7 });
    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with_codec(&CborCodec, &Receipt { id: 7 })
        .with_header(PARTITION_KEY_HEADER, "user-7");
}

/// What the consumer sees is the point of the setting: the key arrives as the delivery's own
/// partition key, which is what a `KeyShared` subscription orders by. Only a delivery can say
/// that, so this one reads the subscriber rather than the publish log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_delivery_reports_the_key_the_step_named() {
    let connected = PulsarTestBroker::new()
        .connect()
        .await
        .expect("the in-process broker connects");
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new("orders.keyed", "workers"))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .message(&Order { id: 7 })
        .to("orders.keyed")
        .partition_key("user-7")
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = stream
        .next()
        .await
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(delivery.partition_key(), Some(b"user-7".as_slice()));
}
