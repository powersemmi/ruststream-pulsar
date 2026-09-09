//! What the crate prelude must put in scope, and under which name; and which name a reply
//! actually lands on.
//!
//! The glob re-exports `ruststream::prelude::*` and aliases this broker's policies over it, so
//! the two vocabularies meet here and nowhere else: a routes file writing this glob has to get
//! the mount-site name `Publish` for the policy and reach it through `.out(Reply, ..)`, while the
//! capability trait a handler body bounds an injected slot with has to stay reachable. All of it
//! is checked by compiling, so a re-export that took the wrong name fails in this crate's own
//! suite rather than in a user's service file.
//!
//! Where a reply goes is a name too, and the reply type settles it: a type carrying
//! `#[outgoing(name = "..")]` is published there, and a type carrying none is published where the
//! mount site says. Both names reach the broker as the published message's own name, and the
//! Pulsar publisher qualifies that one name when it opens the topic's producer, so a bare
//! `receipts` addresses `persistent://public/default/receipts` whichever of the two declared it.

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(feature = "testing")]
use ruststream::testing::TestApp;
#[cfg(feature = "testing")]
use ruststream_pulsar::testing::PulsarTestBroker;

/// A slot is bounded by the broker capability trait, and the glob keeps it reachable.
fn _slots_are_bounded_by_the_capability_trait<T: Publisher>() {}

/// The mount-site vocabulary: the policy answers to its concept name, as a type and as a value.
///
/// The value is spelled bare rather than `Publish::default()`: the policy is a unit struct, and
/// the workspace lints reject both the inherent `default()` on one and the `Default::default()`
/// that would dodge it. What is being pinned is the name, which either spelling would pin.
fn _the_policy_answers_to_its_concept_name() {
    let _: Publish = Publish;
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Order {
    id: u64,
}

/// A reply that leaves its destination to the mount site: the derive carries no name.
#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Confirmation {
    id: u64,
}

/// A reply that is always a receipt on the receipts topic, so the type says so once and every
/// mount site of it inherits the name.
#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber("receipt-requests", publish)]
async fn issue_receipt(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The verb a routes file writes: the reply position takes this broker's policy under the name
/// the glob gives it, for a reply named at the mount site and for one that names itself.
///
/// Assembled rather than described, so the mount-site spelling the prelude and the Pulsar guide
/// quote cannot drift from the one the chain accepts. Assembly is I/O-free - the broker dials in
/// `connect`, which only a started app reaches - so the check costs no server.
#[test]
fn the_policy_binds_the_reply_position() {
    let _app = RustStream::new(AppInfo::new("prelude", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(confirm).out(Reply, Publish);
            b.include(issue_receipt).out(Reply, Publish);
        },
    );
}

/// A reply type that names its own topic is published there, and the mount site never repeats
/// the name.
#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_lands_on_the_topic_its_type_declares() {
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(issue_receipt);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 7 })
        .to("receipt-requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<PulsarTestBroker>()
        .subscriber("receipt-requests")
        .assert_called_once();
    tb.broker::<PulsarTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
}

/// A reply type that names no topic is published where the mount site says.
#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_lands_on_the_topic_the_mount_site_names() {
    let app = RustStream::new(AppInfo::new("mounted", "0.1.0")).with_broker(
        PulsarTestBroker::new(),
        |b| {
            b.include(confirm);
        },
    );
    let tb = TestApp::start(app).await.expect("start harness");

    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<PulsarTestBroker>()
        .subscriber("orders")
        .assert_called_once();
    tb.broker::<PulsarTestBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 7 });
}
