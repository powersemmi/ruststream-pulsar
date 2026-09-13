//! What a Pulsar service's generated `AsyncAPI` document says about its subscriptions.
//!
//! The specification has a `pulsar` binding for the channel, so the namespace and the
//! persistence of the topic travel in it. Its operation and message objects are empty, so what a
//! reader most wants to know about a consumer - the subscription it joins, how that subscription
//! shares its stream, and when an unsettled delivery comes back - travels under the extension
//! key `x-ruststream-pulsar` instead.
//!
//! Everything reported here is read off the descriptor, before anything connects. A namespace
//! that only one topic of a multi-topic subscription agrees with is reported by nobody: a
//! single-valued field standing for several topics describes a deployment that does not exist.
#![cfg(all(feature = "testing", feature = "asyncapi"))]

use ruststream::DescribeServer;
use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// The excerpt the Pulsar guide shows. The test below holds the generated document to it, so the
/// page cannot drift from what a service publishes.
const EXCERPT: &str = include_str!("data/pulsar_document.json");

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Order {
    id: u64,
}

#[subscriber(
    PulsarSubscription::new("persistent://acme/orders/created", "workers")
        .ack_timeout(Duration::from_secs(30))
)]
async fn created(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber("orders")]
async fn by_name(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(
    PulsarSubscription::topics(
        ["persistent://acme/eu/orders", "persistent://acme/us/orders"],
        "regional",
    )
    .subscription_type(SubscriptionType::KeyShared)
)]
async fn regional(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(PulsarSubscription::pattern("audit-.*", "audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// The document a service built on this crate publishes.
fn document() -> Value {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .server(
            "pulsar",
            PulsarBroker::new("pulsar://broker:6650").describe_server(),
        )
        .with_broker(PulsarTestBroker::new(), |b| {
            b.include(created);
            b.include(by_name);
            b.include(regional);
            b.include(audit);
        });
    serde_json::from_str(
        &build_spec(&app)
            .to_json()
            .expect("the generated document must serialize"),
    )
    .expect("the generated document must be JSON")
}

/// Reads one object out of the document, or fails naming the path.
fn at<'a>(document: &'a Value, path: &[&str]) -> &'a Value {
    let mut here = document;
    for step in path {
        here = here
            .get(step)
            .unwrap_or_else(|| panic!("{} is missing from the document", path.join("/")));
    }
    here
}

/// The channel says what the topic name carries, and the operation says what the consumer is.
#[test]
fn a_subscription_describes_its_topic_and_its_consumer() {
    let document = document();
    let expected: Value = serde_json::from_str(EXCERPT).expect("the excerpt must be JSON");

    let channel = "persistent://acme/orders/created";
    assert_eq!(
        at(&document, &["channels", channel, "bindings"]),
        at(&expected, &["channels", channel, "bindings"]),
    );

    let operation = "receive_persistent___acme_orders_created";
    assert_eq!(
        at(&document, &["operations", operation, "bindings"]),
        at(&expected, &["operations", operation, "bindings"]),
    );
}

/// A bare name carries no descriptor, so nothing of Pulsar's reaches the document: a service
/// that wants its namespace and its consumer described names a `PulsarSubscription` instead.
#[test]
fn a_bare_name_describes_nothing() {
    let document = document();

    assert!(
        at(&document, &["channels", "orders"])
            .get("bindings")
            .is_none(),
        "a subscription opened by name has no descriptor to read a namespace off",
    );
    assert!(
        at(&document, &["operations", "receive_orders"])
            .get("bindings")
            .is_none(),
        "a subscription opened by name has no descriptor to describe",
    );
}

/// Two topics in two namespaces have no one namespace between them, so the channel stays
/// undescribed and the topics themselves travel in the extension instead.
#[test]
fn a_multi_topic_subscription_reports_no_single_namespace() {
    let document = document();

    assert!(
        at(&document, &["channels", "regional"])
            .get("bindings")
            .is_none(),
        "one namespace cannot stand for two",
    );
    let consumer = at(
        &document,
        &[
            "operations",
            "receive_regional",
            "bindings",
            "x-ruststream-pulsar",
        ],
    );
    assert_eq!(consumer["subscriptionType"], "KeyShared");
    assert_eq!(
        consumer["topics"],
        serde_json::json!(["persistent://acme/eu/orders", "persistent://acme/us/orders"]),
    );
}

/// A pattern names no topic at all, so it names its regular expression.
#[test]
fn a_pattern_subscription_reports_its_pattern() {
    let document = document();

    assert!(
        at(&document, &["channels", "audit"])
            .get("bindings")
            .is_none(),
        "a pattern has no namespace until it resolves against a server",
    );
    let consumer = at(
        &document,
        &[
            "operations",
            "receive_audit",
            "bindings",
            "x-ruststream-pulsar",
        ],
    );
    assert_eq!(consumer["topicPattern"], "audit-.*");
    assert_eq!(consumer["subscription"], "audit");
}

/// The wire protocol negotiates its version per connection, so the crate states none rather than
/// publishing a number no client has to match.
#[test]
fn the_server_states_no_protocol_version() {
    let document = document();
    let server = at(&document, &["servers", "pulsar"]);

    assert_eq!(server["protocol"], "pulsar");
    assert!(server.get("protocolVersion").is_none());
}

/// The document is published and shared, so a token in the service URL must not reach it - nor
/// may a binding body carry one.
#[test]
fn the_document_carries_no_credentials() {
    harness::describes_without_credentials(
        &PulsarBroker::new("pulsar://admin:s3cret@broker:6650").token("s3cret"),
        &PulsarSubscription::new("persistent://acme/orders/created", "workers"),
        "s3cret",
    );
}
