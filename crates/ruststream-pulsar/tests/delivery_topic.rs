//! The topic a delivery names, in process.
//!
//! The client hands its consumer the topic names the subscription gave it and reports each
//! delivery under that name: `refunds` stays `refunds`, a full name stays full, and a pattern,
//! which the client resolves by listing the namespace, reports the full names of that listing.
//! The in-process mode reports the same, so a layer that reads the topic sees what it sees on a
//! server.
#![cfg(feature = "testing")]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ruststream::runtime::{DynMiddleware, DynStack, Next};
use ruststream::testing::{Outcome, TestApp};
use ruststream_pulsar::PulsarMessage;
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "pulsar://localhost:6650";

/// The header the layer below hands the topic to the handler in.
const TOPIC: &str = "delivered-on";

/// A message that says which topic name its delivery should report.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Ping {
    named: String,
}

/// Hands the delivery's topic to the handler in a header, the way a service's layer over the
/// broker's own message reads it.
struct ReadTopic;

impl DynMiddleware<PulsarMessage> for ReadTopic {
    fn handle<'a>(
        &'a self,
        input: &'a PulsarMessage,
        ctx: &'a mut Context<'_>,
        next: Next<'a, PulsarMessage>,
    ) -> Pin<Box<dyn Future<Output = HandlerOutcome> + Send + 'a>> {
        Box::pin(async move {
            ctx.headers_mut().insert(TOPIC, input.topic().to_owned());
            next.run(input, ctx).await
        })
    }
}

/// Acknowledges a delivery that names the topic its message expects, and drops any other.
fn check<State>(ping: &Ping, ctx: &Context<'_, (), State>) -> HandlerOutcome {
    let named = ctx.headers().get_str(TOPIC);
    if named == Some(ping.named.as_str()) {
        HandlerOutcome::ack()
    } else {
        tracing::warn!(subscription = ctx.name(), ?named, expected = %ping.named, "misnamed");
        HandlerOutcome::drop()
    }
}

#[subscriber(PulsarSubscription::new("refunds", "short"))]
async fn short(ping: &Ping, ctx: &mut Context<'_>) -> HandlerOutcome {
    check(ping, ctx)
}

#[subscriber(PulsarSubscription::new("persistent://public/default/refunds", "full"))]
async fn full(ping: &Ping, ctx: &mut Context<'_>) -> HandlerOutcome {
    check(ping, ctx)
}

#[subscriber(PulsarSubscription::topics(["refunds-eu", "public/default/refunds-us"], "listed"))]
async fn listed(ping: &Ping, ctx: &mut Context<'_>) -> HandlerOutcome {
    check(ping, ctx)
}

#[subscriber(PulsarSubscription::pattern("persistent://public/default/audit-.*", "audit"))]
async fn audit(ping: &Ping, ctx: &mut Context<'_>) -> HandlerOutcome {
    check(ping, ctx)
}

/// The layer, one per registration: a stack is fixed to the broker's message type.
fn read_topic() -> DynStack<PulsarMessage> {
    DynStack::new([Arc::new(ReadTopic) as Arc<dyn DynMiddleware<PulsarMessage>>])
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("topics", "0.1.0")).with_broker(PulsarBroker::new(URL), |b| {
        b.include_router(
            Router::new()
                .include(short)
                .layer(read_topic())
                .include(full)
                .layer(read_topic())
                .include(listed)
                .layer(read_topic())
                .include(audit)
                .layer(read_topic()),
        );
    })
}

/// A single topic is reported as the descriptor wrote it, short or full.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_is_named_as_the_subscription_wrote_it() {
    let tb = TestApp::start(app()).await.expect("start harness");
    let broker = tb.broker::<PulsarBroker>();

    for named in ["refunds", "persistent://public/default/refunds"] {
        broker
            .message(&Ping {
                named: named.to_owned(),
            })
            .to("refunds")
            .publish()
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let by_short = broker.subscriber("refunds").assert_called(2).outcomes();
    let by_full = broker
        .subscriber("persistent://public/default/refunds")
        .assert_called(2)
        .outcomes();
    assert_eq!(by_short, [Outcome::Ack, Outcome::Drop]);
    assert_eq!(by_full, [Outcome::Drop, Outcome::Ack]);
}

/// Each topic of a list is reported as the list wrote it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_listed_topic_is_named_as_the_list_wrote_it() {
    let tb = TestApp::start(app()).await.expect("start harness");
    let broker = tb.broker::<PulsarBroker>();

    for (topic, named) in [
        ("persistent://public/default/refunds-eu", "refunds-eu"),
        ("refunds-us", "public/default/refunds-us"),
    ] {
        broker
            .message(&Ping {
                named: named.to_owned(),
            })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let outcomes = broker.subscriber("listed").assert_called(2).outcomes();
    assert_eq!(outcomes, [Outcome::Ack, Outcome::Ack]);
}

/// A pattern finds its topics by listing their full names, so a full name is what it reports.
///
/// The topic appears after the subscription opened, so the client reads it from its next listing
/// of the namespace; the paused clock is what lets the test reach that listing.
#[tokio::test(start_paused = true)]
async fn a_pattern_names_the_full_topic() {
    let tb = TestApp::start(app()).await.expect("start harness");
    let broker = tb.broker::<PulsarBroker>();
    let ping = Ping {
        named: "persistent://public/default/audit-eu".to_owned(),
    };

    // The first publish creates the topic; the pattern reads it once the client lists it again.
    broker
        .message(&ping)
        .to("audit-eu")
        .publish()
        .await
        .expect("publish");
    tb.advance(Duration::from_secs(30))
        .await
        .expect("the client lists the namespace again");
    broker
        .message(&ping)
        .to("audit-eu")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    broker
        .subscriber("audit")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
}
