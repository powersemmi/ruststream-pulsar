//! Services under `TestApp` whose handlers are mounted by hand: a `Handle` body bound to a Pulsar
//! descriptor with `subscriber(source, body)`, the manual twin of `#[subscriber(..)]`.

#![cfg(feature = "testing")]

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::Outgoing;
use ruststream::runtime::{AppInfo, Context, Handle, HandlerOutcome, RustStream, subscriber};
use ruststream::testing::TestApp;
use ruststream_pulsar::{PulsarBroker, PulsarSubscription};
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "pulsar://localhost:6650";

/// How often the client lists the namespace again for a pattern subscription.
const PATTERN_REFRESH: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Outgoing)]
struct Order {
    id: u64,
}

/// Takes every order it is handed.
struct TakeOrder;

impl Handle<Order> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

/// Each addressing form of the descriptor mounts as it is, the way the attribute takes it, and
/// its subscription reads what is published to its topics. The client moves a spent delivery
/// itself, so no form names a retry destination at its mount site. A pattern reads a topic
/// created after it opened from the client's next listing, hence the second round.
#[tokio::test(start_paused = true)]
async fn a_handle_body_mounts_on_each_pulsar_descriptor() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(PulsarBroker::new(URL), |b| {
            b.include(
                subscriber(PulsarSubscription::new("orders", "workers"), TakeOrder)
                    .undocumented()
                    .build(),
            );
            b.include(
                subscriber(
                    PulsarSubscription::topics(["orders-eu", "orders-us"], "regional"),
                    TakeOrder,
                )
                .undocumented()
                .build(),
            );
            b.include(
                subscriber(
                    PulsarSubscription::pattern("persistent://public/default/audit-.*", "audit"),
                    TakeOrder,
                )
                .undocumented()
                .build(),
            );
        });
    let tb = TestApp::start(app).await.expect("start");

    for (topic, id) in [("orders", 1), ("orders-eu", 2), ("audit-eu", 3)] {
        tb.broker::<PulsarBroker>()
            .message(&Order { id })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }
    tb.advance(PATTERN_REFRESH)
        .await
        .expect("the client lists the namespace again");
    for (topic, id) in [("orders-us", 4), ("audit-eu", 5)] {
        tb.broker::<PulsarBroker>()
            .message(&Order { id })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<PulsarBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    assert_eq!(
        tb.broker::<PulsarBroker>()
            .subscriber("regional")
            .assert_called(2)
            .settled(HandlerOutcome::ack())
            .received::<Order>(),
        vec![Order { id: 2 }, Order { id: 4 }],
    );
    tb.broker::<PulsarBroker>()
        .subscriber("audit")
        .assert_called_once()
        .with(&Order { id: 5 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}
