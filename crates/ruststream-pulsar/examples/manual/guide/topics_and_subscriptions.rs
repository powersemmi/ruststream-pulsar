//! The fulfilment service of the Topics and subscriptions guide, written without the macros: each
//! handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and its
//! descriptor joins the chain through `map_source`.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_topics_and_subscriptions --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    region: String,
}

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Return {
    order: u64,
}

// --8<-- [start:topic]
/// Ships every order. The subscription `fulfilment` is this service's cursor on `orders`.
struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
}

/// Reads the same topic under a subscription of its own, so it sees every order too.
struct Notify;

impl Handle<Order> for Notify {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "customer notified");
        ready(Ok(()))
    }
}
// --8<-- [end:topic]

// --8<-- [start:topics]
/// Totals the orders of two regions through one consumer.
struct Tally;

impl Handle<Order> for Tally {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.region, "order counted");
        ready(Ok(()))
    }
}
// --8<-- [end:topics]

// --8<-- [start:pattern]
/// Audits every regional topic, those created after startup included.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.region, "order audited");
        ready(Ok(()))
    }
}
// --8<-- [end:pattern]

// --8<-- [start:short]
/// Restocks returned items, under the broker's default subscription.
struct Restock;

impl Handle<Return> for Restock {
    fn handle(
        &self,
        item: &Return,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(item.order, "return restocked");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650").default_subscription("fulfilment"),
        |b| {
            b.include(
                subscriber("orders", Fulfil)
                    .build()
                    .map_source(|_| PulsarSubscription::new("orders", "fulfilment")),
            );
            b.include(
                subscriber("orders", Notify)
                    .build()
                    .map_source(|_| PulsarSubscription::new("orders", "notifications")),
            );
            b.include(
                subscriber("regional-totals", Tally)
                    .build()
                    .map_source(|_| {
                        PulsarSubscription::topics(["orders-eu", "orders-us"], "regional-totals")
                    }),
            );
            b.include(subscriber("audit", Audit).build().map_source(|_| {
                PulsarSubscription::pattern("persistent://public/default/orders-.*", "audit")
            }));
            b.include(subscriber("returns", Restock).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:short]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;
    use serde::Serialize;

    use super::{Order, Return, app};

    /// How often the client lists the namespace again for a pattern subscription.
    const LISTING: Duration = Duration::from_secs(30);

    #[derive(Serialize, Outgoing)]
    struct Placed {
        id: u64,
        region: &'static str,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "returns")]
    struct Returned {
        order: u64,
    }

    fn order(id: u64, region: &str) -> Order {
        Order {
            id,
            region: region.to_owned(),
        }
    }

    #[tokio::test]
    async fn every_subscription_reads_its_own_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Placed {
                id: 1,
                region: "eu",
            })
            .to("persistent://public/default/orders")
            .publish()
            .await?;

        // `fulfilment` and `notifications` each read the order once.
        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called(2)
            .with(&order(1, "eu"));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_list_reads_each_of_its_topics() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, region) in [(2, "eu"), (3, "us")] {
            tb.broker::<PulsarBroker>()
                .message(&Placed { id, region })
                .to(format!("orders-{region}"))
                .publish()
                .await?;
        }

        let mut counted: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("regional-totals")
            .assert_called(2)
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        counted.sort_unstable();
        assert_eq!(counted, [2, 3]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_pattern_reads_a_new_topic_from_the_next_listing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // `orders-asia` did not exist when the subscription opened.
        tb.broker::<PulsarBroker>()
            .message(&Placed {
                id: 4,
                region: "asia",
            })
            .to("orders-asia")
            .publish()
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("audit")
            .assert_not_called();

        tb.advance(LISTING).await?;
        tb.broker::<PulsarBroker>()
            .message(&Placed {
                id: 5,
                region: "asia",
            })
            .to("orders-asia")
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("audit")
            .assert_called_once()
            .with(&order(5, "asia"));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_bare_name_joins_the_default_subscription() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Returned { order: 6 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("returns")
            .assert_called_once()
            .with(&Return { order: 6 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
