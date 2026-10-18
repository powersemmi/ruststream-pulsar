//! The checkout service of the Publishing and replies guide, written without the macros: each
//! handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main`
//! is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_publishing --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    warehouse: String,
}

// --8<-- [start:reply]
/// The receipt of an order; the type names the topic it goes to.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// Confirms each order with a receipt.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Receipt { order: order.id }))
    }
}
// --8<-- [end:reply]

// --8<-- [start:named_reply]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
struct QuoteRequest {
    request: u64,
    items: u32,
}

/// A price quote; the mount site names where it goes.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
struct Quote {
    request: u64,
    total_cents: u64,
}

/// Answers a quote request on `quotes`, the topic the requesting services read.
struct AnswerQuote;

impl Handle<QuoteRequest, Quote> for AnswerQuote {
    fn handle(
        &self,
        request: &QuoteRequest,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Quote, HandlerOutcome>> {
        ready(Ok(Quote {
            request: request.request,
            total_cents: u64::from(request.items) * 1_250,
        }))
    }
}
// --8<-- [end:named_reply]

// --8<-- [start:slot]
/// A pick list for the warehouse that ships the order.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "warehouse-picks")]
struct Pick {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Pick)]
struct Picks;

/// Sends each order to its warehouse, keyed by the warehouse so its picks stay in order.
struct Dispatch;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Dispatch
where
    Entry: OutEntry<Picks, Wire: Publisher<Options = PulsarPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("source", "checkout");
        outs.get(Picks)
            .message(&Pick { order: order.id })
            .with_headers(headers)
            .partition_key(order.warehouse.clone())
            .publish()
            .await
            .map_err(|error| {
                tracing::warn!(order.id, %error, "pick list not sent");
                HandlerOutcome::retry()
            })
    }
}
// --8<-- [end:slot]

// --8<-- [start:app]
/// Tells the rest of the system that checkout is up.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "service-events")]
struct Started {
    service: String,
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(
                subscriber("orders", Confirm)
                    .map_source(|_| PulsarSubscription::new("orders", "receipts"))
                    .reply()
                    .build(),
            );
            b.include(
                subscriber("quote-requests", AnswerQuote)
                    .map_source(|_| PulsarSubscription::new("quote-requests", "quotes"))
                    .reply()
                    .to("quotes")
                    .build(),
            );
            b.include(
                subscriber("orders", Dispatch)
                    .map_source(|_| PulsarSubscription::new("orders", "dispatch"))
                    .build(),
            )
            .out(Picks, Publish)
            .build();
            b.after_startup(Publish, async move |publisher| {
                let started = Started {
                    service: "checkout".to_owned(),
                };
                publisher.message(&started).publish().await
            });
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PARTITION_KEY_HEADER;
    use ruststream_pulsar::prelude::*;
    use serde::Serialize;

    use super::{Pick, Picks, Quote, Receipt, Started, app};

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Placed {
        id: u64,
        warehouse: &'static str,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "quote-requests")]
    struct Ask {
        request: u64,
        items: u32,
    }

    #[tokio::test]
    async fn an_order_gets_a_receipt_and_a_keyed_pick_list() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Placed {
                id: 1,
                warehouse: "berlin",
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 1 });
        tb.out::<Picks>()
            .assert_called_once()
            .with_options(&PulsarPublishOptions {
                partition_key: Some("berlin".to_owned()),
            });
        // Headers travel as message properties, the key as the message key.
        tb.broker::<PulsarBroker>()
            .published::<Pick>("warehouse-picks")
            .assert_called_once()
            .with(&Pick { order: 1 })
            .with_header("source", "checkout")
            .with_header(PARTITION_KEY_HEADER, "berlin");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_quote_goes_to_the_topic_the_mount_site_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .message(&Ask {
                request: 2,
                items: 3,
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Quote>("quotes")
            .assert_called_once()
            .with(&Quote {
                request: 2,
                total_cents: 3_750,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_service_announces_itself_once_started() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<PulsarBroker>()
            .published::<Started>("service-events")
            .assert_called_once()
            .with(&Started {
                service: "checkout".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
