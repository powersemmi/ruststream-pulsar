//! The checkout service of the Publishing and replies guide: a reply to the topic its type
//! declares, a reply to the topic the mount site names, a keyed publish through an `Out` slot, and
//! a message published once the service has started.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_publishing --features testing -- run
//! ```

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize)]
struct Order {
    id: u64,
    warehouse: String,
}

// --8<-- [start:reply]
/// The receipt of an order; the type names the topic it goes to.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// Confirms each order with a receipt.
#[subscriber(PulsarSubscription::new("orders", "receipts"), reply)]
async fn confirm(order: &Order) -> Receipt {
    tracing::info!(order.id, "order confirmed");
    Receipt { order: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:named_reply]
#[derive(Debug, PartialEq, Deserialize)]
struct QuoteRequest {
    request: u64,
    items: u32,
}

/// A price quote; the mount site names where it goes.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Quote {
    request: u64,
    total_cents: u64,
}

/// Answers a quote request on `quotes`, the topic the requesting services read.
#[subscriber(PulsarSubscription::new("quote-requests", "quotes"), reply("quotes"))]
async fn quote(request: &QuoteRequest) -> Quote {
    Quote {
        request: request.request,
        total_cents: u64::from(request.items) * 1_250,
    }
}
// --8<-- [end:named_reply]

// --8<-- [start:slot]
/// A pick list for the warehouse that ships the order.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "warehouse-picks")]
struct Pick {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Pick)]
struct Picks;

/// Sends each order to its warehouse, keyed by the warehouse so its picks stay in order.
#[subscriber(PulsarSubscription::new("orders", "dispatch"))]
async fn dispatch(
    order: &Order,
    Out(picks): Out<impl Publisher<Options = PulsarPublishOptions>, Picks>,
) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("source", "checkout");
    let sent = picks
        .message(&Pick { order: order.id })
        .with_headers(headers)
        .partition_key(order.warehouse.clone())
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(order.id, %error, "pick list not sent");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:slot]

// --8<-- [start:app]
/// Tells the rest of the system that checkout is up.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "service-events")]
struct Started {
    service: String,
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(confirm);
            b.include(quote);
            b.include(dispatch).out(Picks, Publish).build();
            b.after_startup(Publish, async move |publisher| {
                let started = Started {
                    service: "checkout".to_owned(),
                };
                publisher.message(&started).publish().await
            });
        },
    )
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
