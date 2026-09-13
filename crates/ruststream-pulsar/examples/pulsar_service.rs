//! A minimal Pulsar service: a shared subscription with a retry cap and a dead-letter topic.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example pulsar_service -- run`

// --8<-- [start:handler]
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
        .ack_timeout(Duration::from_secs(30))
)]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(handle)
                .max_attempts(nonzero!(5))
                .dead_letter("orders-dlq");
        },
    )
}
// --8<-- [end:app]
