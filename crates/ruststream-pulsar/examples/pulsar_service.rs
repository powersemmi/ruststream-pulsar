//! A minimal Pulsar service: a shared subscription with a dead-letter policy.
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
        .dead_letter(DeadLetter::new("orders-dlq").max_deliveries(5))
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
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
