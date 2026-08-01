//! Pattern subscriptions: one consumer over every topic matching a regular expression.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example pulsar_pattern -- run`

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_pulsar::{PulsarBroker, PulsarSubscription};

/// One subscription spans every `orders-*` topic in the lookup namespace, including topics
/// created after the consumer attached. What those producers write is not one schema, so the
/// handler takes the payload raw instead of naming a type.
#[subscriber(PulsarSubscription::pattern("orders-.*", "audit"), raw)]
async fn audit(payload: &[u8]) -> HandlerResult {
    println!("audit: {}", String::from_utf8_lossy(payload));
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(audit);
        },
    )
}
