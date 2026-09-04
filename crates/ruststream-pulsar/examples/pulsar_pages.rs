//! Page handlers: a body that is handed a whole page of deliveries and settles it at once.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example pulsar_pages -- run`

// --8<-- [start:handler]
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Reading {
    sensor: String,
    value: f64,
}

/// Pulsar's client hands over one delivery at a time, so the page is assembled on the client.
/// `page_wait` is the half of that this crate owns: how long a page that is not yet full waits
/// for the rest before the handler sees it.
#[subscriber(
    PulsarSubscription::new("readings", "aggregator")
        .page_wait(Duration::from_millis(250))
)]
async fn aggregate(readings: &[Reading]) -> HandlerOutcome {
    for reading in readings {
        println!("{}: {}", reading.sensor, reading.value);
    }
    println!("settled a page of {}", readings.len());
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            // The page size belongs to the registration, not to the subscription: the framework
            // carries it down to the subscriber, which never delivers more than this.
            b.include(aggregate.batch(nonzero!(64)));
        },
    )
}
// --8<-- [end:app]
