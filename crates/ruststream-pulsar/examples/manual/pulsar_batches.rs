//! The service of `examples/pulsar_batches.rs` written without the macros: a body over `[T]` is
//! a batch handler.
//!
//! ```text
//! cargo test --example manual_pulsar_batches --features testing
//! cargo run --example manual_pulsar_batches --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Reading {
    sensor: String,
    value: f64,
}

// `#[outgoing(name = "readings")]` by hand: the type fixes its destination.
impl OutgoingDestination for Reading {
    type Form = FixedName;

    const DESTINATION: &'static str = "readings";
}

impl MessageHeaders for Reading {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
/// Stores a batch of readings in one round-trip; `Ok` settles the whole batch.
struct Aggregate;

impl Handle<[Reading]> for Aggregate {
    fn handle(
        &self,
        readings: &[Reading],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = readings.len(), "stored in one round-trip");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            // A batch that is not full closes 250 ms after its first reading.
            let readings = PulsarSubscription::new("readings", "aggregator")
                .batch_wait(Duration::from_millis(250));
            // At most 64 readings per call.
            b.include(
                subscriber("readings", Aggregate)
                    .batch(nonzero!(64))
                    .build()
                    .map_source(|_| readings),
            );
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    fn reading(value: f64) -> Reading {
        Reading {
            sensor: "boiler".to_owned(),
            value,
        }
    }

    #[tokio::test]
    async fn three_waiting_readings_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Published at once, the three readings wait in the subscription together.
        let readings = [reading(1.0), reading(2.0), reading(3.0)];
        let broker = tb.broker::<PulsarBroker>();
        for published in join_all(readings.iter().map(|r| broker.message(r).publish())).await {
            published?;
        }

        tb.broker::<PulsarBroker>()
            .subscriber("readings")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
