//! Batch handlers: a body over a slice takes the deliveries the subscription assembled, up to the
//! size the mount site names. Each scenario is a test.
//!
//! ```text
//! cargo test --example pulsar_batches --features testing
//! cargo run --example pulsar_batches --features testing -- run
//! ```

use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "readings")]
struct Reading {
    sensor: String,
    value: f64,
}

// --8<-- [start:handler]
/// Stores a batch of readings in one round-trip; one outcome settles the whole batch. A batch
/// that is not full closes 250 ms after its first reading.
#[subscriber(
    PulsarSubscription::new("readings", "aggregator")
        .batch_wait(Duration::from_millis(250))
)]
async fn aggregate(readings: &[Reading]) -> HandlerOutcome {
    tracing::info!(count = readings.len(), "stored in one round-trip");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            // At most 64 readings per call.
            b.include(aggregate.batch(nonzero!(64)));
        },
    )
}
// --8<-- [end:app]

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
