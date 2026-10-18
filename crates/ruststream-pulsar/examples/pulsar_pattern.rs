//! Pattern subscriptions: one subscription over every topic whose full name matches a regular
//! expression, topics created later included. Each scenario is a test.
//!
//! ```text
//! cargo test --example pulsar_pattern --features testing
//! cargo run --example pulsar_pattern --features testing -- run
//! ```

use ruststream_pulsar::prelude::*;

// --8<-- [start:pattern]
/// What the `orders-*` producers write is not one schema, so the payload rides the byte lane:
/// `Deserialized` names the bytes without decoding them.
#[derive(Deserialized)]
struct Record<'a>(&'a [u8]);

/// Records every `orders-*` topic of `public/default`, those created after startup included.
#[subscriber(PulsarSubscription::pattern("persistent://public/default/orders-.*", "audit"))]
async fn audit(record: &Record<'_>) -> HandlerOutcome {
    tracing::info!(bytes = record.0.len(), "order event recorded");
    HandlerOutcome::ack()
}
// --8<-- [end:pattern]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(audit);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::*;

    /// What one producer writes; the audit reads it as bytes.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    struct Event {
        id: String,
    }

    /// How often the client lists the namespace again for a pattern subscription.
    const LISTING: Duration = Duration::from_secs(30);

    #[tokio::test(start_paused = true)]
    async fn a_new_topic_is_read_from_the_next_listing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // None of these topics existed when the subscription opened.
        for (topic, id) in [
            ("orders-eu", "eu-1"),
            ("orders-us", "us-1"),
            ("returns", "r-1"),
        ] {
            tb.broker::<PulsarBroker>()
                .message(&Event { id: id.to_owned() })
                .to(topic)
                .publish()
                .await?;
        }
        tb.broker::<PulsarBroker>()
            .subscriber("audit")
            .assert_not_called();

        tb.advance(LISTING).await?;
        for (topic, id) in [
            ("orders-eu", "eu-2"),
            ("orders-us", "us-2"),
            ("returns", "r-2"),
        ] {
            tb.broker::<PulsarBroker>()
                .message(&Event { id: id.to_owned() })
                .to(topic)
                .publish()
                .await?;
        }

        // The listing found both `orders-*` topics; `returns` does not match.
        let recorded: Vec<String> = tb
            .broker::<PulsarBroker>()
            .subscriber("audit")
            .assert_called(2)
            .settled(HandlerOutcome::ack())
            .received::<Event>()
            .into_iter()
            .map(|event| event.id)
            .collect();
        assert_eq!(recorded, ["eu-2", "us-2"]);
        Ok(())
    }
}
