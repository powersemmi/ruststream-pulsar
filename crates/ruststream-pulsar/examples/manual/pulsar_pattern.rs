//! The service of `examples/pulsar_pattern.rs` written without the macros: a pattern descriptor
//! is the source a registration maps its name to, and the byte-lane type is two short impls.
//!
//! ```text
//! cargo test --example manual_pulsar_pattern --features testing
//! cargo run --example manual_pulsar_pattern --features testing -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_pulsar::prelude::*;

// --8<-- [start:pattern]
/// What the `orders-*` producers write is not one schema, so the payload rides the byte lane:
/// the type is built from the bytes as they arrived, and no codec runs.
struct Record<'a>(&'a [u8]);

impl Deserialized for Record<'_> {
    type Output<'a> = Record<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Record<'_>, Infallible> {
        Ok(Record(payload))
    }
}

impl Input for Record<'_> {
    type Axis = SoloDeserialized<Record<'static>>;
}

/// Records each event of those topics.
struct Audit;

impl Handle<Record<'_>> for Audit {
    fn handle(
        &self,
        record: &Record<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(bytes = record.0.len(), "order event recorded");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            // Every `orders-*` topic of `public/default`, those created after startup included.
            b.include(
                subscriber(
                    PulsarSubscription::pattern("persistent://public/default/orders-.*", "audit"),
                    Audit,
                )
                .build(),
            );
        },
    )
}
// --8<-- [end:app]
// --8<-- [end:pattern]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::schemars::{self, JsonSchema};
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::*;

    /// What one producer writes; the audit reads it as bytes.
    #[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    struct Event {
        id: String,
    }

    // `#[derive(Outgoing)]` with no `name`, by hand: the call site names the topic.
    impl OutgoingDestination for Event {
        type Form = CallerName;
    }

    impl MessageHeaders for Event {
        type Contract = NoHeaders;
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
