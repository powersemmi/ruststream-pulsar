//! Pattern subscriptions: one consumer over every topic matching a regular expression.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example pulsar_pattern -- run`

use ruststream_pulsar::prelude::*;

/// One subscription spans every `orders-*` topic in the lookup namespace, including topics
/// created after the consumer attached. What those producers write is not one schema, so the
/// payload rides the byte lane: `Deserialized` names the bytes without decoding them, and no
/// codec stands between the broker and the handler.
///
/// An audit trail wants the whole record, so the subscription opens at the beginning of the
/// retained log; the clause seeks on every startup, not only when the subscription is created.
// --8<-- [start:pattern]
#[derive(Deserialized)]
struct Record<'a>(&'a [u8]);

#[subscriber(
    PulsarSubscription::pattern("orders-.*", "audit"),
    start_at(PulsarPosition::earliest())
)]
async fn audit(record: &Record<'_>) -> HandlerOutcome {
    println!("audit: {}", String::from_utf8_lossy(record.0));
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
