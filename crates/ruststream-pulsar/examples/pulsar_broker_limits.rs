//! `max_message_size`: the largest payload a publish may carry. A deployment that lowered the
//! server's `maxMessageSize` sets the same value on the broker, and a larger publish fails before
//! it is sent.
//!
//! ```text
//! cargo test --example pulsar_broker_limits --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[derive(OutSlot)]
#[publishes(Scan)]
struct Archive;

// --8<-- [start:app]
#[subscriber(PulsarSubscription::new("uploads", "archiver"))]
async fn archive(upload: &Upload, Out(scans): Out<impl Publisher, Archive>) -> HandlerOutcome {
    let scan = Scan {
        upload_id: upload.id,
        pages: (1..=upload.pages)
            .map(|page| format!("page-{page:06}"))
            .collect(),
    };
    if let Err(err) = scans.message(&scan).publish().await {
        // Retrying cannot shrink the scan, so it is logged and dropped.
        tracing::warn!(subscription = "archiver", upload.id, error = %err, "scan not archived");
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // The cluster runs with `maxMessageSize=1048576`.
    let broker =
        PulsarBroker::new("pulsar://localhost:6650").max_message_size(nonzero!(1_048_576usize));
    RustStream::new(AppInfo::new("archive", "0.1.0")).with_broker(broker, |b| {
        b.include(archive).out(Archive, Publish).build();
    })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "uploads")]
struct Upload {
    id: u64,
    pages: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "scans")]
struct Scan {
    upload_id: u64,
    pages: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Scan, Upload, app};

    #[tokio::test]
    async fn a_scan_under_the_limit_is_archived() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Upload { id: 7, pages: 2 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Scan>("scans")
            .assert_called_once()
            .with(&Scan {
                upload_id: 7,
                pages: vec!["page-000001".to_owned(), "page-000002".to_owned()],
            });
        tb.broker::<PulsarBroker>()
            .subscriber("uploads")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_scan_over_the_limit_is_refused_before_it_is_sent() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // A hundred thousand pages encode to about 1.4 MB, over the one-mebibyte limit.
        tb.broker::<PulsarBroker>()
            .message(&Upload {
                id: 8,
                pages: 100_000,
            })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Scan>("scans")
            .assert_not_called();
        tb.broker::<PulsarBroker>()
            .subscriber("uploads")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}
