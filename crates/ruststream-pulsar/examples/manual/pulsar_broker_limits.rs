//! The service of `examples/pulsar_broker_limits.rs` without the macros: the handler is a named
//! type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pulsar_broker_limits --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

/// The slot scans leave through.
struct Archive;

impl OutSlot for Archive {
    const NAME: &'static str = "Archive";
    // `Scan` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Archive> for Scan {}

// --8<-- [start:app]
struct ArchiveUpload;

impl<Entry> Handle<Upload, (), Outs<(Entry,)>> for ArchiveUpload
where
    Entry: OutEntry<Archive, Wire: Publisher>,
{
    async fn handle(
        &self,
        upload: &Upload,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let scan = Scan {
            upload_id: upload.id,
            pages: (1..=upload.pages)
                .map(|page| format!("page-{page:06}"))
                .collect(),
        };
        if let Err(err) = outs.get(Archive).message(&scan).publish().await {
            // Retrying cannot shrink the scan, so it is logged and dropped.
            tracing::warn!(subscription = "archiver", upload.id, error = %err, "scan not archived");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

fn app() -> impl App {
    // The cluster runs with `maxMessageSize=1048576`.
    let broker =
        PulsarBroker::new("pulsar://localhost:6650").max_message_size(nonzero!(1_048_576usize));
    RustStream::new(AppInfo::new("archive", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(
                PulsarSubscription::new("uploads", "archiver"),
                ArchiveUpload,
            )
            .build(),
        )
        .out(Archive, Publish)
        .build();
    })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Upload {
    id: u64,
    pages: u32,
}

// `#[outgoing(name = "uploads")]` by hand: an upload names the topic it is published to.
impl OutgoingDestination for Upload {
    type Form = FixedName;
    const DESTINATION: &'static str = "uploads";
}

impl MessageHeaders for Upload {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Scan {
    upload_id: u64,
    pages: Vec<String>,
}

// `#[outgoing(name = "scans")]` by hand: a scan names the topic it is published to.
impl OutgoingDestination for Scan {
    type Form = FixedName;
    const DESTINATION: &'static str = "scans";
}

impl MessageHeaders for Scan {
    type Contract = NoHeaders;
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
