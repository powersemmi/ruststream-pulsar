//! The refunds service of the Retries and dead letters guide, written without the macros: each
//! handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main`
//! is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_retries --features testing -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the card processor refused a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The processor's rate limit is used up for now.
    Busy,
    /// The processor did not answer.
    Unavailable,
    /// The card account is closed; another try gets the same answer.
    Declined,
}

/// Stands in for the card processor's client.
#[derive(Debug, Default)]
struct Processor {
    /// How many more calls find the processor busy.
    busy_for: Mutex<u32>,
    offline: AtomicBool,
}

impl Processor {
    async fn refund(&self, refund: &Refund) -> Result<(), Refusal> {
        tokio::task::yield_now().await;
        if self.offline.load(Ordering::Relaxed) {
            return Err(Refusal::Unavailable);
        }
        if refund.card.is_empty() {
            return Err(Refusal::Declined);
        }
        let busy = {
            let mut busy_for = self.busy_for.lock().unwrap_or_else(PoisonError::into_inner);
            let busy = *busy_for > 0;
            if busy {
                *busy_for -= 1;
            }
            busy
        };
        if busy { Err(Refusal::Busy) } else { Ok(()) }
    }
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone)]
struct Clients {
    processor: Arc<Processor>,
}

// --8<-- [start:refund]
/// How long a busy processor asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[outgoing(name = "refunds")]
struct Refund {
    id: u64,
    card: String,
    amount_cents: u64,
}

/// A refund left unsettled for two minutes comes back; a wait must be shorter than that.
fn refunds() -> PulsarSubscription {
    PulsarSubscription::new("refunds", "refunds").ack_timeout(Duration::from_secs(120))
}

struct ProcessRefund;

impl Handle<Refund, (), (), (), Clients> for ProcessRefund {
    async fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), HandlerOutcome> {
        match ctx.state().processor.refund(refund).await {
            Ok(()) => Ok(()),
            // Past the rate limit: a delivery right now would be refused again.
            Err(Refusal::Busy) => Err(HandlerOutcome::retry_after(BACKOFF)),
            // No answer: try again at once, up to the attempt cap.
            Err(Refusal::Unavailable) => Err(HandlerOutcome::retry()),
            Err(Refusal::Declined) => {
                tracing::warn!(refund.id, "card declined, refund dropped");
                Err(HandlerOutcome::drop())
            }
        }
    }
}
// --8<-- [end:refund]

// --8<-- [start:dead_letters]
/// Reads the refunds that ran out of attempts.
struct Review;

impl Handle<Refund, (), (), (), Clients> for Review {
    fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        _ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(refund.id, "refund needs a person to look at it");
        ready(Ok(()))
    }
}
// --8<-- [end:dead_letters]

// --8<-- [start:app]
fn app() -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("refunds", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| {
            // Five deliveries of one refund, the first included; then the dead-letter topic.
            b.include(subscriber(refunds(), ProcessRefund).build())
                .max_attempts(nonzero!(5u32))
                .dead_letter("refunds-dlq");
            b.include(
                subscriber(
                    PulsarSubscription::new("refunds-dlq", "refunds-review"),
                    Review,
                )
                .build(),
            );
        })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_pulsar::prelude::*;

    use super::{BACKOFF, Clients, Processor, Refund, app};

    fn with_processor(busy_for: u32, offline: bool) -> Clients {
        Clients {
            processor: Arc::new(Processor {
                busy_for: Mutex::new(busy_for),
                offline: AtomicBool::new(offline),
            }),
        }
    }

    fn refund(id: u64, card: &str) -> Refund {
        Refund {
            id,
            card: card.to_owned(),
            amount_cents: 4_200,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_processor_gets_the_refund_again_after_the_backoff() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::with_state(app(), |_| with_processor(1, false)).await?;

        tb.broker::<PulsarBroker>()
            .message(&refund(1, "4111"))
            .publish()
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        // The wait is the service's: nothing is published, and the same message comes back.
        tb.advance(BACKOFF).await?;
        let broker = tb.broker::<PulsarBroker>();
        let refunds = broker.subscriber("refunds").assert_called(2);
        assert_eq!(refunds.outcomes(), [Outcome::Nack, Outcome::Ack]);
        tb.broker::<PulsarBroker>()
            .published::<Refund>("refunds")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_refund_that_keeps_failing_moves_to_the_dead_letter_topic()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(0, true)).await?;

        tb.broker::<PulsarBroker>()
            .message(&refund(2, "4111"))
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("refunds")
            .assert_called(5);
        tb.broker::<PulsarBroker>()
            .published::<Refund>("refunds-dlq")
            .assert_called_once()
            .with(&refund(2, "4111"));
        tb.broker::<PulsarBroker>()
            .subscriber("refunds-dlq")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_declined_refund_is_dropped_and_goes_nowhere() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(0, false)).await?;

        tb.broker::<PulsarBroker>()
            .message(&refund(3, ""))
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<PulsarBroker>()
            .published::<Refund>("refunds-dlq")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
