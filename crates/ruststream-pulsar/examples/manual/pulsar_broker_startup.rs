//! The service of `examples/pulsar_broker_startup.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_pulsar_broker_startup --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

// --8<-- [start:app]
struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order audited");
        ready(Ok(()))
    }
}

fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // A bare topic name: it joins the broker's default subscription.
        b.include(subscriber("orders", Place).build());
        // A pattern: checked as a regular expression before its consumer opens.
        b.include(subscriber(PulsarSubscription::pattern("audit-.*", "auditors"), Audit).build());
    })
}

fn app() -> impl App<State = ()> {
    service(PulsarBroker::new("pulsar://localhost:6650").default_subscription("orders-svc"))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PulsarError;
    use ruststream_pulsar::prelude::*;

    use super::{Audit, Order, Place, app, service};

    /// The `PulsarError` behind `err`, if any.
    fn pulsar_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a PulsarError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(pulsar) = err.downcast_ref::<PulsarError>() {
                return Some(pulsar);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn a_started_service_reads_its_topics() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_bare_name_without_a_default_subscription_fails_startup() {
        let failed = TestApp::start(service(PulsarBroker::new("pulsar://localhost:6650")))
            .await
            .expect_err("a bare name started with no subscription to join");

        assert!(
            matches!(
                pulsar_error(&failed),
                Some(PulsarError::Invalid(reason)) if reason.contains("default_subscription")
            ),
            "{failed}"
        );
    }

    #[tokio::test]
    async fn a_pattern_that_is_no_regular_expression_fails_startup() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            PulsarBroker::new("pulsar://localhost:6650").default_subscription("orders-svc"),
            |b| {
                b.include(subscriber("orders", Place).build());
                b.include(
                    subscriber(PulsarSubscription::pattern("audit-(", "auditors"), Audit).build(),
                );
            },
        );

        let failed = TestApp::start(app)
            .await
            .expect_err("a pattern that does not compile opened a consumer");
        assert!(
            matches!(pulsar_error(&failed), Some(PulsarError::Invalid(_))),
            "{failed}"
        );
    }
}
