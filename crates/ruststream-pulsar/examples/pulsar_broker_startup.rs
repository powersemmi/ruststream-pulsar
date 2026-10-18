//! Startup: the runtime dials the service URL once, then opens one consumer per subscription. A
//! subscription that cannot open stops the start, and the checks that need no server run before
//! any consumer opens.
//!
//! ```text
//! cargo test --example pulsar_broker_startup --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

// --8<-- [start:app]
/// A bare topic name: it joins the broker's default subscription.
#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

/// A pattern: checked as a regular expression before its consumer opens.
#[subscriber(PulsarSubscription::pattern("audit-.*", "auditors"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order audited");
    HandlerOutcome::ack()
}

fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
        b.include(audit);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(PulsarBroker::new("pulsar://localhost:6650").default_subscription("orders-svc"))
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::PulsarError;
    use ruststream_pulsar::prelude::*;

    use super::{Order, app, place, service};

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

    /// A pattern that does not compile as a regular expression.
    #[subscriber(PulsarSubscription::pattern("audit-(", "auditors"))]
    async fn broken(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "never reached");
        HandlerOutcome::ack()
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
                b.include(place);
                b.include(broken);
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
