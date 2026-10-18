//! Security: a broker that dials Pulsar over TLS and authenticates with a JWT read from the
//! environment. The in-process test runs the same app; the token never reaches the generated
//! document.
//!
//! ```text
//! PULSAR_TOKEN=... cargo run --example pulsar_broker_security --features testing -- run
//! cargo test --example pulsar_broker_security --features testing
//! ```

use std::env;

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[subscriber(PulsarSubscription::new("persistent://acme/orders/created", "fulfilment"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order fulfilled");
    HandlerOutcome::ack()
}

fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    })
}

// --8<-- [start:broker]
/// The cluster's TLS listener, authenticated with a JWT.
fn broker(token: &str) -> PulsarBroker {
    PulsarBroker::new("pulsar+ssl://pulsar.acme.internal:6651").token(token)
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let token = env::var("PULSAR_TOKEN").unwrap_or_default();
    service(broker(&token))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "persistent://acme/orders/created")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Order, broker, service};

    #[tokio::test]
    async fn the_service_runs_in_process_with_its_tls_url_and_token() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(service(broker("eyJhbGciOiJIUzI1NiJ9.test"))).await?;
        tb.broker::<PulsarBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .subscriber("persistent://acme/orders/created")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn the_token_stays_out_of_the_document() -> Result<(), Box<dyn Error>> {
        use ruststream::asyncapi::build_spec;

        const TOKEN: &str = "eyJhbGciOiJIUzI1NiJ9.secret";
        let app = RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker_labeled(
            "pulsar",
            broker(TOKEN),
            |_| {},
        );
        let document = build_spec(&app).to_json()?;

        assert!(document.contains("pulsar.acme.internal:6651"), "{document}");
        assert!(!document.contains(TOKEN), "{document}");
        Ok(())
    }
}
