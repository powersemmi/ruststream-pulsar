//! The service of `examples/pulsar_broker_security.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! PULSAR_TOKEN=... cargo run --example manual_pulsar_broker_security --features testing -- run
//! cargo test --example manual_pulsar_broker_security --features testing
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order fulfilled");
        ready(Ok(()))
    }
}

fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        let created = PulsarSubscription::new("persistent://acme/orders/created", "fulfilment");
        b.include(
            subscriber("persistent://acme/orders/created", Fulfil)
                .map_source(|_| created)
                .build(),
        );
    })
}

// --8<-- [start:broker]
/// The cluster's TLS listener, authenticated with a JWT.
fn broker(token: &str) -> PulsarBroker {
    PulsarBroker::new("pulsar+ssl://pulsar.acme.internal:6651").token(token)
}

fn app() -> impl App<State = ()> {
    let token = env::var("PULSAR_TOKEN").unwrap_or_default();
    service(broker(&token))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "persistent://acme/orders/created")]` by hand: the topic an order goes to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "persistent://acme/orders/created";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
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
