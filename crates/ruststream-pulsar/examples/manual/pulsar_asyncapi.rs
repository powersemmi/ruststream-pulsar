//! The service of `examples/pulsar_asyncapi.rs` without the macros: the handlers are named types,
//! the descriptors are functions, and `main` is written by hand. The tests read the same
//! document.
//!
//! ```text
//! cargo run --example manual_pulsar_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_pulsar_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount site names the reply's topic.
impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct Regional;

impl Handle<Order> for Regional {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "routed");
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
        tracing::info!(order.id, "audited");
        ready(Ok(()))
    }
}

// --8<-- [start:server]
/// The cluster's TLS listener, authenticated with a JWT.
fn broker(token: &str) -> PulsarBroker {
    PulsarBroker::new("pulsar+ssl://pulsar.acme.internal:6651").token(token)
}
// --8<-- [end:server]

// --8<-- [start:app]
// --8<-- [start:channels]
// --8<-- [start:operations]
fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("pulsar", broker, |b| {
        b.include(
            subscriber(
                PulsarSubscription::new("persistent://acme/orders/created", "confirmations")
                    .ack_timeout(Duration::from_secs(30)),
                Confirm,
            )
            .reply()
            .to("persistent://acme/orders/confirmed")
            .build(),
        );
        b.include(
            subscriber(
                PulsarSubscription::topics(
                    ["persistent://acme/eu/orders", "persistent://acme/us/orders"],
                    "regional",
                )
                .subscription_type(SubscriptionType::KeyShared),
                Regional,
            )
            .build(),
        );
        b.include(subscriber(PulsarSubscription::pattern("audit-.*", "auditors"), Audit).build());
    })
}
// --8<-- [end:operations]
// --8<-- [end:channels]

fn app() -> impl App<State = ()> {
    let token = env::var("PULSAR_TOKEN").unwrap_or_default();
    service(broker(&token))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    const TOKEN: &str = "eyJhbGciOiJIUzI1NiJ9.secret";

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(TOKEN))).to_json()?,
        )?)
    }

    #[test]
    fn the_server_is_the_host_and_port_alone() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["pulsar"],
            json!({ "host": "pulsar.acme.internal:6651", "protocol": "pulsar" })
        );
        Ok(())
    }

    #[test]
    fn the_document_carries_no_token() -> Result<(), Box<dyn Error>> {
        let text = document()?.to_string();

        assert!(!text.contains(TOKEN), "{text}");
        Ok(())
    }

    #[test]
    fn a_channel_reports_the_namespace_and_persistence_of_its_topic() -> Result<(), Box<dyn Error>>
    {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(
            channels["persistent://acme/orders/created"]["bindings"],
            json!({
                "pulsar": {
                    "namespace": "orders",
                    "persistence": "persistent",
                    "bindingVersion": "0.1.0",
                },
            })
        );
        // The reply destination is a topic name too.
        assert_eq!(
            channels["persistent://acme/orders/confirmed"]["bindings"]["pulsar"]["namespace"],
            "orders"
        );
        // Two namespaces have no one namespace between them, and a pattern has no topic yet.
        for name in ["regional", "auditors"] {
            assert_eq!(channels[name]["address"], name, "{channels}");
            assert!(channels[name]["bindings"].is_null(), "{channels}");
        }
        Ok(())
    }

    #[test]
    fn a_receive_operation_reports_its_consumer() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let operations = &spec["operations"];

        assert_eq!(
            operations["receive_persistent___acme_orders_created"]["bindings"],
            json!({
                "x-ruststream-pulsar": {
                    "subscription": "confirmations",
                    "subscriptionType": "Shared",
                    "ackTimeoutMillis": 30000,
                },
            })
        );
        assert_eq!(
            operations["receive_regional"]["bindings"]["x-ruststream-pulsar"],
            json!({
                "subscription": "regional",
                "subscriptionType": "KeyShared",
                "topics": ["persistent://acme/eu/orders", "persistent://acme/us/orders"],
            })
        );
        assert_eq!(
            operations["receive_auditors"]["bindings"]["x-ruststream-pulsar"]["topicPattern"],
            "audit-.*"
        );
        Ok(())
    }
}
