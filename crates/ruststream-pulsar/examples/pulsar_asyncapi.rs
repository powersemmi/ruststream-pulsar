//! The `AsyncAPI` document of a Pulsar service: the server is the host and port of the service
//! URL, a channel's `pulsar` binding carries the namespace and persistence of its topic, and a
//! receive operation reports its consumer under `x-ruststream-pulsar`. The tests read the document
//! the app generates.
//!
//! ```text
//! cargo run --example pulsar_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example pulsar_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_pulsar::prelude::*;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
struct Confirmation {
    id: u64,
}

// --8<-- [start:channels]
// --8<-- [start:operations]
#[subscriber(
    PulsarSubscription::new("persistent://acme/orders/created", "confirmations")
        .ack_timeout(Duration::from_secs(30)),
    reply("persistent://acme/orders/confirmed")
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}
// --8<-- [end:operations]

#[subscriber(
    PulsarSubscription::topics(
        ["persistent://acme/eu/orders", "persistent://acme/us/orders"],
        "regional",
    )
    .subscription_type(SubscriptionType::KeyShared)
)]
async fn regional(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "routed");
    HandlerOutcome::ack()
}

#[subscriber(PulsarSubscription::pattern("audit-.*", "auditors"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "audited");
    HandlerOutcome::ack()
}
// --8<-- [end:channels]

// --8<-- [start:server]
/// The cluster's TLS listener, authenticated with a JWT.
fn broker(token: &str) -> PulsarBroker {
    PulsarBroker::new("pulsar+ssl://pulsar.acme.internal:6651").token(token)
}
// --8<-- [end:server]

// --8<-- [start:app]
fn service(broker: PulsarBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("pulsar", broker, |b| {
        b.include(confirm);
        b.include(regional);
        b.include(audit);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let token = env::var("PULSAR_TOKEN").unwrap_or_default();
    service(broker(&token))
}
// --8<-- [end:app]

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
