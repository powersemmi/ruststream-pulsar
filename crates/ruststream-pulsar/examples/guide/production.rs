//! The service of the production guide: an orders service configured for a cluster that takes
//! TLS and a token, with its address and token read from the environment.
//!
//! ```text
//! PULSAR_URL=pulsar+ssl://pulsar.example.com:6651 PULSAR_TOKEN=... \
//!     cargo run --example guide_production --features testing -- run
//! ```

use std::env;
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
/// Where the cluster is and who the service is, as the deployment provides them.
struct Cluster {
    url: String,
    token: String,
}

impl Cluster {
    fn from_env() -> Self {
        let read = |name: &str| env::var(name).unwrap_or_default();
        Self {
            url: read("PULSAR_URL"),
            token: read("PULSAR_TOKEN"),
        }
    }
}

fn broker(cluster: Cluster) -> PulsarBroker {
    // Ten tries, three seconds apart; then the error.
    let retries = OperationRetries::attempts(nonzero!(10u32)).delay(Duration::from_secs(3));
    PulsarBroker::new(cluster.url)
        .token(cluster.token)
        .operation_retries(retries)
        // The cluster runs with `maxMessageSize=10485760`.
        .max_message_size(nonzero!(10_485_760usize))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

// --8<-- [start:service]
#[subscriber(PulsarSubscription::new("orders", "orders-svc"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { id: order.id }
}

fn service(broker: PulsarBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(broker(Cluster::from_env()))
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{Cluster, Confirmation, Order, broker, service};

    fn cluster(url: &str) -> Cluster {
        Cluster {
            url: url.to_owned(),
            token: "eyJhbGciOiJIUzI1NiJ9.orders-svc".to_owned(),
        }
    }

    #[tokio::test]
    async fn the_production_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let production = cluster("pulsar+ssl://pulsar.example.com:6651");
        let tb = TestApp::start(service(broker(production))).await?;

        tb.broker::<PulsarBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<PulsarBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_url_the_client_cannot_dial_fails_the_start() {
        let refused = cluster("https://pulsar.example.com:6651");

        let error = TestApp::start(service(broker(refused)))
            .await
            .err()
            .map(|error| error.to_string());

        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("pulsar or pulsar+ssl")),
            "{error:?}",
        );
    }
}
