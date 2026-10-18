//! `retry_after` on Pulsar: the service holds the delivery unacknowledged for the delay, then
//! negatively acknowledges it, and the broker hands the same message over again. Nothing is
//! republished. The scenario runs in process and, with `PULSAR_TEST_URL` set, against a broker.
//!
//! ```text
//! cargo test --example pulsar_settlement_retry_after --features testing
//! PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --example pulsar_settlement_retry_after --features testing
//! cargo run --example pulsar_settlement_retry_after -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    customer: u64,
}

// A stand-in for the customer directory: the first lookup after startup misses a customer that
// has not replicated yet, every later one finds it. Wired once at startup, injected through
// `State`.
#[derive(Clone, Default)]
struct Directory {
    replicated: Arc<AtomicBool>,
}

impl Directory {
    async fn find(&self, customer: u64) -> Result<(), String> {
        tokio::task::yield_now().await;
        if self.replicated.swap(true, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(format!("customer {customer} is not in the directory yet"))
        }
    }
}

#[derive(FromRef)]
struct AppState {
    directory: Directory,
}

// --8<-- [start:handler]
/// How long a customer the directory has not replicated yet gets before the next try.
const REPLICATION_LAG: Duration = Duration::from_secs(2);

/// Issues each invoice. An invoice whose customer is missing waits in the service for the lag,
/// then the broker delivers it again.
#[subscriber(PulsarSubscription::new("invoices", "billing"))]
async fn issue(invoice: &Invoice, State(directory): State<Directory>) -> HandlerOutcome {
    match directory.find(invoice.customer).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(invoice.id, %err, "customer not found, trying again later");
            HandlerOutcome::retry_after(REPLICATION_LAG)
        }
    }
}
// --8<-- [end:handler]

/// The service on the Pulsar cluster `url` names.
fn service(url: &str) -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                directory: Directory::default(),
            })
        })
        .with_broker(PulsarBroker::new(url), |b| {
            b.include(issue);
        })
}

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    service("pulsar://localhost:6650")
}

#[cfg(test)]
#[path = "support/pulsar_flow_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_pulsar::prelude::*;

    use super::{AppState, Invoice, REPLICATION_LAG, app, live, service};

    /// The body both modes run: the first delivery waits out the lag, and the broker hands the
    /// same message over again without a second publish.
    async fn a_late_customer_is_invoiced_after_the_lag(
        tb: TestApp<AppState>,
    ) -> Result<(), Box<dyn Error>> {
        let invoice = Invoice {
            id: 7,
            customer: 42,
        };
        tb.broker::<PulsarBroker>()
            .message(&invoice)
            .publish()
            .await?;
        tb.broker::<PulsarBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(REPLICATION_LAG));

        tb.advance(REPLICATION_LAG).await?;
        tb.broker::<PulsarBroker>()
            .subscriber("invoices")
            .assert_called(2)
            .with(&invoice);
        tb.broker::<PulsarBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_customer_is_invoiced_after_the_lag_in_process() -> Result<(), Box<dyn Error>> {
        a_late_customer_is_invoiced_after_the_lag(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_late_customer_is_invoiced_after_the_lag_live() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        live::delete_topic(&url, "invoices");
        a_late_customer_is_invoiced_after_the_lag(TestApp::start_live(service(&url)).await?).await
    }
}
