//! `SubscriptionType`: how the consumers of one subscription share it. Two handlers on one
//! `Shared` subscription are competing consumers. Each scenario is a test.
//!
//! ```text
//! cargo test --example pulsar_subscriptions_type --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "tasks")]
struct Task {
    id: u64,
}

// --8<-- [start:descriptor]
/// Two workers attach to the subscription `workers`: each task goes to one of them.
#[subscriber(
    PulsarSubscription::new("tasks", "workers").subscription_type(SubscriptionType::Shared)
)]
async fn first_worker(task: &Task) -> HandlerOutcome {
    tracing::info!(task.id, "first worker ran the task");
    HandlerOutcome::ack()
}

#[subscriber(
    PulsarSubscription::new("tasks", "workers").subscription_type(SubscriptionType::Shared)
)]
async fn second_worker(task: &Task) -> HandlerOutcome {
    tracing::info!(task.id, "second worker ran the task");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("workers", "0.1.0")).with_broker(broker, |b| {
        b.include(first_worker);
        b.include(second_worker);
    })
}
// --8<-- [end:descriptor]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn each_task_runs_once_between_the_workers() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in 1..=4 {
            tb.broker::<PulsarBroker>()
                .message(&Task { id })
                .publish()
                .await?;
        }

        // Four tasks, four calls between the two workers: none runs twice.
        let mut ran: Vec<u64> = tb
            .broker::<PulsarBroker>()
            .subscriber("tasks")
            .assert_called(4)
            .settled(HandlerOutcome::ack())
            .received::<Task>()
            .into_iter()
            .map(|task| task.id)
            .collect();
        ran.sort_unstable();
        assert_eq!(ran, [1, 2, 3, 4]);
        Ok(())
    }
}
