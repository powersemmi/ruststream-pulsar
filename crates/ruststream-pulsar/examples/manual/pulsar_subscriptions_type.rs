//! The service of `examples/pulsar_subscriptions_type.rs` written without the macros: two
//! handlers on one `Shared` subscription are competing consumers.
//!
//! ```text
//! cargo test --example manual_pulsar_subscriptions_type --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Task {
    id: u64,
}

// `#[outgoing(name = "tasks")]` by hand: the type fixes its destination.
impl OutgoingDestination for Task {
    type Form = FixedName;

    const DESTINATION: &'static str = "tasks";
}

impl MessageHeaders for Task {
    type Contract = NoHeaders;
}

// --8<-- [start:descriptor]
/// One worker; the service runs two of them on one subscription.
struct Worker(&'static str);

impl Handle<Task> for Worker {
    fn handle(
        &self,
        task: &Task,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(task.id, worker = self.0, "task ran");
        ready(Ok(()))
    }
}

/// The subscription both workers attach to: each task goes to one of them.
fn workers() -> PulsarSubscription {
    PulsarSubscription::new("tasks", "workers").subscription_type(SubscriptionType::Shared)
}

fn app() -> impl App {
    let broker = PulsarBroker::new("pulsar://localhost:6650");
    RustStream::new(AppInfo::new("workers", "0.1.0")).with_broker(broker, |b| {
        for name in ["first", "second"] {
            b.include(
                subscriber("tasks", Worker(name))
                    .build()
                    .map_source(|_| workers()),
            );
        }
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
