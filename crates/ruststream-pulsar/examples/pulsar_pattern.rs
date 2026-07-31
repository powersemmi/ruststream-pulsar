//! Pattern subscriptions: one consumer over every topic matching a regular expression.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example pulsar_pattern`

use std::pin::pin;

use futures::StreamExt;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber};
use ruststream_pulsar::{PulsarBroker, PulsarSubscription};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connected = PulsarBroker::new("pulsar://localhost:6650")
        .connect()
        .await?;

    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::pattern("orders-.*", "audit"))
        .await?;
    let mut stream = pin!(subscriber.stream());
    while let Some(message) = stream.next().await {
        let message = message?;
        println!(
            "{}: {}",
            message.topic(),
            String::from_utf8_lossy(message.payload())
        );
        message.ack().await?;
    }

    connected.shutdown().await?;
    Ok(())
}
