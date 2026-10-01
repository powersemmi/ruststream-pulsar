<h1 align="center">ruststream-pulsar</h1>

<p align="center">
  <i>The Apache Pulsar broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: four subscription types, consumer-side dead-letter policy, and validated topic addressing.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-pulsar/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-pulsar/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-pulsar"><img src="https://img.shields.io/crates/v/ruststream-pulsar.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-pulsar"><img src="https://img.shields.io/crates/dr/ruststream-pulsar" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-pulsar"><img src="https://img.shields.io/docsrs/ruststream-pulsar" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-pulsar/">Documentation</a></b>
</p>

---

`ruststream-pulsar` connects a RustStream service to Apache Pulsar over the
[`pulsar`](https://crates.io/crates/pulsar) client maintained by StreamNative. Handlers, routing,
codecs and middleware come from the framework; this crate is the transport.

## Features

- **Subscription types as an enum:** exclusive, shared, failover and key-shared.
- **Retry caps as Pulsar's own dead-letter policy,** so the client counts redeliveries and moves a
  spent message itself.
- **Validated topic names,** checked when the descriptor is built.
- **Multi-topic and pattern subscriptions,** including topics created later.
- **Start positions and repositioning:** `start_at(..)` on every startup, and a handler moves its
  subscription by message id or timestamp.
- **Batches** assembled on the client.
- **Key sharing:** the partition key is a per-message setting that keyed routing and `KeyShared`
  subscriptions order by.
- **Plain Pulsar messages:** headers ride message properties.
- **AsyncAPI** with the specification's `pulsar` binding, behind the `asyncapi` feature.
- **Tests without a server:** handlers run against an in-process Pulsar.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-pulsar = { version = "0.7", features = ["testing"] }
```

Building requires `protoc` on the path.

## Write a service

```rust
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, PartialEq, Serialize, Deserialize)]
struct Confirmation {
    id: u64,
}

#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
        .ack_timeout(Duration::from_secs(30)),
    publish("confirmations")
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(confirm)
                .out_reply(Publish)
                .max_attempts(nonzero!(5))
                .dead_letter("orders-dlq");
        },
    )
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

## Test it

`TestApp` runs the service's own app with `PulsarBroker` in process, with no server.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<PulsarBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<PulsarBroker>()
    .subscriber("orders")
    .assert_called_once()
    .settled(HandlerOutcome::ack());

tb.broker::<PulsarBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { id: 42 });
```

## Documentation

- This crate: <https://docs.rs/ruststream-pulsar>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
