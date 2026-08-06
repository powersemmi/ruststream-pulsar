<h1 align="center">ruststream-pulsar</h1>

<p align="center">
  <i>The Apache Pulsar broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: four subscription types, consumer-side dead-letter policy, and validated topic addressing.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-pulsar/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-pulsar/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-pulsar` implements the RustStream broker contract over the [`pulsar`](https://crates.io/crates/pulsar) client maintained by StreamNative. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Lazy startup contract.** `PulsarBroker::new(url)` is synchronous and does no I/O (JWT auth and `pulsar+ssl://` as options); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`. The client reconnects consumers and producers transparently after broker restarts.
- **Subscription types as an enum.** Exclusive, shared, failover, and key-shared - with per-variant meaning, so combinations that do not exist are unrepresentable.
- **Server-side reliability.** The dead-letter policy (with its delivery-attempt limit) and the ack timeout are consumer settings the Pulsar server enforces, not behaviour emulated in this crate; `nack(requeue = true)` asks for redelivery and drives the delivery count towards the policy.
- **Validated addressing.** `PulsarTopic` parses and validates the four meanings a topic name carries (persistence, tenant, namespace, topic) on construction, not at first use.
- **Multi-topic and pattern subscriptions.** `PulsarSubscription::topics([...])` subscribes to a fixed list; `::pattern("orders-.*")` follows every topic in the namespace whose name matches, including topics created after the consumer attached.
- **Start position on the framework's own surface.** `PulsarPosition` (`earliest()`, `latest()`, `timestamp(ms)`, or a captured message id) is the `Seekable` capability's position type, so a subscription's start position is the `start_at(..)` clause and a live reposition is the `Seek` handler parameter; the descriptor itself carries no separate start options. A `start_at` seek runs on every startup, unlike Pulsar's server-side initial position, which applies only when a subscription is first created.
- **Key sharing as the partition key.** A `partition-key` header becomes the message's partition key on publish (keyed routing) and comes back as the same header, which `KeyShared` subscriptions order by.
- **Properties carry headers directly.** Headers map onto Pulsar message properties with no extra envelope, so non-Rust peers see plain Pulsar messages.
- **In-process test broker** (feature `testing`). `PulsarTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

Transactions, consumer-side batch receive, and the schema registry are out of scope for the first release: the client does not implement them, and the capability traits they would back are optional.

## Status

Implemented and verified against Apache Pulsar standalone (the framework's conformance lifecycle suite and the integration tests, including dead-letter routing, run in CI against it). Published on crates.io as `ruststream-pulsar = "0.6"`, built on the `ruststream` 0.6 line. Design and scope are tracked in [powersemmi/ruststream#190](https://github.com/powersemmi/ruststream/issues/190).

Building requires `protoc` on the path (the client compiles the Pulsar protocol definitions).

## Write a service

```rust
use std::time::Duration;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_pulsar::{DeadLetter, PulsarBroker, PulsarSubscription, SubscriptionType};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
        .dead_letter(DeadLetter::new("orders-dlq").max_deliveries(5))
        .ack_timeout(Duration::from_secs(30))
)]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(PulsarBroker::new("pulsar://localhost:6650"), |b| b.include(handle))
}
```

## Test it

The `testing` feature runs handlers against an in-process Pulsar stand-in - no server, same routing. Pulsar's own behaviour (subscription types, dead-lettering, ack timeouts, redelivery) is covered by the env-gated live suite instead: `just test-brokers` starts Pulsar standalone and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-pulsar/
├── crates/
│   └── ruststream-pulsar/      the published crate
│       └── examples/           runnable pulsar_* examples
├── docker-compose.test.yml     Pulsar standalone for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against Pulsar standalone
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
