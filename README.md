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

`ruststream-pulsar` implements the RustStream broker contract over the [`pulsar`](https://crates.io/crates/pulsar) client maintained by StreamNative. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Lazy startup contract.** `PulsarBroker::new(url)` is synchronous and does no I/O (JWT auth and `pulsar+ssl://` as options); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`. The client reconnects consumers and producers transparently after broker restarts.
- **Subscription types as an enum.** Exclusive, shared, failover, and key-shared - with per-variant meaning, so combinations that do not exist are unrepresentable.
- **Server-side reliability.** The dead-letter policy (with its delivery-attempt limit) and the ack timeout are consumer settings the Pulsar server enforces, not behaviour emulated in this crate; a `HandlerOutcome::retry()` negatively acknowledges the message, asking for redelivery and advancing the delivery count towards the policy's limit.
- **Validated addressing.** `PulsarTopic` parses and validates the four meanings a topic name carries (persistence, tenant, namespace, topic) on construction, not at first use.
- **Multi-topic and pattern subscriptions.** `PulsarSubscription::topics(["orders", "returns"], "workers")` subscribes to a fixed list; `::pattern("orders-.*", "audit")` follows every topic in the namespace whose name matches, including topics created after the consumer attached.
- **Start position on the framework's own surface.** `PulsarPosition` (`earliest()`, `latest()`, `timestamp(ms)`, or a captured message id) is the `Seekable` capability's position type, so a subscription's start position is the `start_at(..)` clause; the descriptor itself carries no separate start options. A `start_at` seek runs on every startup, unlike Pulsar's server-side initial position, which applies only when a subscription is first created.
- **Repositioning from a handler, by key.** A delivery's context carries where it sits and the handle that moves the subscription, read as `Ctx<Position>` and `Ctx<SeekHandle>` parameters (or `ctx.context(..)`); a batch body reads the handle off the subscription-scoped `PulsarBatchContext`. Nothing is attached at the include site, and asking for a key the broker does not carry is a compile error rather than a runtime miss.
- **Batches, assembled on the client.** The Pulsar client has no consumer-side batch receive, so a `&[T]` batch handler is served from single deliveries: the mount site names the batch size with `.batch(nonzero!(n))`, the descriptor's `batch_wait` names how long a partial batch waits for the rest, and a batch never carries more than the size that was asked for. Nothing at the mount site or in the body says which side the batch was built on.
- **Key sharing as the partition key.** A `partition-key` header becomes the message's partition key on publish (keyed routing) and comes back as the same header, which `KeyShared` subscriptions order by. `PulsarPublishExt::with_partition_key` sets it as a per-message publish argument.
- **Properties carry headers directly.** Headers map onto Pulsar message properties with no extra envelope, so non-Rust peers see plain Pulsar messages.
- **In-process test broker** (feature `testing`). `PulsarTestBroker` reproduces core routing with no server, a service mounts on it and runs under the `TestApp` harness, and it answers the way a real Pulsar does, which the crate's own tests hold it to.

Transactions and the schema registry are out of scope: the client does not implement them, and the capability traits they would back are optional.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-pulsar = { version = "0.7", features = ["testing"] }
```

Building requires `protoc` on the path (the client compiles the Pulsar protocol definitions).

## Write a service

```rust
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
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
        .dead_letter(DeadLetter::new("orders-dlq").max_deliveries(5))
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
            b.include(confirm).out(Reply, Publish);
        },
    )
}
```

`.out(marker, policy)` fills a publishing slot, marker first; `Reply` is the slot the handler's return value goes out on.

The one glob `ruststream_pulsar::prelude::*` carries the framework's prelude along with this crate's descriptors, contexts, seek keys and policy. The policy arrives under the uniform name `Publish`, so the include line reads the same whichever broker a service mounts, and the absence of a `TransactionalPublish` name is the statement that Pulsar's client has no transactions. A handler body imports `ruststream::prelude::*` instead and names framework things only, bounding an injected slot as `Out<impl Publisher>`.

## Test it

The `testing` feature ships an in-process Pulsar stand-in - no server, same routing, same ladder - and the `TestApp` harness drives a whole service against it. The stand-in subscribes by topic name, so the body under test is declared on its topic rather than on a `PulsarSubscription` descriptor; what the descriptor configures is Pulsar server behaviour, which the live suite covers anyway.

```rust
use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;

// The same body, on a topic-name subscriber. Mounted plainly, so the reply leaves through the
// broker's default publish policy and the mount site names no publisher on either broker.
#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(PulsarTestBroker::new(), |b| {
        b.include(confirm);
    });
let tb = TestApp::start(app).await?;

// The publish drives the handler to a standstill before returning.
tb.broker::<PulsarTestBroker>()
    .publish("orders", &Order { id: 42 })
    .await?;

tb.broker::<PulsarTestBroker>()
    .subscriber("orders")
    .assert_called_once()
    .settled(HandlerOutcome::ack());

tb.broker::<PulsarTestBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { id: 42 });
```

Pulsar's own behaviour (subscription types, dead-lettering, ack timeouts, redelivery, seeking) is covered by the env-gated live suite instead: `just test-brokers` starts Pulsar standalone and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-pulsar/
├── crates/
│   └── ruststream-pulsar/      the published crate
│       └── examples/           runnable pulsar_* examples
├── docs/                       the documentation site
├── docker-compose.test.yml     Pulsar standalone for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # the suite that needs no server (the live tests skip themselves)
just test-brokers   # live integration + conformance against Pulsar standalone
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
