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
- **The retry declaration becomes Pulsar's own policy.** `b.include(handle).max_attempts(nonzero!(5)).dead_letter("orders-dlq")` is the framework's spelling on every broker; here it becomes the consumer's dead-letter policy, so the client counts a message's redeliveries and produces the spent one to that topic. A `HandlerOutcome::retry()` is the negative acknowledgement that advances the count, and the ack timeout advances it without one. Nothing is republished by the service, so `out_retry(..)` over a Pulsar subscription is a compile error. A `retry_after(delay)` outcome is honoured by holding the delivery unacknowledged for the delay and negatively acknowledging it then, because Pulsar's negative acknowledgement carries no delay of its own; the wait is the process's, so it must be shorter than the subscription's `ack_timeout` and a process that exits mid-wait loses the wait rather than the message.
- **Validated addressing.** `PulsarTopic` parses and validates the four meanings a topic name carries (persistence, tenant, namespace, topic) on construction, not at first use.
- **Multi-topic and pattern subscriptions.** `PulsarSubscription::topics(["orders", "returns"], "workers")` subscribes to a fixed list; `::pattern("orders-.*", "audit")` follows every topic in the namespace whose name matches, including topics created after the consumer attached, from the client's next listing of the namespace.
- **Start position on the framework's own surface.** `PulsarPosition` (`earliest()`, `latest()`, `timestamp(ms)`, or a captured message id) is the `Seekable` capability's position type, so a subscription's start position is the `start_at(..)` clause; the descriptor itself carries no separate start options. A `start_at` seek runs on every startup, unlike Pulsar's server-side initial position, which applies only when a subscription is first created.
- **Repositioning from a handler, by key.** A delivery's context carries where it sits and the handle that moves the subscription, read as `Ctx<Position>` and `Ctx<SeekHandle>` parameters (or `ctx.context(..)`); a batch body reads the handle off the subscription-scoped `PulsarBatchContext`. Nothing is attached at the include site, and asking for a key the broker does not carry is a compile error rather than a runtime miss.
- **Batches, assembled on the client.** The Pulsar client has no consumer-side batch receive, so a `&[T]` batch handler is served from single deliveries: the mount site names the batch size with `.batch(nonzero!(n))`, the descriptor's `batch_wait` names how long a partial batch waits for the rest, and a batch never carries more than the size that was asked for. Nothing at the mount site or in the body says which side the batch was built on.
- **Key sharing as the partition key.** The partition key is the one per-message setting (`PulsarPublishOptions`), named at the call site by the `partition_key` step of the publish builder: `ledger.message(&receipt).to("receipts").partition_key("user-42").publish()`. The step keeps the publish on the slot the mount site wired, so the message leaves in that slot's codec and the test harness reads the key back off the slot. Keyed routing places the message by it, `KeyShared` subscriptions order by it, and a delivery reports it. The `partition-key` header carries the same key in the spelling every broker reads, for a body that writes its own headers.
- **The document describes the topology** (feature `asyncapi`). A subscription fills the specification's `pulsar` channel binding with its topic's namespace and persistence, and carries the consumer itself - the subscription name, the subscription type, the ack timeout - under `x-ruststream-pulsar`, because the specification's Pulsar operation object is empty. Every value is read off the descriptor before anything connects, so a subscription whose topics span two namespaces reports neither, and the credentials a service URL carries never reach a published document.
- **Properties carry headers directly.** Headers map onto Pulsar message properties with no extra envelope, so non-Rust peers see plain Pulsar messages.
- **Tests run the production app** (feature `testing`). `TestApp::start(app())` runs the app `main` runs with `PulsarBroker` connected in process, no server, and a test addresses it as `tb.broker::<PulsarBroker>()`. The in-process mode reads the broker's own settings and refuses what a server refuses, and it passes every framework suite this crate's capabilities justify - routing, lifecycle, seeking, batches - as the live broker does. Competing consumers on one `Shared` subscription split the stream in process instead of each replaying all of it. `TestApp::start_live(app())` runs the same test body against a running Pulsar.

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

`out_reply(policy)` binds the slot a handler's return value goes out on; `.out(marker, policy)` fills a slot the handler publishes through itself, marker first.

The one glob `ruststream_pulsar::prelude::*` carries the framework's prelude along with this crate's descriptors, contexts, seek keys and policy. The policy arrives under the uniform name `Publish`, so the include line reads the same whichever broker a service mounts, and the absence of a `TransactionalPublish` name is the statement that Pulsar's client has no transactions. A handler body imports `ruststream::prelude::*` instead and names framework things only, bounding an injected slot as `Out<impl Publisher>`.

## Test it

The app `main` runs, handed to the harness unchanged: `TestApp::start` connects `PulsarBroker` in
process, with no server, and the test addresses it by that type.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

// `publish` returns once the handlers it woke have settled.
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

The in-process mode reads the broker's own settings (the default subscription, the retry cap and its dead-letter topic, every descriptor setting) and refuses what a server refuses. A topic answers to either spelling of its name, a pattern subscription reads a topic created after it opened from the client's next listing thirty seconds on, and a seek moves the whole subscription. `TestApp::start_live(app())` runs the same test against a running Pulsar, which is where the acknowledgement timeout's own redelivery, partitioned topics and `KeyShared` hash ranges are exercised (`just test-brokers`).

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
