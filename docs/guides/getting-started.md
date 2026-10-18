---
sidebar_position: 1
description: "Install ruststream-pulsar, start a local Pulsar, run a first subscriber against it and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-pulsar`, starts a local Pulsar and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-pulsar = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-pulsar` is the Pulsar broker.
Its `testing` feature adds the [in-process Pulsar](../components/testing/index.md) that tests run on.

The broker crate has these features, both off by default:

| Feature | What it gives |
|---|---|
| `testing` | the in-process Pulsar behind `TestApp` |
| `asyncapi` | the [Pulsar bindings](../components/asyncapi/index.md) in the generated AsyncAPI document |

## System requirements

The Pulsar client compiles the Pulsar protocol definitions during the build, so the build needs `protoc`, the Protocol Buffers compiler.
On Debian and Ubuntu it comes with `protobuf-compiler`, and on macOS with `brew install protobuf`.
A `protoc` outside the path is named by the `PROTOC` environment variable.

## A local Pulsar

Pulsar in standalone mode runs a broker and its storage in one container:

```yaml title="compose.yaml"
services:
  pulsar:
    image: apachepulsar/pulsar:4.1.2
    command: bin/pulsar standalone
    ports:
      - "6650:6650"
      - "8080:8080"
    environment:
      # The address the broker hands to clients outside the container.
      PULSAR_PREFIX_advertisedAddress: 127.0.0.1
      PULSAR_PREFIX_advertisedListeners: pulsar://127.0.0.1:6650
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080/admin/v2/brokers/health || exit 1"]
      interval: 10s
      retries: 30
```

Start it and wait until it is healthy:

```bash
docker compose up -d --wait
```

The broker listens on `localhost:6650`, and the admin API on `localhost:8080`.
Pulsar creates a topic on its first use, so the service needs no setup.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the `orders` topic.
It acknowledges an order with an amount and drops one without.
Pulsar reads a topic only through a named subscription.
[`PulsarSubscription::new`](../components/subscriptions/pulsar-subscription.md) names the topic and the subscription, here `billing`.
The subscription is a cursor the server keeps, so the service resumes where it stopped after a restart.
[`PulsarBroker::new`](../components/broker/index.md) takes the service URL and only records it.
The connection opens when the service starts.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish an order from another terminal with the client that ships in the image:

```bash
docker compose exec pulsar bin/pulsar-client produce orders \
  -s ';' -m '{"id":1,"amount_cents":1250}'
```

`-s ';'` keeps the message whole: by default the client splits it at every comma.
The service logs `order taken` with the order's id.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`PulsarBroker` connects to an in-process Pulsar there, so the tests need no server:

```bash
cargo test
```

[Testing a Pulsar service](testing.md) walks through these tests and what the in-process Pulsar models.

## Coming from the pulsar crate

A service on the [`pulsar`](https://docs.rs/pulsar) crate builds its own client, consumer loop and producers.
Here is where each part goes in RustStream:

| `pulsar` crate | RustStream |
|---|---|
| `Pulsar::builder(url, TokioExecutor).build().await` | [`PulsarBroker::new(url)`](../components/broker/index.md), connected by the runtime |
| `.with_auth(Authentication { name: "token", .. })` | [`PulsarBroker::token`](../components/broker/security.md) |
| `.with_operation_retry_options(..)` | [`PulsarBroker::operation_retries`](../components/broker/operation-retries.md) |
| `client.consumer().with_topic(..).with_subscription(..)` and a loop over the consumer | a `#[subscriber]` handler on a [`PulsarSubscription`](../components/subscriptions/pulsar-subscription.md) |
| `.with_subscription_type(..)` | [`subscription_type`](../components/subscriptions/subscription-type.md) on the descriptor |
| `.with_topics([..])`, `.with_topic_regex(..)` | [`PulsarSubscription::topics`](../components/subscriptions/topics.md), [`PulsarSubscription::pattern`](../components/subscriptions/pattern.md) |
| deserializing `msg.payload.data` | the handler's parameter type, decoded by the codec |
| `consumer.ack(&msg)`, `consumer.nack(&msg)` | the handler's outcome ([Acknowledgement](acknowledgement.md)) |
| `.with_dead_letter_policy(..)` | `max_attempts(n)` and `dead_letter(topic)` at the mount site ([Retries and dead letters](retries.md)) |
| `.with_unacked_message_resend_delay(..)` | [`ack_timeout`](../components/settlement/ack-timeout.md) on the descriptor |
| `consumer.seek(..)` | [`start_at`](../components/positions/start-at.md) at the mount site, or the [seek handle](../components/positions/seek.md) in the handler ([Replay and seeking](replay.md)) |
| `client.producer().with_topic(..).build()` and `send_non_blocking` | a reply or an `Out` parameter, sent through the [publish policy](../components/publishing/index.md) ([Publishing and replies](publishing.md)) |
| `producer::Message { partition_key, properties, .. }` | the [`partition_key`](../components/publishing/partition-key.md) step; [headers](../components/publishing/headers.md) become properties |
