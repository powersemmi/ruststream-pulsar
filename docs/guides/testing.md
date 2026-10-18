---
sidebar_position: 8
description: "Test a Pulsar service in process with TestApp: the production app, keyed payments, replies, delays, and the same test against a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a Pulsar service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process Pulsar](../components/testing/index.md) or against a running server.

The service under test charges card payments.
A payment arrives on `card-payments`, and its partition key is the account to charge.
The handler replies with a receipt on `card-receipts`.
A payment without a key is dropped.
A payment over the bank's instant limit is held: the handler asks for it again in a minute with [`retry_after`](../components/settlement/retry-after.md).
After three deliveries a payment goes to `card-payments-review`, the [dead-letter topic](../components/settlement/dead-letters.md).
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the production one.
The tests need the `testing` feature of `ruststream-pulsar` in `[dev-dependencies]`.

## A keyed payment

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`PulsarBroker`](../components/broker/index.md) to an in-process Pulsar.
The service URL is checked as the client would parse it, and never dialled.
The test addresses the broker by its production type, `tb.broker::<PulsarBroker>()`.
The partition key travels as the [`PARTITION_KEY_HEADER`](../components/publishing/keys.md) header, so the test sets it with `with_headers`.
The broker turns that header into the message's key, and the handler reads it back from the same header.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `card-receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## A payment without a key

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#keyless"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#keyless"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
`retry_after` holds the payment in the service for the delay and then negatively acknowledges it, so the broker delivers the same message again.
Nothing is published for the retry: the test's own payment is the only message on `card-payments`.
The third delivery is the last one the mount site allows, so after its wait the payment goes to `card-payments-review` as it arrived.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process Pulsar models

The in-process Pulsar reads the configuration a server would get: the broker's settings, each descriptor and the retry declaration of each registration.
It refuses what a server refuses, with the error the live broker returns: a service URL the client cannot parse, a destination that is not a topic name, a second consumer of an `Exclusive` subscription, and a handle used after shutdown.
It models Pulsar rather than a queue:

- A topic is its full name, so `orders` and `persistent://public/default/orders` are one topic.
- Each subscription gets every message of its topics once, and its [type](subscription-types-and-ordering.md) picks the consumer.
- A negatively acknowledged message goes back to its subscription, and the dead-letter policy counts its deliveries as the client does.
- Each topic keeps its log, so a [seek](replay.md) replays it; a non-persistent topic replays nothing.
- A pattern subscription reads a new topic from the client's next listing, 30 seconds of the paused clock later.
- Headers travel as text properties, and a payload over `max_message_size` fails the publish.

A server keeps a second `Exclusive` consumer waiting, while the in-process Pulsar refuses it at once, so such a test fails instead of hanging.
The redelivery after the acknowledgement timeout, partitioned topics, the key ranges of `KeyShared` and messages published while a subscription has no consumer need a server.

## The same test on a server

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to a running Pulsar.
The test reads the address from `PULSAR_TEST_URL` with `stand`, a helper of the example's own, and skips when it is unset.
`RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure, so a CI job cannot pass without the server.
The test joins a subscription of its own, named after the process.
A new subscription starts at the end of the topic, so nothing an earlier run left behind is read.
The assertions are the in-process ones, unchanged.

```bash
cargo test
PULSAR_TEST_URL=pulsar://localhost:6650 cargo test -- --test-threads=1
```

The server is the one from [Getting started](getting-started.md).
