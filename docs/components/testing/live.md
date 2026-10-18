---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a running Pulsar, for what only a server has."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a running Pulsar, for what only a server has.

It connects the broker through its ordinary connect, so the client dials the server and every subscription opens a real consumer.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process mode leaves to the server:

- the [ack timeout's](../settlement/ack-timeout.md) own redelivery;
- partitioned topics, and the partition a [key](../publishing/keys.md) picks;
- which consumer of a `Shared` or `KeyShared` subscription the server picks, and `KeyShared` hash ranges;
- messages published while a subscription has no consumer;
- a second `Exclusive` consumer waiting, and the [`OperationRetries`](../broker/operation-retries.md) that bound the wait;
- the [token and TLS](../broker/security.md);
- a backlog older than the test, which [`start_at`](../positions/start-at.md) replays.

A server keeps what earlier runs left in it, and a subscription is a durable cursor on it.
So each run reads in a subscription of its own, which starts at the end of the topic, past what earlier runs published.

In the example, `stand` returns the server's URL from `PULSAR_TEST_URL`, and the live test returns early without one, so `cargo test` passes on a machine with no server.
A job that stood a server up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing URL then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, a Pulsar standalone on `127.0.0.1:6650` that `just brokers-up` starts.

[Testing a Pulsar service](../../guides/testing.md) runs the same test against a server.

## Example

The two tests share one body: one in process, one against the server at `PULSAR_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_testing_live.rs#both"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_testing_live.rs#both"
```

</TabItem>
</Tabs>
