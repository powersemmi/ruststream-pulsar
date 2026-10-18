---
sidebar_position: 6
description: "Read a Pulsar topic's retained log again: from the start on every startup, or from a position a handler seeks to."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replay and seeking

This guide reads a Pulsar topic's retained log again: from the start on every startup, or from a position a handler seeks to.

A subscription is a cursor on the topic's log, and the server keeps it.
A seek moves that cursor, and the subscription reads on from its new place.

The example is a search service.
It keeps account balances built from a ledger, and a search index of orders.
`Search` stands in for the client of the search cluster.

## Positions

[`PulsarPosition`](../components/positions/index.md) names a place in a topic's log:

| Position | Where |
|---|---|
| `PulsarPosition::earliest()` | the oldest retained message |
| `PulsarPosition::latest()` | the end of the log: only messages published after the seek |
| `PulsarPosition::timestamp(millis)` | the first message published at or after that time, in milliseconds since the Unix epoch |
| `PulsarPosition::MessageId(..)` | a delivered message, read from its [`Position`](../components/context/index.md) |

## Every startup from the start

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#start_at"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#start_at"
```

</TabItem>
</Tabs>

[`start_at`](../components/positions/start-at.md) seeks the subscription each time the service starts.
`balances` reads the ledger from its oldest retained entry on every start, so the balances are rebuilt from scratch.
Without `start_at`, an existing subscription resumes where its consumers stopped, and a new one starts at the end of the topic.

Pulsar has an initial position of its own, but it applies only when a subscription is created.
`start_at` applies to the subscription that already exists too.
Every instance that starts moves the shared cursor, so the instances of a service with `start_at` replay the log at each start of any of them.

## A seek from the handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#seek"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#seek"
```

</TabItem>
</Tabs>

[`SeekHandle`](../components/positions/seek.md) is the subscription's own seeker, and `index` reads it from the [Pulsar context](../components/context/index.md).
When the search cluster loses its index, `index` seeks to the oldest retained order, and every order is indexed again.
In the manual form the handler names `PulsarContext` as its context and reads the seeker with `ctx.context(SeekHandle)`.

A seek moves the whole subscription: every consumer of it, every topic it reads and every partition.
Deliveries already queued for the subscription are discarded, and the log is read from the new place.

Two other seeks come up often:

- `PulsarPosition::timestamp(millis)` skips a stretch of bad data: the subscription resumes at the first message published after it.
- A delivery's position, read with `Ctx<Position>`, delivers exactly that message again, then the rest of the log in order.

A [batch handler](../components/context/batch-context.md) seeks the same way through `PulsarBatchContext`, which carries the seeker and no position, because a batch spans many messages.

## What a replay reaches

A replay reads what the topic still holds.
By default Pulsar deletes a message once every subscription has acknowledged it.
A [retention policy](https://pulsar.apache.org/docs/next/cookbooks-retention-expiry/) on the namespace keeps acknowledged messages for a size or a time:

```bash
docker compose exec pulsar bin/pulsar-admin namespaces set-retention public/default \
  --size 1G --time 3d
```

A non-persistent topic keeps nothing, so a seek there replays nothing.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#app"
```

</TabItem>
</Tabs>

`service` builds the app on the broker it is given, so a test can take a publisher off that broker before the app starts.

## Testing a seek

The tests in the example file run the app in process.
The in-process Pulsar keeps each topic's log, so a seek there replays it as a server does.
The test publishes three orders in one burst through `broker.publisher()`, so all three wait in the subscription before the first one is handled.
`tb.settle()` then waits until the subscription is idle.
A backlog left by an earlier run of the service exists only on a server, so `start_at` over such a backlog is checked by the crate's live tests.
