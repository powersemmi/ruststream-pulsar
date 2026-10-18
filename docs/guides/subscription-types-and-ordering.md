---
sidebar_position: 3
description: "Choose how the consumers of a Pulsar subscription share it, keep the order of each key, and place keyed messages on partitions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscription types and ordering

This guide chooses how the consumers of a Pulsar subscription share it, keeps the order of each key, and places keyed messages on partitions.

The example is a shipping service.
It packs orders, tracks shipments, books payments and invoices orders, and each of these reads under a different subscription type.

## The four types

Every consumer of a subscription is a handler in one of the service's instances.
The [subscription type](https://pulsar.apache.org/docs/next/concepts-messaging/#subscription-types) decides how those consumers share the messages:

| Type | Who gets a message | Order |
|---|---|---|
| `Shared` *(default)* | any consumer, in turn | none |
| `KeyShared` | the consumer that holds the message's key | per key |
| `Exclusive` | the one consumer; a second one is refused | the topic's order |
| `Failover` | the active consumer; the others stand by | the topic's order |

[`subscription_type`](../components/subscriptions/subscription-type.md) on the descriptor sets the type.
Every consumer of one subscription names the same type.

## Shared: any consumer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#shared"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#shared"
```

</TabItem>
</Tabs>

`Shared` hands each order to one consumer of `packing`, whichever is next.
More instances of the service pack more orders at once, and two orders may finish in any order.
That suits work whose messages do not depend on each other.

`pack` publishes a shipment event for each order.
The [`partition_key`](../components/publishing/partition-key.md) step keys the event by the order's id.
The step comes from the broker's prelude, so the slot is bounded `Publisher<Options = PulsarPublishOptions>` to offer it.
A [publish](../components/publishing/index.md) returns once the broker has stored the message.

## KeyShared: in order per key

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#key_shared"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#key_shared"
```

</TabItem>
</Tabs>

`KeyShared` gives all the messages of one key to one consumer, so the events of one order reach `track` in the order `pack` published them.
Different keys spread over the consumers, so the instances share the work.
The server assigns the keys and moves some of them when a consumer joins or leaves.

Inside one instance, `workers(8, by_key)` runs eight lanes, and the deliveries of one key run one after another on one lane.
The lane key is the message's partition key, so the order of each key holds within the process too.
In the manual form the same setting is [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).
The key reaches the handler as the [`partition-key` header](../components/publishing/keys.md).

## Exclusive: one consumer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#exclusive"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#exclusive"
```

</TabItem>
</Tabs>

`Exclusive` lets one consumer read the subscription, so `book` sees the payments in the order they were taken.
The server refuses a second consumer as busy, and the client keeps asking until the first one leaves.
So a second instance of the service does not start, and reports nothing while it waits.
An `Exclusive` subscription belongs to a service that runs one instance.

## Failover: one active consumer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#failover"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#failover"
```

</TabItem>
</Tabs>

`Failover` accepts every consumer but delivers to one of them.
The others are hot standbys: when the active one disconnects, the server hands its messages to the next.
Several instances keep the order of an `Exclusive` subscription without its single point of failure.
On a partitioned topic each partition has its own active consumer, so the instances share the partitions.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#app"
```

</TabItem>
</Tabs>

[`OperationRetries::attempts`](../components/broker/operation-retries.md) bounds how often the client asks again for an operation the server has not accepted.
With three attempts, a second consumer of `ledger` stops the service at startup with an error that names the topic.
The bound covers every operation of the connection, the reconnect after a broker restart included, so it trades waiting out an outage for an early error.

## Partitioned topics

A [partitioned topic](https://pulsar.apache.org/docs/next/concepts-messaging/#partitioned-topics) is several topics under one name, and each partition is served on its own.
An administrator creates it with its number of partitions:

```bash
docker compose exec pulsar bin/pulsar-admin topics create-partitioned-topic \
  persistent://public/default/shipments -p 3
```

A subscription reads every partition of the topic, and a service reads it with the same descriptor as any other topic.
A keyed message goes to the partition its key hashes to, so the messages of one key stay in one partition and in order.
Unkeyed messages go to the partitions in turn.

## Testing the types

The tests in the example file run the app in process.
There, each subscription gets every message of its topic once, and its type picks the consumer.
The in-process Pulsar refuses a second `Exclusive` consumer at once, so the test fails instead of waiting.
Partitions, the key ranges of `KeyShared` and the consumer a server picks are the server's, and the crate's live tests check them against a running Pulsar.
