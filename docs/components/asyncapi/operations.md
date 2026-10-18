---
sidebar_position: 3
title: Operations
description: "A receive operation reports its consumer under x-ruststream-pulsar: the subscription, its type, the ack timeout, topics or pattern."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A receive operation reports its consumer under `x-ruststream-pulsar`: the subscription, its type, the ack timeout, topics or pattern.

The extension carries what a reader of the document most wants to know about a consumer:

| Field | Value |
|---|---|
| `subscription` | the durable subscription the consumer joins |
| `subscriptionType` | `Exclusive`, `Shared`, `Failover` or `KeyShared` |
| `ackTimeoutMillis` | the [ack timeout](../settlement/ack-timeout.md), when the descriptor sets one |
| `topics` | every topic of a [`topics`](../subscriptions/topics.md) subscription |
| `topicPattern` | the regular expression of a [`pattern`](../subscriptions/pattern.md) subscription |

The values come from the descriptor alone, before the broker connects.
A subscription by bare topic name has no descriptor, so its operation carries no extension.

## Example

The operation `receive_persistent___acme_orders_created` reports the subscription `confirmations`, of the default type `Shared`, with a 30-second ack timeout.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
