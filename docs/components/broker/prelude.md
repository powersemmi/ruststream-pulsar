---
sidebar_position: 6
title: Prelude
description: "One glob, ruststream_pulsar::prelude, brings the core prelude, the broker, its descriptor, positions, context keys and Publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_pulsar::prelude`](api:ruststream_pulsar::prelude), brings the core prelude, the broker, its descriptor, positions, context keys and `Publish`.

`use ruststream_pulsar::prelude::*;` is the one import a Pulsar routes file writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`PulsarBroker`](./index.md) and [`OperationRetries`](./operation-retries.md), the descriptor [`PulsarSubscription`](../subscriptions/pulsar-subscription.md) with [`SubscriptionType`](../subscriptions/subscription-type.md) and [`PulsarTopic`](../subscriptions/pulsar-topic.md), the positions [`PulsarPosition`](../positions/index.md) and [`PulsarSeeker`](../positions/seek.md), and the [context](../context/index.md) types with their keys `Position` and `SeekHandle`.
The core traits `Positioned` and `Seeker` come with them.

The publish policy comes under the name every broker crate uses: `Publish` is [`PulsarPublish`](../publishing/index.md).
So a mount site reads the same on every broker.
Two broker preludes in one file collide on this name; the prefixed type at the crate root tells them apart.

A handler body bounds its slots with capability traits, such as `Out<impl Publisher>`, and names no broker type.
The one exception is a body that keys a publish: [`partition_key`](../publishing/partition-key.md) needs `PulsarPublishSteps` in scope and the slot bounded `Out<impl Publisher<Options = PulsarPublishOptions>, Marker>`, both from this prelude.

## Example

The descriptor, its subscription type, the broker's retries and the reply policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
