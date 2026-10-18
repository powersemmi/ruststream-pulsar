---
sidebar_position: 2
title: topics
description: "PulsarSubscription::topics reads a fixed list of topics under one subscription; one seek or one retry policy covers them all."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# topics

[`PulsarSubscription::topics`](api:ruststream_pulsar::PulsarSubscription::topics) reads a fixed list of topics under one subscription; one seek or one retry policy covers them all.

The first argument is the list, and each topic in it takes any of the three spellings a [single topic](./pulsar-subscription.md) takes.
The subscription keeps a cursor on every topic of the list, and its consumers read all of them.
A list has no single topic to name the subscription by, so it reports under the subscription's own name: a test reads its deliveries under that name, and the [AsyncAPI document](../asyncapi/channels.md) reports that channel.
A [seek](../positions/seek.md) from a handler moves the cursor on every topic of the list.
The registration's [dead-letter topic](../settlement/dead-letters.md) is one for the whole list.
The settings and their defaults are those of [`PulsarSubscription`](./pulsar-subscription.md).
An empty list, or a name in it that Pulsar would refuse, stops the service at startup.

[Topics and subscriptions](../../guides/topics-and-subscriptions.md) totals the orders of two regional topics under one subscription.

## Example

The European and the American orders, fulfilled under one subscription.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_subscriptions_topics.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_subscriptions_topics.rs#descriptor"
```

</TabItem>
</Tabs>
