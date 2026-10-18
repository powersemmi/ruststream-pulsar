---
sidebar_position: 1
title: PulsarSubscription
description: "PulsarSubscription::new joins a named durable subscription on one topic: a bare name, tenant/namespace/topic, or a full name."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PulsarSubscription

[`PulsarSubscription::new`](api:ruststream_pulsar::PulsarSubscription::new) joins a named durable subscription on one topic: a bare name, `tenant/namespace/topic`, or a full name.

The first argument is the topic, and the second is the subscription.
A bare name is a topic of `persistent://public/default`, a triple is a persistent topic of that tenant and namespace, and a full name such as `non-persistent://acme/sales/pings` says everything itself.
The subscription is the cursor the server keeps for its readers.
Every consumer that joins the same subscription shares one backlog, and a second subscription on the same topic reads every message again, from a cursor of its own.
[`subscription()`](api:ruststream_pulsar::PulsarSubscription::subscription) returns the name.

Every setting has a default, so the two names are all a descriptor needs:

| Setting | What it does | Default |
| --- | --- | --- |
| [`subscription_type`](./subscription-type.md) | how the consumers of the subscription share it | `Shared` |
| [`ack_timeout`](../settlement/ack-timeout.md) | redelivers what a handler left unsettled longer than this | none |
| [`batch_wait`](./batches.md) | how long a batch that is not full waits for more deliveries | 10 ms |

The subscription reports under its topic, as written: a test reads its deliveries under that name, and the [AsyncAPI document](../asyncapi/channels.md) reports that channel.
A topic name Pulsar would refuse stops the service at startup with an error that names it, before any call to the server; [`PulsarTopic`](./pulsar-topic.md) is the same check as a type.

## Example

Billing and shipping read every order of `acme/sales/orders`, each under a subscription of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_subscriptions_descriptor.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_subscriptions_descriptor.rs#descriptor"
```

</TabItem>
</Tabs>
