---
sidebar_position: 5
title: SubscriptionType
description: "SubscriptionType says how the consumers of one subscription share it: Exclusive, Shared, Failover or KeyShared."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SubscriptionType

[`SubscriptionType`](api:ruststream_pulsar::SubscriptionType) says how the consumers of one subscription share it: `Exclusive`, `Shared`, `Failover` or `KeyShared`.

[`PulsarSubscription::subscription_type`](api:ruststream_pulsar::PulsarSubscription::subscription_type) sets it, and `Shared` is the default.
Each handler mounted on a subscription is one consumer of it, and so is each instance of the service.
A message reaches every subscription on its topic once, and the type decides which consumer of the subscription takes it:

| Type | Who takes a message |
| --- | --- |
| `Exclusive` | the one consumer of the subscription; a second consumer waits until the first leaves |
| `Shared` | any consumer, in turn |
| `Failover` | the active consumer; a standby takes over when it leaves |
| `KeyShared` | the consumer that holds the message's key, so one key is read in order |

The server refuses a second `Exclusive` consumer as busy, and the client asks again until its [operation retries](../broker/operation-retries.md) run out; with the default retries it asks for ever, so the second instance does not finish starting.
An `Exclusive` subscription belongs to a service that runs one instance.
Under the test harness that second consumer is refused at startup instead, so the test fails rather than hangs.
A message's key is its [partition key](../publishing/partition-key.md).
A retry cap needs `Shared` or `KeyShared`, because only there does the server count a message's redeliveries; a cap on the other two stops the service at startup, as [Dead letters](../settlement/dead-letters.md) explains.

[Subscription types and ordering](../../guides/subscription-types-and-ordering.md) reads a shipping service under each of the four types.

## Example

Two workers on the `Shared` subscription `workers`: each task runs once, on one of them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_subscriptions_type.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_subscriptions_type.rs#descriptor"
```

</TabItem>
</Tabs>
