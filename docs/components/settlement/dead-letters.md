---
sidebar_position: 3
title: Dead letters
description: "max_attempts with dead_letter becomes the consumer's own dead-letter policy: the client moves a spent message, and the service publishes nothing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) with [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) becomes the consumer's own dead-letter policy: the client moves a spent message, and the service publishes nothing.

`max_attempts(n)` is how many times one message reaches the handler, the first delivery included.
`dead_letter(topic)` is where it goes once those are spent.
The Pulsar client counts the redeliveries, produces the spent message to that topic and acknowledges the original.
A `retry`, a [`retry_after`](./retry-after.md) and an expired [`ack_timeout`](./ack-timeout.md) each advance the count.
The two steps go together: a registration that writes one without the other refuses to start, and the error names the missing step.
The broker counts redeliveries only where consumers compete for messages, so the cap belongs to a `Shared` or a `KeyShared` [subscription](../subscriptions/subscription-type.md).
A registration that declares a cap over `Exclusive` or `Failover` refuses to start, and the error names the type.
A handler mounted by a bare topic name declares the same pair, and one policy covers every topic of a [multi-topic](../subscriptions/topics.md) subscription.
The client keeps the count to itself, so a handler cannot read how many deliveries a message has had.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_settlement_dead_letters.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_settlement_dead_letters.rs#app"
```

</TabItem>
</Tabs>
