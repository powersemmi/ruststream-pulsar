---
title: Acknowledgement
description: "ack acknowledges a Pulsar delivery, retry negatively acknowledges it, and drop acknowledges it too: Pulsar has no terminal reject."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

[`ack`](api:ruststream::runtime::HandlerOutcome::ack) acknowledges a Pulsar delivery, [`retry`](api:ruststream::runtime::HandlerOutcome::retry) negatively acknowledges it, and [`drop`](api:ruststream::runtime::HandlerOutcome::drop) acknowledges it too: Pulsar has no terminal reject.

The handler's outcome settles each delivery on its subscription.
An acknowledged message moves the subscription's cursor past it, and the subscription does not deliver it again.
A negative acknowledgement hands the message back to the subscription, not to the consumer that gave up on it: on a `Shared` subscription another consumer may take it.
A message that should leave the subscription for good goes through a [dead-letter policy](./dead-letters.md), which `drop` does not need.
The client queues acknowledgements and sends them in the background, so a settlement that returns is queued on the consumer, not yet confirmed by the broker.
A message nobody settles comes back after the subscription's [`ack_timeout`](./ack-timeout.md).
[`retry_after`](./retry-after.md) asks for the delivery back after a delay.

[Acknowledgement](../../guides/acknowledgement.md) settles the stock reservations of an inventory service by the handler's outcome.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_settlement_ack.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_settlement_ack.rs#handler"
```

</TabItem>
</Tabs>
