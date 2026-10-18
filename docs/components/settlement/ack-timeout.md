---
sidebar_position: 1
title: ack_timeout
description: "ack_timeout redelivers a message left unacknowledged longer than the timeout; it also bounds the delay a retry_after may ask for."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ack_timeout

[`ack_timeout`](api:ruststream_pulsar::PulsarSubscription::ack_timeout) redelivers a message left unacknowledged longer than the timeout; it also bounds the delay a [`retry_after`](./retry-after.md) may ask for.

It is a setting of the [`PulsarSubscription`](../subscriptions/pulsar-subscription.md) descriptor and has no default: without it, an unsettled message comes back only when its consumer disconnects.
With it, the client asks the broker for every message its handler has held longer than the timeout, whatever the reason: a handler that hangs, or a service that stopped mid-delivery.
Each such redelivery counts toward the [dead-letter](./dead-letters.md) cap, as a `retry` does.
A `retry_after` delay that is not shorter than the timeout is refused when the handler returns it, with an error naming both values, because the timeout would bring the message back first.
A handler keeps its backoff below the timeout for that reason.
The in-process transport of a [`TestApp`](api:ruststream::testing::TestApp) test applies the bound on `retry_after`; the redelivery itself happens on a broker.

[Acknowledgement](../../guides/acknowledgement.md) bounds an unsettled delivery of an inventory service with an acknowledgement timeout.

## Example

An export the archive refused waits thirty seconds, inside the one-minute ack timeout of its subscription.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_settlement_ack_timeout.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_settlement_ack_timeout.rs#descriptor"
```

</TabItem>
</Tabs>
