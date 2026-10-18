---
sidebar_position: 2
title: retry_after
description: "retry_after holds the delivery unacknowledged in the service for the delay, then negatively acknowledges it; nothing is republished."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) holds the delivery unacknowledged in the service for the delay, then negatively acknowledges it; nothing is republished.

A Pulsar negative acknowledgement carries no delay of its own, so the wait happens in the service.
When it is over, the subscription delivers the same message again, with its own payload, properties and position.
The redelivery is an ordinary one, so a [dead-letter policy](./dead-letters.md) counts it like any other.
The wait runs on the runtime the broker connected on.
A process that exits during the wait loses the wait, not the message: the broker delivers it again once the [`ack_timeout`](./ack-timeout.md) elapses, or at once when the consumer disconnects.
A delay that is not shorter than the subscription's `ack_timeout` is refused when the handler returns it; a subscription with no `ack_timeout` accepts any delay.
A test waits the delay out with [`tb.advance`](api:ruststream::testing::TestApp::advance), in process on a paused clock and against a [live](../testing/live.md) broker in real time.

[Retries and dead letters](../../guides/retries.md) holds a refund back while the card processor's rate limit is used up.

## Example

An invoice whose customer has not reached the directory yet comes back after the replication lag.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_settlement_retry_after.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_settlement_retry_after.rs#handler"
```

</TabItem>
</Tabs>
