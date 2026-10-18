---
sidebar_position: 5
title: Shutdown
description: "Shutdown closes the producers the crate opened, and a publisher or a seek used afterwards reports PulsarError::NotConnected."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown closes the producers the crate opened, and a publisher or a seek used afterwards reports [`PulsarError::NotConnected`](api:ruststream_pulsar::PulsarError::NotConnected).

The crate opens one producer per topic the service publishes to, on the first publish there.
At shutdown it closes each of them, so the server releases them at once.
A producer that fails to close is logged and does not fail the shutdown.
The client's connection itself ends with the client.
The shutdown therefore returns `()`, with no report of its own.

A publisher taken before the shutdown keeps pointing at the closed connection.
That includes one taken off the broker before startup with [`PulsarBroker::publisher`](api:ruststream_pulsar::PulsarBroker::publisher).
A publish through it returns `PulsarError::NotConnected` and never succeeds against the dead connection.
A [seek](../positions/seek.md) through a handle that outlived the connection is refused the same way.

## Example

The service replies to each order with a shipment, which opens a producer on `shipments`; shutdown closes it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
