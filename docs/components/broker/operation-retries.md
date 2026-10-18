---
sidebar_position: 2
title: OperationRetries
description: "OperationRetries bounds how long the client keeps retrying an operation a server has not accepted, which is unbounded by default."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# OperationRetries

[`OperationRetries`](api:ruststream_pulsar::OperationRetries) bounds how long the client keeps retrying an operation a server has not accepted, which is unbounded by default.

A server sometimes answers "not now": a broker is still starting, a topic is moving between brokers, a subscription is taken.
The client then tries again, by default for ever and five seconds apart.
That is what waits out a broker restart.

[`attempts(n)`](api:ruststream_pulsar::OperationRetries::attempts) gives up after `n` tries and reports what the server answered.
One try makes the first answer final.
[`delay`](api:ruststream_pulsar::OperationRetries::delay) sets the wait between tries, five seconds by default, and [`timeout`](api:ruststream_pulsar::OperationRetries::timeout) the time one try has to be answered, thirty seconds by default.
[`PulsarBroker::operation_retries`](api:ruststream_pulsar::PulsarBroker::operation_retries) applies the bound.

The bound covers every retried operation of the connection: topic lookups, opening producers and consumers, and a consumer's reconnect after a disconnect.
A short bound turns a long outage into a subscription that gives up.

The case that needs a bound is an [`Exclusive`](../subscriptions/subscription-type.md) subscription another consumer holds.
Unbounded, the second consumer waits for the first to leave, and the service does not start.
Bounded, startup fails with [`PulsarError::Subscribe`](api:ruststream_pulsar::PulsarError::Subscribe), naming the topic.
In a [`TestApp`](../testing/index.md) test the in-process transport refuses a second `Exclusive` consumer at once, so the bound shows only against a server.

## Example

The ledger has one writer at a time.
A second instance of the service tries three times, a second apart, and then fails its startup instead of waiting.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_retries.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_retries.rs#broker"
```

</TabItem>
</Tabs>
