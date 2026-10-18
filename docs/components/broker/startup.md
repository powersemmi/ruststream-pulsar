---
sidebar_position: 4
title: Startup
description: "At startup the runtime dials the service URL, then opens a consumer per subscription; a refused descriptor stops the start."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

At startup the runtime dials the service URL, then opens a consumer per subscription; a refused descriptor stops the start.

The connect builds the client and dials the URL once.
A URL the client cannot parse fails it with [`PulsarError::Connect`](api:ruststream_pulsar::PulsarError::Connect).

Each subscription then opens its consumer over that connection.
Before any I/O, the crate checks what needs no server and refuses it with [`PulsarError::Invalid`](api:ruststream_pulsar::PulsarError::Invalid):

- an empty subscription name or an empty topic list;
- a topic name Pulsar does not accept;
- a pattern that is not a regular expression;
- a retry cap without a dead-letter topic, or the reverse, and a cap on an `Exclusive` or `Failover` subscription (see [Dead letters](../settlement/dead-letters.md));
- a bare topic name on a broker with no [default subscription](api:ruststream_pulsar::PulsarBroker::default_subscription); the error names that setting.

A consumer the server refuses fails with [`PulsarError::Subscribe`](api:ruststream_pulsar::PulsarError::Subscribe), naming the topic.
A second consumer of an `Exclusive` subscription waits for the first to leave, unless [`OperationRetries`](./operation-retries.md) bounds the tries.

One refused subscription fails the whole startup, and the service exits before it reads anything.
A [`TestApp`](../testing/index.md) test refuses the same descriptors, so a service that does not start in production does not start in its tests either.

## Example

The service reads `orders` by its bare name, in the default subscription `orders-svc`, and every `audit-*` topic through a pattern.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_startup.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_startup.rs#app"
```

</TabItem>
</Tabs>
