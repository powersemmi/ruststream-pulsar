---
title: Pulsar broker
description: "PulsarBroker connects a service to Pulsar; new only records the service URL, and the runtime dials it once at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Pulsar broker

[`PulsarBroker`](api:ruststream_pulsar::PulsarBroker) connects a service to Pulsar; `new` only records the service URL, and the runtime dials it once at startup.

[`PulsarBroker::new`](api:ruststream_pulsar::PulsarBroker::new) takes one service URL, `pulsar://host:6650` or `pulsar+ssl://host:6651`.
It does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: the [token](./security.md), the [default subscription](api:ruststream_pulsar::PulsarBroker::default_subscription) of a bare topic name, the [operation retries](./operation-retries.md) and the [payload limit](./max-message-size.md).

At [startup](./startup.md) the runtime connects the broker and receives a [`ConnectedPulsarBroker`](api:ruststream_pulsar::ConnectedPulsarBroker).
Every subscription and every publisher of the service opens over that one connection.
At [shutdown](./shutdown.md) the runtime closes the producers the handlers published through.

A subscription names its topic and its durable subscription in a [`PulsarSubscription`](../subscriptions/pulsar-subscription.md).
`#[subscriber("orders")]` names the topic alone and joins the broker's default subscription on it, as the [Subscriptions](../subscriptions/index.md) page shows.

Building the crate needs `protoc` on the path, because the client compiles the Pulsar protocol definitions.
Two features add to the crate: `testing` connects the broker in process for [`TestApp`](../testing/index.md), and `asyncapi` fills in the Pulsar part of the [generated document](../asyncapi/index.md).

## Example

The service reads the topic `orders` in the shared subscription `workers`.
An order with no items comes back, at most five deliveries in all, and then moves to the topic `orders-dlq`.
`cargo run --example pulsar_service --features testing -- run` starts it against a Pulsar on `localhost:6650`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_service.rs#handler"
```

```rust reference="examples/pulsar_service.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_service.rs#handler"
```

```rust reference="examples/manual/pulsar_service.rs#app"
```

</TabItem>
</Tabs>
