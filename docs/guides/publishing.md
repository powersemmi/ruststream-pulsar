---
sidebar_position: 7
description: "Reply to a Pulsar message, publish keyed messages with headers through an Out slot, and publish once the service has started."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publishing and replies

This guide replies to Pulsar messages, publishes keyed messages with headers through an `Out` slot, and publishes once the service has started.

The example is a checkout service.
It confirms orders with receipts, answers price quotes, sends pick lists to warehouses and announces itself at startup.

## A reply to a declared topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#reply"
```

</TabItem>
</Tabs>

`confirm` returns a `Receipt`, and the framework publishes it.
`#[outgoing(name = "receipts")]` fixes the topic on the type, so the subscriber writes the bare `reply` clause; in the manual form, `.reply()`.
The broker's [publish policy](../components/publishing/index.md), `Publish` in the prelude, is its default, so a plain `include` replies through it.
A publish returns once the broker has stored the message.

## A reply to a topic the mount site names

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#named_reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#named_reply"
```

</TabItem>
</Tabs>

`Quote` names no topic, so `reply("quotes")` names it at the mount site; in the manual form, `.reply().to("quotes")`.
The same type can then answer on another topic in another service.

A request and its answer travel on two ordinary topics.
A service that asks for quotes publishes to `quote-requests` and reads `quotes` under a subscription of its own.
It matches each quote to its request by the `request` id the two messages share.

## A keyed publish with headers

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#slot"
```

</TabItem>
</Tabs>

An [`Out` slot](api:ruststream::runtime::Out) publishes from the handler's body, and `Picks` declares the type it carries.
[`partition_key`](../components/publishing/partition-key.md) keys each pick list by its warehouse.
On a partitioned topic the key picks the partition, and a `KeyShared` subscription keeps the order of each key.
The step comes from the broker's prelude, so the slot is bounded `Publisher<Options = PulsarPublishOptions>` to offer it.

Each [header](../components/publishing/headers.md) becomes a message property of the same name.
A consumer in another language reads `source` as an ordinary Pulsar property, with no envelope around the payload.
Pulsar carries properties as text, so a header value that is not UTF-8 fails the publish.

A publish that fails returns the error, and `dispatch` retries the order.
A payload larger than the broker's [`max_message_size`](../components/broker/max-message-size.md), five mebibytes by default, fails before it is sent.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`.out(Picks, Publish)` wires the slot to the publish policy.
`after_startup` runs once the subscriptions are open, with a publisher of the policy it names, and `checkout` announces itself on `service-events`.
A hook that fails stops the service at startup.

The publisher opens one producer per topic on the first publish to it, and `shutdown` closes them.
Code outside the app takes a publisher from the broker: [`PulsarBroker::publisher`](api:ruststream_pulsar::PulsarBroker::publisher) before the service starts, and [`ConnectedPulsarBroker::publisher`](api:ruststream_pulsar::ConnectedPulsarBroker::publisher) after.

## Testing the publishes

The tests in the example file run the app in process.
`published` checks what reached a topic, with its headers, and `tb.out::<Picks>()` checks the key the slot's publish carried.
The in-process Pulsar frames headers and the key the way the client does, so a header the server would refuse fails the test too.
