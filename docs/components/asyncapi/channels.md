---
sidebar_position: 2
title: Channels
description: "A topic's channel carries the pulsar binding: its namespace and persistence, for subscriptions and publish destinations alike."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A topic's channel carries the `pulsar` binding: its namespace and persistence, for subscriptions and publish destinations alike.

The binding has two fields, read off the topic name.
`persistent://acme/orders/created` has the namespace `orders` and the persistence `persistent`; a `non-persistent://` topic reports `non-persistent`.
A [`PulsarSubscription::new`](../subscriptions/pulsar-subscription.md) reads one topic and reports it.
A [`topics`](../subscriptions/topics.md) subscription reports the binding when all its topics share one namespace and one persistence.
Its channel is named after the subscription.
A [`pattern`](../subscriptions/pattern.md) subscription has no topic until it lists the server's topics, so its channel carries no binding.

A channel the service publishes to reports the topic the mount site resolved.
That is a `reply("dest")` clause, a reply type's `#[outgoing(name)]`, the name of a slot entry, or a declared [dead-letter topic](../settlement/dead-letters.md).
A destination that is not a valid topic name carries no binding.

## Example

The channel `persistent://acme/orders/created` reports the namespace `orders`, and so does the reply destination `persistent://acme/orders/confirmed`.
The channel `regional` spans two namespaces and the pattern channel `auditors` names no topic, so neither carries a binding.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
