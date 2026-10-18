---
sidebar_position: 2
description: "Name Pulsar topics, read them under durable subscriptions, and cover one topic, a fixed list or every topic a pattern matches."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Topics and subscriptions

This guide names Pulsar topics, reads them under durable subscriptions, and covers one topic, a fixed list or every topic a pattern matches.

The example is a fulfilment service.
It ships orders, notifies customers, totals and audits regional orders, and restocks returns.

## Topic names

A Pulsar topic name has four parts: `persistent://public/default/orders`.
The first is the persistence, then come the [tenant and the namespace](https://pulsar.apache.org/docs/next/concepts-multi-tenancy/), then the topic itself.
A bare name such as `orders` is short for `persistent://public/default/orders`, and a `tenant/namespace/topic` triple is persistent too.
`public/default` is the namespace every Pulsar installation starts with.

A persistent topic stores its messages until every subscription has acknowledged them.
A [non-persistent topic](https://pulsar.apache.org/docs/next/concepts-messaging/#non-persistent-topics) keeps them in memory and hands each one only to the consumers connected at that moment, so it suits data that is worthless a moment later.
Pulsar creates a topic on its first use, so a service only names the topics it reads and writes.

The service checks every topic name before it connects, and a malformed one stops it at startup.
[`PulsarTopic`](../components/subscriptions/pulsar-topic.md) builds a name from its parts, or parses one that comes from outside the code.

## One topic, two subscriptions

Pulsar reads a topic only through a [subscription](https://pulsar.apache.org/docs/next/concepts-messaging/#subscriptions).
A subscription is a cursor the server keeps: it remembers what its consumers acknowledged, and it outlives the service.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#topic"
```

</TabItem>
</Tabs>

[`PulsarSubscription::new`](../components/subscriptions/pulsar-subscription.md) names the topic and the subscription.
`fulfil` and `notify` read `orders` under two subscriptions, so each of them gets every order.
Two instances of the service share one cursor per subscription, so they split the orders between them instead of shipping each one twice.
Name a subscription after the service that owns it: another service that names the same one shares its backlog.

A new subscription starts at the end of its topic and sees only what is published after it.
An existing one resumes where its consumers stopped, also after a restart.
[Replay and seeking](replay.md) moves the cursor.

## A fixed list of topics

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#topics"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#topics"
```

</TabItem>
</Tabs>

[`PulsarSubscription::topics`](../components/subscriptions/topics.md) reads several topics through one consumer, under one subscription name.
Each topic keeps its own cursor under that name.
All the topics share one payload type.

## A pattern

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#pattern"
```

</TabItem>
</Tabs>

[`PulsarSubscription::pattern`](../components/subscriptions/pattern.md) reads every topic whose full name matches a regular expression, so the expression is written in full.
It reads the namespace its prefix names: `persistent://acme/orders/.*` reads `acme/orders`, and a pattern with no `tenant/namespace/` part reads `public/default`.
`audit` reads `orders-eu` and `orders-us`, and any topic created later whose full name the expression matches.
The client lists the topics again every 30 seconds.
A new topic is read from the listing that finds it on, and what reached it before that listing is not delivered.
An expression that is not a valid regular expression, or whose tenant or namespace is itself a regular expression, stops the service at startup.

## The short form

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#short"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#short"
```

</TabItem>
</Tabs>

`#[subscriber("returns")]` names a topic and no subscription.
It joins the broker's [`default_subscription`](../components/subscriptions/index.md), here `fulfilment`, as a `Shared` subscription.
A bare name on a broker without a default subscription stops the service at startup, and the error names the setting.
Every other setting of a subscription, such as its type or its acknowledgement timeout, needs a descriptor.

## Testing the subscriptions

The tests in the example file run the app in process.
Each subscription over a topic gets every message published to it, as on a server.
The pattern test starts on a paused clock and moves it 30 seconds forward with `tb.advance`, so the next listing comes at once.
[Testing a Pulsar service](testing.md) covers the harness.
