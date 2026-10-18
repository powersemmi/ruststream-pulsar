---
sidebar_position: 3
title: pattern
description: "PulsarSubscription::pattern reads every topic whose full name matches a regular expression, topics created later included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# pattern

[`PulsarSubscription::pattern`](api:ruststream_pulsar::PulsarSubscription::pattern) reads every topic whose full name matches a regular expression, topics created later included.

The expression is matched against the full name of each topic, so it is written in full: `persistent://public/default/orders-.*`.
The client lists one namespace for it, the one the prefix names: `persistent://acme/orders/.*` reads `acme/orders`, and a pattern with no `tenant/namespace/` part reads `public/default`.
The client lists the topics again every 30 seconds and joins the subscription to each new match.
A topic created after startup is read from the listing that finds it on, and what reached it before that listing is not delivered.
A pattern has no single topic to name the subscription by, so it reports under the subscription's own name.
The expression is checked at startup, and one that is not a valid regular expression, or whose tenant or namespace is itself a regular expression, stops the service before any call to the server.
The settings and their defaults are those of [`PulsarSubscription`](./pulsar-subscription.md), and a [seek](../positions/seek.md) moves the cursor on every topic the pattern reads.

[Topics and subscriptions](../../guides/topics-and-subscriptions.md) audits every regional order topic, those created later included.

## Example

Every `orders-*` topic is recorded byte for byte, whatever its producer wrote; `returns` is not.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_pattern.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_pattern.rs#pattern"
```

</TabItem>
</Tabs>
