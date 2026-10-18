---
title: Subscriptions
description: "A topic in #[subscriber] joins the broker's default subscription on it; a PulsarSubscription carries every other setting."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A topic in `#[subscriber]` joins the broker's default subscription on it; a [`PulsarSubscription`](./pulsar-subscription.md) carries every other setting.

`#[subscriber("orders")]` reads the topic `orders`, which is `persistent://public/default/orders`.
Pulsar reads a topic only through a named subscription, and a bare name names none, so the name comes from [`PulsarBroker::default_subscription`](api:ruststream_pulsar::PulsarBroker::default_subscription).
A bare name on a broker with no default subscription stops the service at startup with an error that names the setting.
The subscription is a durable cursor on the server, and it is `Shared`: every handler and every instance of a service that joins it competes for its messages.
Name it after the service that owns the cursor, because another service that names the same subscription shares its backlog.

Every other setting of a subscription belongs to a descriptor, which goes where the name goes, as the attribute's first argument:

| Descriptor | Reads |
| --- | --- |
| [`PulsarSubscription::new`](./pulsar-subscription.md) | one topic |
| [`PulsarSubscription::topics`](./topics.md) | a fixed list of topics |
| [`PulsarSubscription::pattern`](./pattern.md) | every topic whose name matches a regular expression |

The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber), and a descriptor joins the chain through [`map_source`](api:ruststream::runtime::SubscriberSettings::map_source).
A subscription the service creates starts at the tip of its topics, and one that already exists resumes at its cursor; [`start_at`](../positions/start-at.md) moves it.
The Pulsar client moves a delivery whose retries are spent to a [dead-letter topic](../settlement/dead-letters.md) itself, so a registration on any of these forms declares its retries and publishes nothing.

[Topics and subscriptions](../../guides/topics-and-subscriptions.md) builds a fulfilment service on one topic, a fixed list, a pattern and the short form.

## Example

The topic `orders`, read under the subscription `orders-svc`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
