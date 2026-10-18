---
sidebar_position: 1
title: start_at
description: "start_at seeks a subscription to a position on every startup, rewinding its durable cursor, unlike Pulsar's initial position."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_at

`start_at` seeks a subscription to a [position](./index.md) on every startup, rewinding its durable cursor, unlike Pulsar's initial position.

It is a clause of the attribute, `start_at(PulsarPosition::earliest())`, or [`.start_at(position)`](api:ruststream::runtime::SubscriberSettings::start_at) among the mount-site settings.
A Pulsar subscription is durable state on the broker: its cursor records what was acknowledged, and it outlives the service.
The seek lands before the subscription's first delivery, so the handler sees the log from the position on.
`start_at(PulsarPosition::earliest())` therefore replays the retained backlog each time the service starts, acknowledged messages included.
Pulsar's own initial position is different: the broker applies it only when it creates the subscription.
A subscription without the clause resumes from its cursor.
It suits a read model rebuilt from the retained log on each boot.

## Example

The audit projection is rebuilt from the oldest retained entry on every startup, although its subscription acknowledged everything it read before.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_positions_start_at.rs#start_at"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_positions_start_at.rs#start_at"
```

</TabItem>
</Tabs>
