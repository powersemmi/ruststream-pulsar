---
sidebar_position: 4
title: PulsarTopic
description: "PulsarTopic is a topic name checked on construction: persistent and non_persistent for literals, parse for names read at run time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PulsarTopic

[`PulsarTopic`](api:ruststream_pulsar::PulsarTopic) is a topic name checked on construction: `persistent` and `non_persistent` for literals, `parse` for names read at run time.

A Pulsar topic name carries four things: the persistence, the tenant, the namespace and the topic.
Each of the last three may hold ASCII letters, digits, `-`, `_` and `.`, and none may be empty.
[`PulsarTopic::persistent`](api:ruststream_pulsar::PulsarTopic::persistent) and [`PulsarTopic::non_persistent`](api:ruststream_pulsar::PulsarTopic::non_persistent) build a name from its parts and panic on a part Pulsar would refuse, so they are for names written in the code.
[`PulsarTopic::parse`](api:ruststream_pulsar::PulsarTopic::parse) is the form for a name that arrives at run time.
It accepts a full name, a `tenant/namespace/topic` triple, which is persistent, or a bare name, which belongs to `persistent://public/default`, and returns an error for anything else.
[`as_str`](api:ruststream_pulsar::PulsarTopic::as_str) is the full name, and `Display` writes the same.
[`persistence`](api:ruststream_pulsar::PulsarTopic::persistence), [`tenant`](api:ruststream_pulsar::PulsarTopic::tenant), [`namespace`](api:ruststream_pulsar::PulsarTopic::namespace) and [`name`](api:ruststream_pulsar::PulsarTopic::name) return the parts.
A descriptor and a publish parse every topic they are given the same way, so a name Pulsar would refuse never reaches the server: a descriptor stops the service at startup, and a publish returns an error.

## Example

Each order goes to its tenant's own topic; an order naming a tenant that is no valid name part is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_subscriptions_topic.rs#route"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_subscriptions_topic.rs#route"
```

</TabItem>
</Tabs>
