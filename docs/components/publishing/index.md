---
title: Publish
description: "PulsarPublish, Publish in the prelude, opens one producer per topic and waits for the broker's receipt, so Ok means stored."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publish

[`PulsarPublish`](api:ruststream_pulsar::PulsarPublish), `Publish` in the prelude, opens one producer per topic and waits for the broker's receipt, so `Ok` means stored.

A mount site names it wherever a registration publishes: [`.out_reply(Publish)`](api:ruststream::runtime::Mounting::out_reply) for a reply, `.out(Slot, Publish)` for a slot.
The policy holds no connection; the runtime pairs it with the connected broker at startup into a [`PulsarPublisher`](api:ruststream_pulsar::PulsarPublisher).
A registration that names no policy for its reply publishes through this one.
A reply goes to the topic its type names with `#[outgoing(name = "..")]`, or to the one a `reply("..")` clause names when the type leaves it open.
The publisher opens a producer for a topic on the first publish there and keeps it until shutdown.
A publish returns once the broker has stored the message, or with the error it reported.
Outside a handler, [`PulsarBroker::publisher`](api:ruststream_pulsar::PulsarBroker::publisher) hands out a publisher before the service starts.
Per message, a publish carries [headers](./headers.md) and a [key](./keys.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_publishing_publish.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_publishing_publish.rs#reply"
```

</TabItem>
</Tabs>
