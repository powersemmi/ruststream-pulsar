---
title: PulsarPosition
description: "PulsarPosition names a place in a topic's retained log: either end, a delivered message, or a publish time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PulsarPosition

[`PulsarPosition`](api:ruststream_pulsar::PulsarPosition) names a place in a topic's retained log: either end, a delivered message, or a publish time.

Pulsar keeps a topic's messages as a log, so a subscription can be moved through it, at startup with [`start_at`](./start-at.md) or from a handler with a [seek](./seek.md).
`PulsarPosition::earliest()` is the oldest message the topic still retains, and every retained message is delivered again from there.
`PulsarPosition::latest()` is the tip of the log: only messages published after the move are delivered.
`PulsarPosition::timestamp(millis)` is the first message published at or after that time, in milliseconds since the Unix epoch.
A delivery's own place is a position as well: `Ctx<Position>` binds it through the [`Position`](api:ruststream_pulsar::Position) key, a copy the handler can keep.
A seek to such a position delivers that message again, then the rest of the log in order.
A position applies to every topic and every partition the subscription reads.
A non-persistent topic retains nothing, so a move over it replays nothing.

## Example

A projection bookmarks the entry that closes a day; a rebuild request moves the ledger subscription back to it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_positions.rs#position"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_positions.rs#position"
```

</TabItem>
</Tabs>
