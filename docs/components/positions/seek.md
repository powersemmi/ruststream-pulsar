---
sidebar_position: 2
title: Seek
description: "A handler moves its whole subscription through the SeekHandle key; queued deliveries are dropped and replay starts at the target."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Seek

A handler moves its whole subscription through the [`SeekHandle`](api:ruststream_pulsar::SeekHandle) key; queued deliveries are dropped and replay starts at the target.

`Ctx<SeekHandle>` binds a [`PulsarSeeker`](api:ruststream_pulsar::PulsarSeeker) for the subscription the delivery came from.
Its `seek(position)` takes a [`PulsarPosition`](./index.md), and the next delivery comes from there.
The seek moves the subscription, not one consumer: every topic and every partition it reads, and every consumer that shares it.
Deliveries already queued when the seek lands are dropped, so the handler never sees them.
A batch still being assembled is the exception: it closes with the deliveries it had taken.
A seeker is a value, so a handler can keep it and move the subscription later from elsewhere, as the [PulsarPosition](./index.md) example does.
A seek after the service shut down returns [`PulsarError::NotConnected`](api:ruststream_pulsar::PulsarError::NotConnected).
A batch handler reaches the same key through its [`PulsarBatchContext`](../context/batch-context.md).
A position that has to apply on each boot belongs in [`start_at`](./start-at.md) instead.

## Example

A job marked poisoned starts a run the producer knows to be unprocessable; the subscription jumps to the tip of the log past it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_positions_seek.rs#seek"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_positions_seek.rs#seek"
```

</TabItem>
</Tabs>
