---
sidebar_position: 1
title: PulsarBatchContext
description: "A batch handler's context carries the subscription's seek handle alone; asking a batch for Position does not compile."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PulsarBatchContext

A [batch handler's](../subscriptions/batches.md) context, [`PulsarBatchContext`](api:ruststream_pulsar::PulsarBatchContext), carries the subscription's seek handle alone; asking a batch for `Position` does not compile.

A batch spans many deliveries, so it has no single position in the log.
The subscription builds the context once per batch, from what every delivery of the batch shares: the [`SeekHandle`](api:ruststream_pulsar::SeekHandle) key, the same key a [`PulsarContext`](./index.md) carries.
A batch body takes `ctx: &mut Context<'_, PulsarBatchContext>` and reads the handle with `ctx.context(SeekHandle)`.
A seek from a batch body takes effect once the batch has settled, so the next batch opens at the new position.
`Position` is a compile error in a batch body, because the batch context does not carry it.
In the manual form the context is the fourth parameter of `Handle`: `impl Handle<[Job], (), (), PulsarBatchContext>`.

## Example

A batch that holds a job whose result was lost replays the retained log once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_context.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_context.rs#batch"
```

</TabItem>
</Tabs>
