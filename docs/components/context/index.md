---
title: PulsarContext
description: "PulsarContext gives a handler where its delivery sits in the log and the handle that moves the subscription, each read through Ctx."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PulsarContext

[`PulsarContext`](api:ruststream_pulsar::PulsarContext) gives a handler where its delivery sits in the log and the handle that moves the subscription, each read through `Ctx`.

The subscription builds it once per delivery, beside the payload and the headers, with no allocation.
A handler binds one field with [`Ctx<K>`](api:ruststream::runtime::Ctx), where `K` is a key:

| Key | Value | What it is |
| --- | --- | --- |
| [`Position`](api:ruststream_pulsar::Position) | [`PulsarPosition`](../positions/index.md) | the delivery's message id; a seek to it delivers this message again |
| [`SeekHandle`](api:ruststream_pulsar::SeekHandle) | [`PulsarSeeker`](api:ruststream_pulsar::PulsarSeeker) | the handle that [moves the subscription](../positions/seek.md), on every topic it reads |

Both keys are in the prelude.
`Ctx<Position>` copies the message id, while `ctx.context(Position)` borrows it.
A body that reads both keys takes `ctx: &mut Context<'_, PulsarContext>` instead and calls `ctx.context(key)` per key.
In the manual form the context is the fourth parameter of `Handle`: `impl Handle<Job, (), (), PulsarContext>`.
A batch body reads a [`PulsarBatchContext`](./batch-context.md), which carries the seek handle alone.

## Example

A job whose result was lost moves the subscription back to its own position, so it is done again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_context.rs#context"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_context.rs#context"
```

</TabItem>
</Tabs>
