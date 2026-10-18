---
sidebar_position: 6
title: Batches
description: "A slice handler takes a batch the service assembles: full at the mount site's size, or closed batch_wait after its first delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler takes a batch the service assembles: full at the mount site's size, or closed `batch_wait` after its first delivery.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
The Pulsar client hands over one delivery at a time, so the batch is assembled in the service.
A batch closes when it holds `n` deliveries, or when [`batch_wait`](api:ruststream_pulsar::PulsarSubscription::batch_wait) has passed since its first delivery, whichever comes first.
`batch_wait` is a setting of the descriptor, 10 ms by default; a longer wait gives fuller batches on a quiet topic, at the cost of latency.
An idle subscription waits for the first delivery as long as it takes, so a batch is never empty.
One outcome settles every delivery of the batch.
A `Vec<HandlerOutcome>` settles delivery `i` with outcome `i`.
`workers(n)` keeps up to `n` batches in flight at once.
A batch body reads a [`PulsarBatchContext`](../context/batch-context.md), which carries the seek handle and no per-delivery field.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acks the batch and `Err(outcomes)` settles delivery by delivery.

[Acknowledgement](../../guides/acknowledgement.md) records the stock counts of warehouse scans in batches.

## Example

Readings that arrive together are stored in one call.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_batches.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_batches.rs#handler"
```

</TabItem>
</Tabs>

## The batch size

The mount site names the size, so the same handler can run with another size in another service.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_batches.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_batches.rs#app"
```

</TabItem>
</Tabs>
