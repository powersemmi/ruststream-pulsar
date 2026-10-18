---
sidebar_position: 3
title: partition_key
description: "The partition_key step keys one publish from a handler body; it wins over a partition-key header and keeps the slot's codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# partition_key

The [`partition_key`](api:ruststream_pulsar::PulsarPublishSteps::partition_key) step keys one publish from a handler body; it wins over a partition-key header and keeps the slot's codec.

The key does what the [partition-key header](./keys.md) does: it places the message on a partition and orders `KeyShared` consumers.
The step writes the one field of [`PulsarPublishOptions`](api:ruststream_pulsar::PulsarPublishOptions) and returns the builder.
The message still leaves through the slot the mount site wired, in that slot's codec and through its transforms.
A body reaches the step by bounding its slot `Out<impl Publisher<Options = PulsarPublishOptions>, Slot>`.
That body imports `ruststream_pulsar::prelude::*`, the one case where a handler body names this broker.
The mount site declares nothing for it, and a publish without the step goes unkeyed.
Where a publish carries both the step and the header, the step wins.
A test reads the key a slot publish carried with `tb.out::<Slot>().with_options(..)`.

[Publishing and replies](../../guides/publishing.md) keys each pick list by its warehouse.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_publishing_partition_key.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_publishing_partition_key.rs#handler"
```

</TabItem>
</Tabs>
