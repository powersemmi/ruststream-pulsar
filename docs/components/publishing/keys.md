---
sidebar_position: 2
title: Keys
description: "The partition-key header is the message key: it picks the partition of a partitioned topic and orders KeyShared and by_key lanes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Keys

The partition-key header, [`PARTITION_KEY_HEADER`](api:ruststream_pulsar::PARTITION_KEY_HEADER), is the message key: it picks the partition of a partitioned topic and orders `KeyShared` and `by_key` lanes.

The header is named `partition-key`.
On publish its value becomes the Pulsar message key and does not travel as a property of its own.
On a partitioned topic the producer hashes the key, so every message of one key goes to one partition, in the order it was published.
A publish without a key spreads over the partitions in turn.
A message type declares the header in its typed contract, `#[outgoing(headers = ..)]`, with a field renamed to `partition-key`; a `HeaderMap` with that entry works the same.
On consume the key comes back as that header.
A [`KeyShared`](../subscriptions/subscription-type.md) subscription hands every message of one key to one consumer, and `workers(n, by_key)` keeps one key on one lane inside a consumer.
A handler body that keys one publish of a slot uses the [`partition_key`](./partition-key.md) step instead.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_publishing_keys.rs#keys"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_publishing_keys.rs#keys"
```

</TabItem>
</Tabs>
