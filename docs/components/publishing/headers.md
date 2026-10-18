---
sidebar_position: 1
title: Headers
description: "A header travels as a Pulsar message property, one per header and no envelope; a value that is not UTF-8 fails the publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

A header travels as a Pulsar message property, one per header and no envelope; a value that is not UTF-8 fails the publish.

The property has the header's name and value, so a consumer in another language reads it as a plain Pulsar property.
The payload stays the encoded message alone.
A message type declares its headers in a typed contract, `#[outgoing(headers = ..)]`, and the handler sets them with `with_headers`.
A `HeaderMap` written by hand works the same.
Pulsar properties are text, so a header value that is not valid UTF-8 fails the publish with [`PulsarError::Publish`](api:ruststream_pulsar::PulsarError::Publish).
On delivery the properties come back as the message's headers.
One header has a meaning of its own: the partition-key header becomes the message [key](./keys.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_publishing_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_publishing_headers.rs#headers"
```

</TabItem>
</Tabs>
