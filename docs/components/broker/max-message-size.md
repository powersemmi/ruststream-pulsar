---
sidebar_position: 3
title: max_message_size
description: "max_message_size is the largest payload a publish may carry, five mebibytes as on the server; a larger one fails before it is sent."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# max_message_size

[`max_message_size`](api:ruststream_pulsar::PulsarBroker::max_message_size) is the largest payload a publish may carry, five mebibytes as on the server; a larger one fails before it is sent.

The default matches the server's own `maxMessageSize`.
A deployment that changed that setting sets the same number here, in bytes.

A publish over the limit returns [`PulsarError::Publish`](api:ruststream_pulsar::PulsarError::Publish), naming the topic, and nothing leaves the service.
The check matters because a server answers an oversized frame by dropping the connection, and every producer and consumer on that connection fails with it.
A handler that receives the error decides how its delivery settles.
Retrying cannot make a payload smaller, so a handler usually logs it and drops the delivery or settles it some other final way.

A [`TestApp`](../testing/index.md) test refuses the same publishes in process.

[Connecting to a production cluster](../../guides/production.md) sets the message size a cluster accepts.

## Example

The cluster runs with a one-mebibyte limit.
A scan that encodes to more is logged and dropped; a smaller one is archived.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_limits.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_limits.rs#app"
```

</TabItem>
</Tabs>
