---
sidebar_position: 1
title: Server
description: "The server reports the host and port of the service URL, never a token or credentials, and no protocol version."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host and port of the service URL, never a token or credentials, and no protocol version.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the host and the port of the service URL: the `pulsar://` or `pulsar+ssl://` scheme and any user info stay out of it.
The `protocol` is `pulsar` for both schemes.
`protocolVersion` stays out, because a Pulsar client and the server agree on the protocol version per connection.

The [token](../broker/security.md) the broker authenticates with is not part of the server.
A test that builds the document with a token in the broker finds the token nowhere in it.

## Example

The broker dials the TLS listener of `pulsar.acme.internal` and authenticates with a token.
Its server in the document is `pulsar.acme.internal:6651` with the protocol `pulsar`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_asyncapi.rs#server"
```

</TabItem>
</Tabs>
