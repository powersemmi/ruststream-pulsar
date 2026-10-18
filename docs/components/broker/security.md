---
sidebar_position: 1
title: Security
description: "token attaches a JWT to the connection, pulsar+ssl:// dials the TLS listener, and credentials never reach the generated document."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Security

[`token`](api:ruststream_pulsar::PulsarBroker::token) attaches a JWT to the connection, `pulsar+ssl://` dials the TLS listener, and credentials never reach the generated document.

`token` is Pulsar's token authentication: the client sends the JWT when it connects and when it reconnects.
A service reads the token from its environment or its secret store, as the example does.

A `pulsar+ssl://` URL dials the server's TLS listener, port 6651 by convention.
The client verifies the server's certificate against the system's trusted roots and checks its host name.
The connection is TLS from the first byte, so the URL is the whole setting.

A [`TestApp`](../testing/index.md) test runs the same broker in process.
It checks the URL as the client parses it, and the token goes nowhere.
The generated [AsyncAPI document](../asyncapi/server.md) reports the host and the port, and never the token or a user name in the URL.

## Example

The service authenticates with the token in `PULSAR_TOKEN` and talks to the cluster over TLS.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_security.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_security.rs#broker"
```

</TabItem>
</Tabs>
