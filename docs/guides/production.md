---
sidebar_position: 9
description: "Connect a Pulsar service to a production cluster: TLS and a token, bounded retries, the message size limit and a test of the configuration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to a production cluster

A production cluster asks for [TLS and authentication](../components/broker/security.md), and a service decides how long it waits for the cluster and how large a message it sends.

## TLS and a token

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

The deployment provides the service URL and the token in environment variables.
A Pulsar client dials one service URL.
A cluster of several brokers is reached through one entry point in front of them: a DNS name, a load balancer or the [Pulsar proxy](https://pulsar.apache.org/docs/next/concepts-architecture-overview/#service-discovery).

A `pulsar+ssl://` URL dials the cluster's TLS listener, port 6651 by convention.
The client checks the server's certificate against the system's trusted roots, and its host name.
[`token`](api:ruststream_pulsar::PulsarBroker::token) authenticates with a JWT, which the client sends when it connects and when it reconnects.
StreamNative Cloud and a self-hosted cluster with token authentication both take this pair.

## Retries and the message size

The client reconnects on its own after a broker restart, and asks again for any operation the cluster has not accepted yet.
By default it asks for ever, five seconds apart, which waits out a restart of any length.
[`OperationRetries`](../components/broker/operation-retries.md) bounds that: ten tries three seconds apart, and then the operation fails with the cluster's answer.
The bound covers the lookups, the producers and the reconnect of every consumer.
It also turns a second consumer of an `Exclusive` subscription into an error at startup instead of a wait.

[`max_message_size`](../components/broker/max-message-size.md) is the largest payload a publish may carry, five mebibytes by default as on the server.
A deployment that changed the server's `maxMessageSize` sets the same value here.
A larger publish fails in the service, before it reaches the cluster.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

Every setting of the broker is a builder step that dials nothing, so `#[ruststream::app]` builds the app as it does locally.
The connection opens when the service starts.
The generated [AsyncAPI document](../components/asyncapi/server.md) reports the host and the port of the URL, and never the token.
A service that [replays](replay.md) its topics also needs the namespace's retention policy on the cluster.

## Testing the configuration

The tests start the production `service` with the production `broker`, given a test token, under `TestApp::start`.
The [in-process Pulsar](../components/testing/index.md) checks the service URL as the client parses it, without dialling the cluster.
A URL with another scheme fails the start, and the error names the schemes the client dials:

```text
the service URL's scheme "https" is not one the client dials: pulsar or pulsar+ssl
```

TLS, the token and the retry bound take effect only on a real cluster.
