---
title: Pulsar bindings
description: "With asyncapi the document speaks Pulsar: each topic's namespace and persistence, and each consumer under x-ruststream-pulsar."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Pulsar bindings

With the `asyncapi` feature the generated document speaks Pulsar: each topic's namespace and persistence, and each consumer under `x-ruststream-pulsar`.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds what it knows about the Pulsar side.
A [server](./server.md) reports the host and the port of the service URL.
A [channel](./channels.md) carries the specification's `pulsar` binding, version `0.1.0`: the namespace and the persistence of the topic behind it.
The specification's Pulsar operation binding has no fields, so a receive [operation](./operations.md) reports its consumer under the extension key `x-ruststream-pulsar`.

Everything comes from the service's own declarations: the broker, the descriptors and the destinations the mount sites resolved.
A subscription by bare topic name carries no descriptor, so its channel and its operation carry no binding.
The document is published and shared, so no binding carries a token or a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-pulsar = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `pulsar`, the key of its server in the document.
`cargo run --example pulsar_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_asyncapi.rs#app"
```

</TabItem>
</Tabs>
