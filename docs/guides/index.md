---
slug: /
sidebar_position: 0
description: "Guides to building Apache Pulsar services with ruststream-pulsar, the Pulsar broker for RustStream."
---

# Guides

`ruststream-pulsar` is the Apache Pulsar broker for [RustStream](https://powersemmi.github.io/ruststream/latest/),
built on the [`pulsar`](https://docs.rs/pulsar) client maintained by StreamNative. The guides take a
Pulsar service from installation to production.

The crate is added beside the framework:

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

Building needs `protoc` on the path: the client compiles the Pulsar protocol definitions.
