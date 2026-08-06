# ruststream-pulsar

**`ruststream-pulsar`** is the Apache Pulsar broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework. It covers the four
subscription types, multi-topic and pattern subscriptions, the consumer-side dead-letter policy,
seeking over the retained log, and ships an in-process test broker under its `testing` feature.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport over the [`pulsar`](https://docs.rs/pulsar) client maintained by StreamNative, and
nothing broker-specific leaks back into the framework.

Building the crate requires `protoc` on the path, since the client compiles the Pulsar protocol
definitions.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-pulsar = "0.6"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Pulsar guide](pulsar.md)** - subscription descriptors, seeking, acknowledgement, publishing, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-pulsar)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the Pulsar broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to Pulsar and link back to the framework docs where the two meet.
