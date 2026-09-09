# ruststream-pulsar

**`ruststream-pulsar`** runs a [RustStream](https://powersemmi.github.io/ruststream/) service on
Apache Pulsar. A topic is a retained log, so a subscription rewinds over it. You pick one of the
four subscription types, subscribe to a list of topics or to a pattern, and set the consumer-side
dead-letter policy. The `testing` feature ships an in-process broker.

The transport is implemented over the [`pulsar`](https://docs.rs/pulsar) client maintained by
StreamNative.

Building requires `protoc` on the path: the client compiles the Pulsar protocol definitions.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
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

This site documents the Pulsar broker only. Everything that works the same on every broker is in
the [RustStream documentation](https://powersemmi.github.io/ruststream/).
