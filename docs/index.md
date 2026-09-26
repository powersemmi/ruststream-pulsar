# ruststream-pulsar

**`ruststream-pulsar`** runs a [RustStream](https://powersemmi.github.io/ruststream/) service on
Apache Pulsar. A topic is a retained log, so a subscription rewinds over it. You pick one of the
four subscription types, subscribe to a list of topics or to a pattern, and cap a message's
retries at a dead-letter topic. With the `testing` feature, tests run the production app with the
broker in process.

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

## What the crate offers

A subscription descriptor carries one subscription's form and its settings: one topic, a fixed
list, or a pattern over the lookup namespace. The four subscription types are an enum, the retry
cap and the dead-letter topic a registration declares become the consumer's own policy, a topic is
a retained log so a handler repositions its own subscription, and the partition key is the one
setting a publish adjusts per message. The crate's reference documents each of them:

- [Subscribing](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#subscribing):
  descriptors, acknowledgement,
  [retries and dead-lettering](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#retries-and-dead-lettering),
  [batches](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#batches) and
  [seeking](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#seeking).
- [Publishing](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#publishing):
  the publish policy, replies and
  [per-message settings](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#per-message-settings).
- [The generated document](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#the-generated-document)
  and [Testing](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#testing):
  what AsyncAPI reports, and the production app under `TestApp`, in process or against a live
  broker.
- [Operations](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#operations):
  the service URL, authentication, and the client's limits.

## Where to go next

<div class="grid cards" markdown>

- :material-language-rust: **[API reference](https://docs.rs/ruststream-pulsar)** - the crate's guide and its rustdoc on docs.rs.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.

</div>

## How this site relates to the RustStream docs

This site documents the Pulsar broker only. Everything that works the same on every broker is in
the [RustStream documentation](https://powersemmi.github.io/ruststream/).
