<h1 align="center">ruststream-pulsar</h1>

<p align="center">
  <i>The Apache Pulsar broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: four subscription types, consumer-side dead-letter policy, and validated topic addressing.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-pulsar/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-pulsar/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-pulsar` will implement the [RustStream](https://github.com/powersemmi/ruststream) broker contract over the [`pulsar`](https://crates.io/crates/pulsar) client maintained by StreamNative. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Status

**Not implemented yet.** This repository is a scaffold: the workspace, CI, and release plumbing are in place, and the crate is an empty stub. The implementation will target the `ruststream` 0.6 line; the design and scope are tracked in [powersemmi/ruststream#190](https://github.com/powersemmi/ruststream/issues/190).

## Planned surface

- Subscription types as an enum with per-variant data: exclusive, shared, failover, and key-shared.
- Consumer-side dead-letter policy with a delivery-attempt limit, plus an ack timeout that redelivers automatically.
- `PulsarTopic` as a validated first-class type (persistence, tenant, namespace, topic), with multi-topic and pattern subscriptions.
- Key sharing mapped onto `Partitioned`; message properties carrying headers.
- The client's serialization traits bridged so encoding stays with the framework's codecs.

The broker contract (lazy startup, the typed connect/shutdown lifecycle, and the optional capability traits) is defined by [`ruststream`](https://crates.io/crates/ruststream) and verified by `ruststream::conformance`, with the suite run against a real broker before release.

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # tests
just ci      # the full local gate
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
