---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на Apache Pulsar с ruststream-pulsar, брокером Pulsar для RustStream."
---

# Руководства

Крейт `ruststream-pulsar` реализует брокер Apache Pulsar для [RustStream](https://powersemmi.github.io/ruststream/latest/ru/).
Он построен на клиенте [`pulsar`](https://docs.rs/pulsar), который сопровождает StreamNative.
Руководства проводят сервис на Pulsar от установки до промышленной эксплуатации.

Крейт подключается рядом с фреймворком:

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

Для сборки нужен `protoc` в `PATH`: клиент компилирует описания протокола Pulsar.
