# ruststream-pulsar {#ruststream-pulsar}

**`ruststream-pulsar`** запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
Apache Pulsar. Топик - это журнал, который брокер хранит, поэтому подписка по нему перематывается.
Вы выбираете один из четырёх типов подписки, подписываетесь на список топиков или на шаблон и
задаёте политику dead-letter на стороне консьюмера. Фича `testing` даёт брокер внутри процесса.

Транспорт построен на клиенте [`pulsar`](https://docs.rs/pulsar), который ведёт StreamNative.

Для сборки нужен `protoc` в `PATH`: клиент компилирует определения протокола Pulsar.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:app"
```

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Руководство по Pulsar](pulsar.md)** - дескрипторы подписки, перемотка, подтверждение, публикация и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-pulsar)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает только брокер Pulsar. Всё, что работает одинаково на любом брокере, описано в
[документации RustStream](https://powersemmi.github.io/ruststream/).
