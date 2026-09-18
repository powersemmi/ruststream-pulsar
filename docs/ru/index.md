# ruststream-pulsar {#ruststream-pulsar}

**`ruststream-pulsar`** запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
Apache Pulsar. Топик - это журнал, который брокер хранит, поэтому подписка по нему перематывается.
Вы выбираете один из четырёх типов подписки, подписываетесь на список топиков или на шаблон и
ограничиваете число повторов сообщения топиком dead-letter. Фича `testing` даёт внутрипроцессный
брокер.

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

## Что даёт крейт {#what-the-crate-offers}

Дескриптор подписки хранит её форму и настройки: один топик, фиксированный список или шаблон по
пространству имён поиска. Четыре типа подписки собраны в перечисление; предел повторов и топик
dead-letter, объявленные при монтировании, становятся собственной политикой консьюмера; топик - это
журнал, который брокер хранит, поэтому обработчик сам перематывает свою подписку;
ключ партиционирования - единственная настройка, которую публикация меняет для одного сообщения.
Справочник крейта описывает каждую из этих тем:

- [Подписка](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#subscribing):
  дескрипторы, подтверждение,
  [повторы и dead-letter](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#retries-and-dead-lettering),
  [пакеты](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#batches) и
  [перемотка](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#seeking).
- [Публикация](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#publishing):
  политика публикации, ответы и
  [настройки отдельного сообщения](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#per-message-settings).
- [Сгенерированный документ](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#the-generated-document)
  и [тестирование](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#testing):
  что попадает в AsyncAPI и что умеет внутрипроцессный брокер.
- [Эксплуатация](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#operations):
  адрес сервиса, аутентификация и ограничения клиента.

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-pulsar)** - руководство по крейту и его rustdoc на docs.rs.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает только брокер Pulsar. Всё, что работает одинаково на любом брокере, описано в
[документации RustStream](https://powersemmi.github.io/ruststream/).
