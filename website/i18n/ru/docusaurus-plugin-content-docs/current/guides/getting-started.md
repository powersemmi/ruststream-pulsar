---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-pulsar, локальный Pulsar, первый подписчик на нём и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-pulsar`, запускает локальный Pulsar и первый сервис на нём.

## Установка {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-pulsar = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-pulsar` - брокер Pulsar.
Его фича `testing` добавляет [Pulsar внутри процесса](../components/testing/index.md), на котором работают тесты.

У крейта брокера две фичи, обе выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `testing` | Pulsar внутри процесса, на котором работает `TestApp` |
| `asyncapi` | [привязки Pulsar](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |

## Системные требования {/* #system-requirements */}

Клиент Pulsar компилирует описания протокола Pulsar во время сборки, поэтому для сборки нужен `protoc`, компилятор Protocol Buffers.
В Debian и Ubuntu его ставит пакет `protobuf-compiler`, а в macOS - команда `brew install protobuf`.
Путь к `protoc`, который не лежит в `PATH`, задаёт переменная окружения `PROTOC`.

## Локальный Pulsar {/* #a-local-pulsar */}

Pulsar в автономном режиме (standalone) запускает брокер и его хранилище в одном контейнере:

```yaml title="compose.yaml"
services:
  pulsar:
    image: apachepulsar/pulsar:4.1.2
    command: bin/pulsar standalone
    ports:
      - "6650:6650"
      - "8080:8080"
    environment:
      # The address the broker hands to clients outside the container.
      PULSAR_PREFIX_advertisedAddress: 127.0.0.1
      PULSAR_PREFIX_advertisedListeners: pulsar://127.0.0.1:6650
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080/admin/v2/brokers/health || exit 1"]
      interval: 10s
      retries: 30
```

Запустите его и дождитесь, пока он станет работоспособным:

```bash
docker compose up -d --wait
```

Брокер слушает `localhost:6650`, а API администрирования - `localhost:8080`.
Pulsar создаёт топик при первом обращении к нему, поэтому сервису не нужна предварительная настройка.

## Первый сервис {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из топика `orders`.
Заказ с суммой он подтверждает, а заказ без суммы отбрасывает.
Pulsar читает топик только через именованную подписку.
[`PulsarSubscription::new`](../components/subscriptions/pulsar-subscription.md) называет топик и подписку, здесь `billing`.
Подписка - это курсор, который хранит сервер, поэтому после перезапуска сервис продолжает с того места, где остановился.
[`PulsarBroker::new`](../components/broker/index.md) принимает адрес подключения (service URL) и только запоминает его.
Соединение открывается при запуске сервиса.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте заказ из другого терминала клиентом, который входит в образ:

```bash
docker compose exec pulsar bin/pulsar-client produce orders \
  -s ';' -m '{"id":1,"amount_cents":1250}'
```

`-s ';'` сохраняет сообщение целым: по умолчанию клиент разбивает его по каждой запятой.
Сервис пишет в журнал `order taken` с номером заказа.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест {/* #the-first-test */}

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `PulsarBroker` подключается к Pulsar внутри процесса, поэтому тестам не нужен сервер:

```bash
cargo test
```

Руководство [Тестирование сервиса на Pulsar](testing.md) разбирает эти тесты и то, что моделирует Pulsar внутри процесса.

## Переход с крейта pulsar {/* #coming-from-the-pulsar-crate */}

Сервис на крейте [`pulsar`](https://docs.rs/pulsar) сам строит клиент, цикл консьюмера и продюсеры.
Вот куда переходит каждая часть в RustStream:

| Крейт `pulsar` | RustStream |
|---|---|
| `Pulsar::builder(url, TokioExecutor).build().await` | [`PulsarBroker::new(url)`](../components/broker/index.md), который подключает рантайм |
| `.with_auth(Authentication { name: "token", .. })` | [`PulsarBroker::token`](../components/broker/security.md) |
| `.with_operation_retry_options(..)` | [`PulsarBroker::operation_retries`](../components/broker/operation-retries.md) |
| `client.consumer().with_topic(..).with_subscription(..)` и цикл по консьюмеру | обработчик `#[subscriber]` на [`PulsarSubscription`](../components/subscriptions/pulsar-subscription.md) |
| `.with_subscription_type(..)` | [`subscription_type`](../components/subscriptions/subscription-type.md) в дескрипторе |
| `.with_topics([..])`, `.with_topic_regex(..)` | [`PulsarSubscription::topics`](../components/subscriptions/topics.md), [`PulsarSubscription::pattern`](../components/subscriptions/pattern.md) |
| десериализация `msg.payload.data` | тип параметра обработчика, который декодирует кодек |
| `consumer.ack(&msg)`, `consumer.nack(&msg)` | исход обработчика ([Подтверждение](acknowledgement.md)) |
| `.with_dead_letter_policy(..)` | `max_attempts(n)` и `dead_letter(topic)` в точке монтирования ([Повторы и dead-letter](retries.md)) |
| `.with_unacked_message_resend_delay(..)` | [`ack_timeout`](../components/settlement/ack-timeout.md) в дескрипторе |
| `consumer.seek(..)` | [`start_at`](../components/positions/start-at.md) в точке монтирования или [ручка перемотки](../components/positions/seek.md) в обработчике ([Повторное чтение и перемотка](replay.md)) |
| `client.producer().with_topic(..).build()` и `send_non_blocking` | ответ или параметр `Out`, отправленный через [политику публикации](../components/publishing/index.md) ([Публикация и ответы](publishing.md)) |
| `producer::Message { partition_key, properties, .. }` | шаг [`partition_key`](../components/publishing/partition-key.md); [заголовки](../components/publishing/headers.md) становятся свойствами |
