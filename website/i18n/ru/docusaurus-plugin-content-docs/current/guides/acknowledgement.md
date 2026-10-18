---
title: Подтверждение сообщений
sidebar_position: 4
description: "Завершение сообщений Pulsar по исходу обработчика, тайм-аут подтверждения для незавершённой доставки и завершение пакета целиком."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Подтверждение сообщений

Это руководство завершает сообщения Pulsar по исходу обработчика, ограничивает незавершённую доставку тайм-аутом подтверждения и завершает пакет целиком.

Пример - сервис складского учёта.
Он резервирует товар под заказы и записывает остатки по результатам сканирования на складах.
`Stock` заменяет клиент сервиса остатков, и приложение открывает его при запуске.

## Исход и есть завершение {/* #the-outcome-is-the-settlement */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#reserve"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#reserve"
```

</TabItem>
</Tabs>

Значение, которое возвращает `reserve`, решает, что происходит с сообщением в Pulsar:

| Исход | В Pulsar |
|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | сообщение [подтверждается](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment) и больше не возвращается |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | сообщение [подтверждается отрицательно](https://pulsar.apache.org/docs/next/concepts-messaging/#negative-acknowledgment) и сразу доставляется снова |
| [`retry_after(delay)`](../components/settlement/retry-after.md) | сообщение удерживается на время `delay`, затем подтверждается отрицательно |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | сообщение подтверждается |

Сервис остатков, который не ответил, может ответить на следующий вызов, поэтому `reserve` повторяет.
Сообщение возвращается в подписку, и на подписке `Shared` его может взять другой консьюмер.
Артикул, которого нет ни у одного товара, так и останется неизвестным, поэтому `reserve` отбрасывает резервирование.
В Pulsar нет команды, которая отвергает сообщение окончательно, поэтому `drop` подтверждает его, и сообщение исчезает.
Сообщение, которое раз за разом не обрабатывается, покидает подписку через [топик dead-letter](retries.md).

Клиент отправляет подтверждения в фоне.
Завершение возвращает управление, когда подтверждение встало в очередь консьюмера, а брокер записывает его мгновением позже.
Сервис, остановленный в этом промежутке, получит сообщение снова при следующем запуске, поэтому Pulsar доставляет каждое сообщение at-least-once.
Обрабатывайте сообщение так, чтобы вторая доставка не навредила, например привязывайте работу к номеру заказа.

## Тайм-аут подтверждения {/* #the-acknowledgement-timeout */}

Доставка, которую никто не завершил, остаётся у своего консьюмера.
[`ack_timeout`](../components/settlement/ack-timeout.md) в дескрипторе ограничивает её срок: когда срок истёк, клиент просит брокер доставить сообщение снова.
Без тайм-аута такое сообщение возвращается, только когда его консьюмер отключается.

Задайте тайм-аут больше самого долгого времени работы обработчика.
Обработчик, который ещё работает, когда тайм-аут истёк, получит вторую доставку того же сообщения.
Каждая повторная доставка засчитывается в [предел повторов](retries.md), как `retry`.

## Пакеты {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#counts"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#counts"
```

</TabItem>
</Tabs>

Обработчик, который принимает срез, - это [пакетный обработчик](../components/subscriptions/batches.md).
Клиент Pulsar выдаёт сообщения по одному, поэтому пакет собирает сервис.
Пакет закрывается, когда он заполнен или когда с его первого сообщения прошло [`batch_wait`](api:ruststream_pulsar::PulsarSubscription::batch_wait), здесь 250 миллисекунд.
Один исход завершает все сообщения пакета.
В ручной форме `Ok(())` подтверждает пакет, а `Err(outcomes)` завершает каждое сообщение его собственным исходом.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

[`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) в точке монтирования задаёт наибольший размер пакета, здесь 64 записи остатков.
`on_startup` открывает клиенты один раз, а обработчики читают их как состояние приложения.

## Тестирование исходов {/* #testing-the-outcomes */}

Тесты в файле примера запускают приложение [внутри процесса](../components/testing/index.md), с `TestApp::with_state` вместо `on_startup`.
Они намеренно заставляют сервис остатков отказывать и проверяют исход каждой доставки.
Pulsar внутри процесса возвращает отрицательно подтверждённое сообщение в подписку, как это делает сервер.
Повторная доставка по тайм-ауту подтверждения принадлежит серверу, и её проверяют живые тесты крейта на работающем Pulsar.
