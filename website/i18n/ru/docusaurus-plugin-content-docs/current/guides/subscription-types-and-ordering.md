---
title: Типы подписки и порядок
sidebar_position: 3
description: "Выбор того, как консьюмеры подписки Pulsar делят её между собой, порядок каждого ключа и размещение сообщений с ключом по партициям."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Типы подписки и порядок

Это руководство выбирает, как консьюмеры подписки Pulsar делят её между собой, сохраняет порядок каждого ключа и размещает сообщения с ключом по партициям.

Пример - сервис доставки.
Он упаковывает заказы, отслеживает отправления, проводит платежи и выставляет счета по заказам, и каждая из этих задач читает через подписку своего типа.

## Четыре типа {/* #the-four-types */}

Каждый консьюмер подписки - это обработчик в одном из экземпляров сервиса.
[Тип подписки](https://pulsar.apache.org/docs/next/concepts-messaging/#subscription-types) решает, как эти консьюмеры делят сообщения:

| Тип | Кто получает сообщение | Порядок |
|---|---|---|
| `Shared` *(по умолчанию)* | любой консьюмер, по очереди | не сохраняется |
| `KeyShared` | консьюмер, которому принадлежит ключ сообщения | для каждого ключа |
| `Exclusive` | единственный консьюмер; второй ждёт его ухода | порядок топика |
| `Failover` | активный консьюмер; остальные в резерве | порядок топика |

Тип задаёт [`subscription_type`](../components/subscriptions/subscription-type.md) в дескрипторе.
Все консьюмеры одной подписки называют один и тот же тип.

## Shared: любой консьюмер {/* #shared-any-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#shared"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#shared"
```

</TabItem>
</Tabs>

`Shared` отдаёт каждый заказ одному из консьюмеров `packing`, тому, чья очередь.
Чем больше экземпляров сервиса, тем больше заказов упаковывается одновременно, и два заказа могут завершиться в любом порядке.
Этот тип подходит для работы, сообщения которой не зависят друг от друга.

`pack` публикует событие отправления для каждого заказа.
Шаг [`partition_key`](../components/publishing/partition-key.md) задаёт ключом события номер заказа.
Этот шаг берётся из [прелюдии](../components/broker/prelude.md) брокера, поэтому слот ограничен `Publisher<Options = PulsarPublishOptions>`, чтобы шаг был доступен.
[Публикация](../components/publishing/index.md) возвращает управление, когда брокер сохранил сообщение.

## KeyShared: по порядку для каждого ключа {/* #keyshared-in-order-per-key */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#key_shared"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#key_shared"
```

</TabItem>
</Tabs>

`KeyShared` отдаёт все сообщения одного ключа одному консьюмеру, поэтому события одного заказа приходят в `track` в том порядке, в каком их опубликовал `pack`.
Разные ключи распределяются по консьюмерам, поэтому экземпляры делят работу.
Сервер назначает ключи и переносит часть из них, когда консьюмер подключается или уходит.

Внутри одного экземпляра `workers(8, by_key)` запускает восемь партиций воркеров, и доставки одного ключа выполняются друг за другом на одной партиции воркеров.
Партицию воркеров выбирает ключ партиционирования сообщения, поэтому порядок каждого ключа сохраняется и внутри процесса.
В ручной форме та же настройка - [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).
Ключ приходит в обработчик [заголовком `partition-key`](../components/publishing/keys.md).

## Exclusive: один консьюмер {/* #exclusive-one-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#exclusive"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#exclusive"
```

</TabItem>
</Tabs>

`Exclusive` даёт читать подписку одному консьюмеру, поэтому `book` видит платежи в том порядке, в каком их приняли.
Второму консьюмеру сервер отказывает как занятой, а клиент повторяет запрос, пока первый не уйдёт или не кончатся попытки [`OperationRetries`](../components/broker/operation-retries.md), а по умолчанию они не кончаются никогда.
Поэтому второй экземпляр сервиса не запускается и ничего не сообщает, пока ждёт.
Подписка `Exclusive` принадлежит сервису, который работает в одном экземпляре.

## Failover: один активный консьюмер {/* #failover-one-active-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#failover"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#failover"
```

</TabItem>
</Tabs>

`Failover` принимает всех консьюмеров, но доставляет одному из них.
Остальные находятся в горячем резерве: когда активный отключается, сервер передаёт его сообщения следующему.
Несколько экземпляров сохраняют порядок подписки `Exclusive`, но без её единой точки отказа.
На партиционированном топике у каждой партиции свой активный консьюмер, поэтому экземпляры делят партиции между собой.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#app"
```

</TabItem>
</Tabs>

[`OperationRetries::attempts`](../components/broker/operation-retries.md) ограничивает, сколько раз клиент повторяет операцию, которую сервер не принял.
С тремя попытками второй консьюмер `ledger` останавливает сервис при запуске с ошибкой, которая называет топик.
Ограничение действует на каждую операцию соединения, включая переподключение после перезапуска брокера, поэтому сервис получает раннюю ошибку вместо того, чтобы переждать сбой.

## Партиционированные топики {/* #partitioned-topics */}

[Партиционированный топик](https://pulsar.apache.org/docs/next/concepts-messaging/#partitioned-topics) - это несколько топиков под одним именем, и каждая партиция обслуживается отдельно.
Администратор создаёт его с заданным числом партиций:

```bash
docker compose exec pulsar bin/pulsar-admin topics create-partitioned-topic \
  persistent://public/default/shipments -p 3
```

Подписка читает все партиции топика, а сервис читает его тем же дескриптором, что и любой другой топик.
Сообщение с ключом попадает в партицию, на которую указывает хеш его ключа, поэтому сообщения одного ключа остаются в одной партиции и по порядку.
Сообщения без ключа распределяются по партициям по очереди.

## Тестирование типов {/* #testing-the-types */}

Тесты в файле примера запускают приложение внутри процесса.
Там каждая подписка получает каждое сообщение своего топика один раз, а её тип выбирает консьюмера.
Pulsar внутри процесса сразу отказывает второму консьюмеру `Exclusive`, поэтому тест сразу не проходит, а не ждёт.
Партиции, диапазоны ключей `KeyShared` и выбор консьюмера принадлежат серверу, и их проверяют живые тесты крейта на работающем Pulsar.
