---
title: Топики и подписки
sidebar_position: 2
description: "Имена топиков Pulsar, чтение через долговечные подписки и подписка на один топик, на фиксированный список или на все топики, которые подходят под шаблон."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Топики и подписки

Это руководство называет топики Pulsar, читает их через долговечные подписки и охватывает один топик, фиксированный список или все топики, которые подходят под шаблон.

Пример - сервис исполнения заказов.
Он отгружает заказы, уведомляет покупателей, подсчитывает итоги и проверяет региональные заказы, а возвраты возвращает на склад.

## Имена топиков {/* #topic-names */}

Имя топика Pulsar состоит из четырёх частей: `persistent://public/default/orders`.
Первая часть - режим хранения, затем идут [арендатор и пространство имён](https://pulsar.apache.org/docs/next/concepts-multi-tenancy/), затем сам топик.
Короткое имя вроде `orders` означает `persistent://public/default/orders`, и тройка `tenant/namespace/topic` тоже обозначает хранимый топик.
`public/default` - пространство имён, с которого начинается любая установка Pulsar.

Хранимый (persistent) топик хранит сообщения, пока их не подтвердят все подписки.
[Нехранимый топик](https://pulsar.apache.org/docs/next/concepts-messaging/#non-persistent-topics) держит их в памяти и отдаёт каждое только консьюмерам, подключённым в этот момент, поэтому он подходит для данных, которые через мгновение теряют ценность.
Pulsar создаёт топик при первом обращении к нему, поэтому сервис только называет топики, которые читает и в которые пишет.

Сервис проверяет каждое имя топика до подключения, и неправильное имя останавливает его при запуске.
[`PulsarTopic`](../components/subscriptions/pulsar-topic.md) строит имя из частей или разбирает имя, которое приходит извне кода.

## Один топик, две подписки {/* #one-topic-two-subscriptions */}

Pulsar читает топик только через [подписку](https://pulsar.apache.org/docs/next/concepts-messaging/#subscriptions).
Подписка - это курсор, который хранит сервер. Она помнит, что подтвердили её консьюмеры, и продолжает существовать после остановки сервиса.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#topic"
```

</TabItem>
</Tabs>

[`PulsarSubscription::new`](../components/subscriptions/pulsar-subscription.md) называет топик и подписку.
`fulfil` и `notify` читают `orders` через две подписки, поэтому каждый из них получает каждый заказ.
Два экземпляра сервиса делят один курсор на подписку, поэтому они распределяют заказы между собой, а не отгружают каждый дважды.
Называйте подписку по сервису, которому она принадлежит: другой сервис, который назовёт ту же подписку, будет делить с ним её непрочитанные сообщения.

Новая подписка начинается с конца своего топика и видит только то, что опубликовано после неё.
Существующая продолжает с того места, где остановились её консьюмеры, в том числе после перезапуска.
Курсор переставляет руководство [Повторное чтение и перемотка](replay.md).

## Фиксированный список топиков {/* #a-fixed-list-of-topics */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#topics"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#topics"
```

</TabItem>
</Tabs>

[`PulsarSubscription::topics`](../components/subscriptions/topics.md) читает несколько топиков через один консьюмер под одним именем подписки.
Каждый топик хранит под этим именем собственный курсор.
У всех топиков один тип полезной нагрузки.

## Шаблон {/* #a-pattern */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#pattern"
```

</TabItem>
</Tabs>

[`PulsarSubscription::pattern`](../components/subscriptions/pattern.md) читает каждый топик, полное имя которого подходит под регулярное выражение, поэтому выражение записывается полностью.
`audit` читает `orders-eu` и `orders-us`, а также любой топик `orders-*` в `public/default`, созданный позже.
Клиент заново получает список топиков каждые 30 секунд.
Новый топик читается с того обновления списка, которое его нашло, а то, что попало в него раньше, не доставляется.
Выражение, которое не является корректным регулярным выражением, останавливает сервис при запуске.

## Короткая форма {/* #the-short-form */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#short"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#short"
```

</TabItem>
</Tabs>

`#[subscriber("returns")]` называет топик, но не подписку.
Обработчик присоединяется к подписке брокера по умолчанию [`default_subscription`](../components/subscriptions/index.md), здесь `fulfilment`, как к подписке `Shared`.
Короткое имя на брокере без подписки по умолчанию останавливает сервис при запуске, и ошибка называет эту настройку.
Любой другой настройке подписки, например её типу или тайм-ауту подтверждения, нужен дескриптор.

## Тестирование подписок {/* #testing-the-subscriptions */}

Тесты в файле примера запускают приложение внутри процесса.
Каждая подписка на топик получает каждое сообщение, опубликованное в него, как на сервере.
Тест шаблона начинается с остановленными часами и сдвигает их на 30 секунд вперёд через `tb.advance`, поэтому следующее обновление списка происходит сразу.
Работу с `TestApp` описывает руководство [Тестирование сервиса на Pulsar](testing.md).
