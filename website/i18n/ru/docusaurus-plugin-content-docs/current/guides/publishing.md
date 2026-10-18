---
title: Публикация и ответы
sidebar_position: 7
description: "Ответ на сообщение Pulsar, публикация сообщений с ключом и заголовками через слот Out и публикация после запуска сервиса."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Публикация и ответы

Это руководство отвечает на сообщения Pulsar, публикует сообщения с ключом и заголовками через слот `Out` и публикует после запуска сервиса.

Пример - сервис оформления заказов.
Он подтверждает заказы квитанциями, отвечает на запросы цен, отправляет на склады листы комплектации и объявляет о себе при запуске.

## Ответ в объявленный топик {/* #a-reply-to-a-declared-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#reply"
```

</TabItem>
</Tabs>

`confirm` возвращает `Receipt`, и фреймворк его публикует.
`#[outgoing(name = "receipts")]` закрепляет топик за типом, поэтому подписчику достаточно аргумента `reply` макроса без имени. В ручной форме - `.reply()`.
[Политика публикации](../components/publishing/index.md) брокера, `Publish` из [прелюдии](../components/broker/prelude.md), используется по умолчанию, поэтому простой `include` отвечает через неё.
Публикация возвращает управление, когда брокер сохранил сообщение.

## Ответ в топик, который называет точка монтирования {/* #a-reply-to-a-topic-the-mount-site-names */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#named_reply"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#named_reply"
```

</TabItem>
</Tabs>

`Quote` не называет топик, поэтому его называет `reply("quotes")` в точке монтирования. В ручной форме - `.reply().to("quotes")`.
Тогда тот же тип может отвечать в другой топик в другом сервисе.

Запрос и ответ на него проходят через два обычных топика.
Сервис, который запрашивает цены, публикует в `quote-requests` и читает `quotes` через собственную подписку.
Он сопоставляет каждый ответ с запросом по общему для двух сообщений номеру `request`.

## Публикация с ключом и заголовками {/* #a-keyed-publish-with-headers */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#slot"
```

</TabItem>
</Tabs>

[Слот `Out`](api:ruststream::runtime::Out) публикует из тела обработчика, а `Picks` объявляет тип, который он передаёт.
[`partition_key`](../components/publishing/partition-key.md) задаёт ключом каждого листа комплектации его склад.
На партиционированном топике ключ выбирает партицию, а подписка [`KeyShared`](../components/subscriptions/subscription-type.md) сохраняет порядок каждого ключа.
Этот шаг берётся из прелюдии брокера, поэтому слот ограничен `Publisher<Options = PulsarPublishOptions>`, чтобы шаг был доступен.

Каждый [заголовок](../components/publishing/headers.md) становится одноимённым свойством сообщения.
Консьюмер на другом языке читает `source` как обычное свойство Pulsar, без конверта вокруг полезной нагрузки.
Pulsar передаёт свойства текстом, поэтому публикация со значением заголовка не в UTF-8 возвращает ошибку.

Публикация, которая не удалась, возвращает ошибку, и `dispatch` повторяет заказ.
Полезная нагрузка больше [`max_message_size`](../components/broker/max-message-size.md) брокера, по умолчанию пять мебибайт, возвращает ошибку ещё до отправки.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`.out(Picks, Publish)` связывает слот с политикой публикации.
`after_startup` выполняется один раз, когда подписки открыты, с издателем названной в нём политики, и `checkout` объявляет о себе в `service-events`.
Хук, который возвращает ошибку, останавливает сервис при запуске.

Издатель открывает по одному продюсеру на топик при первой публикации в него, а `shutdown` закрывает их.
Код вне приложения берёт издателя у брокера: [`PulsarBroker::publisher`](api:ruststream_pulsar::PulsarBroker::publisher) до запуска сервиса и [`ConnectedPulsarBroker::publisher`](api:ruststream_pulsar::ConnectedPulsarBroker::publisher) после.

## Тестирование публикаций {/* #testing-the-publishes */}

Тесты в файле примера запускают приложение внутри процесса.
`published` проверяет, что дошло до топика, вместе с заголовками, а `tb.out::<Picks>()` проверяет ключ, с которым публиковал слот.
Pulsar внутри процесса упаковывает заголовки и ключ так же, как клиент, поэтому тест с заголовком, который отверг бы сервер, тоже не проходит.
