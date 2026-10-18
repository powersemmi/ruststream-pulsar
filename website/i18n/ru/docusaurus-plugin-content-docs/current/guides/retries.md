---
title: Повторы и dead-letter
sidebar_position: 5
description: "Повтор сообщения Pulsar сразу или позже, предел доставок и топик dead-letter, который читает обработчик, для того, что не обрабатывается."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Повторы и dead-letter

Это руководство повторяет сообщение Pulsar сразу или позже, ограничивает число доставок и переносит то, что раз за разом не обрабатывается, в [топик dead-letter](../components/settlement/dead-letters.md), который читает обработчик.

Pulsar сам доставляет сообщение повторно, поэтому повтор ничего не публикует.
Точка монтирования объявляет предел и топик dead-letter, а применяет их клиент Pulsar.

Пример - сервис возвратов, который проводит возвраты через процессинг карт.
`Processor` заменяет клиент процессинга, и приложение открывает его при запуске.

## Подписчик {/* #the-subscriber */}

Процессинг отказывает в возврате тремя способами, и каждому нужен свой исход.
`Busy` значит, что его лимит запросов исчерпан. Повтор прямо сейчас снова получит отказ, поэтому [`retry_after`](../components/settlement/retry-after.md) ждёт 30 секунд.
`Unavailable` значит, что процессинг не ответил: `retry` пробует снова сразу.
`Declined` значит, что карточный счёт закрыт. Новая попытка получит тот же ответ, поэтому возврат отбрасывается.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) отрицательно подтверждает возврат, и брокер сразу доставляет его снова.
[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) держит возврат в сервисе неподтверждённым в течение задержки, а затем отрицательно подтверждает его.
У отрицательного подтверждения Pulsar нет собственной задержки, поэтому ожидание идёт в процессе.
Процесс, который остановился во время ожидания, теряет ожидание, а не возврат: брокер доставит возврат снова после [тайм-аута подтверждения](../components/settlement/ack-timeout.md) или сразу, когда консьюмер отключится.

Задержка должна быть короче тайм-аута подтверждения подписки, иначе тайм-аут вернул бы возврат раньше.
Более долгая задержка отвергается, когда обработчик её возвращает, с ошибкой, которая называет оба значения.
Подписка без тайм-аута подтверждения принимает любую задержку.

## Предел и топик dead-letter {/* #the-cap-and-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) считает все доставки одного возврата, включая первую.
[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) называет, куда уходит возврат, когда доставки исчерпаны.
Эта пара становится собственной [политикой dead-letter](https://pulsar.apache.org/docs/next/concepts-messaging/#dead-letter-topic) консьюмера.
Клиент Pulsar считает повторные доставки, публикует пятый неудачный возврат в `refunds-dlq` и подтверждает оригинал.
`retry`, `retry_after` и истёкший тайм-аут подтверждения засчитываются как повторная доставка.
Отброшенный возврат завершается и никуда не уходит. В топике dead-letter лежат только возвраты, у которых кончились попытки.

Указывайте оба шага или ни одного: предел без адреса или адрес без предела останавливает сервис при запуске, и ошибка называет недостающий шаг.
Брокер считает повторные доставки только на подписках `Shared` и `KeyShared`, поэтому предел задаётся на них.
Предел на подписке `Exclusive` или `Failover` никогда не был бы достигнут, и такая регистрация останавливает сервис при запуске с ошибкой, которая называет тип.
Обработчик, смонтированный по короткому имени топика, принимает то же объявление на [подписке брокера по умолчанию](../components/subscriptions/index.md).

## Чтение топика dead-letter {/* #reading-the-dead-letter-topic */}

Топик dead-letter - обычный топик, и `review` читает его, как любой другой.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

Возврат в топике dead-letter сохраняет полезную нагрузку и заголовки, с которыми пришёл.
Клиент добавляет два свойства: `REAL_TOPIC`, топик, из которого пришёл возврат, и `ORIGIN_MESSAGE_ID`, его позицию там.
`review` пишет возврат в журнал, чтобы его посмотрел человек.

## Проверка задержки {/* #testing-the-delay */}

Тесты в файле примера запускают приложение внутри процесса с `TestApp::with_state`, который передаёт обработчикам намеренно отказывающий процессинг.
Тест задержки начинается с остановленными часами, поэтому `tb.advance(BACKOFF)` проматывает 30 секунд мгновенно.
Pulsar внутри процесса считает доставки и переносит возврат с исчерпанными попытками в `refunds-dlq` так же, как клиент.
Работу с `TestApp` описывает руководство [Тестирование сервиса на Pulsar](testing.md).
