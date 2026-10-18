---
title: Тестирование сервиса на Pulsar
sidebar_position: 8
description: "Тестирование сервиса на Pulsar внутри процесса с TestApp: рабочее приложение, платежи с ключом, ответы, задержки и тот же тест на сервере."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на Pulsar

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [Pulsar внутри процесса](../components/testing/index.md) или на работающем сервере.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в топик `card-payments`, и его ключ партиционирования - счёт для списания.
Обработчик отвечает квитанцией в топик `card-receipts`.
Платёж без ключа отбрасывается.
Платёж сверх мгновенного лимита банка откладывается: обработчик просит доставить его снова через минуту с [`retry_after`](../components/settlement/retry-after.md).
После трёх доставок платёж уходит в `card-payments-review`, [топик dead-letter](../components/settlement/dead-letters.md).
Приложение строит `service(broker)`, поэтому тест выбирает брокер сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-pulsar` в `[dev-dependencies]`.

## Платёж с ключом {/* #a-keyed-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`PulsarBroker`](../components/broker/index.md) к Pulsar внутри процесса.
Адрес подключения проверяется так, как его разобрал бы клиент, но соединение по нему не устанавливается.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<PulsarBroker>()`.
Ключ партиционирования передаётся заголовком [`PARTITION_KEY_HEADER`](../components/publishing/keys.md), поэтому тест задаёт его через `with_headers`.
Брокер превращает этот заголовок в ключ сообщения, а обработчик читает его обратно из того же заголовка.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик завершил доставку и что дошло до `card-receipts`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на сервере ниже.

## Платёж без ключа {/* #a-payment-without-a-key */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#keyless"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#keyless"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Задержка без ожидания {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` не стоит ничего.
`retry_after` держит платёж в сервисе в течение задержки, а затем отрицательно подтверждает его, поэтому брокер доставляет то же сообщение снова.
Для повтора ничего не публикуется: собственный платёж теста - единственное сообщение в `card-payments`.
Третья доставка - последняя, которую допускает точка монтирования, поэтому после её ожидания платёж уходит в `card-payments-review` в том виде, в каком пришёл.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует Pulsar внутри процесса {/* #what-the-in-process-pulsar-models */}

Pulsar внутри процесса читает ту же конфигурацию, которую получил бы сервер: настройки брокера, каждый дескриптор и объявление повторов каждой регистрации.
Он отвергает то, что отвергает сервер, с той же ошибкой, которую возвращает живой брокер: адрес подключения, который клиент не может разобрать, адрес публикации, который не является именем топика, второго консьюмера подписки `Exclusive` и дескриптор, использованный после остановки.
Он моделирует Pulsar, а не очередь:

- Топик определяется полным именем, поэтому `orders` и `persistent://public/default/orders` - один топик.
- Каждая подписка получает каждое сообщение своих топиков один раз, а её [тип](subscription-types-and-ordering.md) выбирает консьюмера.
- Отрицательно подтверждённое сообщение возвращается в свою подписку, а политика dead-letter считает его доставки так же, как клиент.
- Каждый топик хранит свой журнал, поэтому [перемотка](replay.md) перечитывает его. Нехранимый топик ничего не перечитывает.
- Подписка по шаблону читает новый топик со следующего обновления списка у клиента, через 30 секунд по остановленным часам.
- Заголовки передаются текстовыми свойствами, а публикация с полезной нагрузкой больше `max_message_size` возвращает ошибку.

Сервер заставляет второго консьюмера `Exclusive` ждать, а Pulsar внутри процесса сразу ему отказывает, поэтому такой тест не проходит, а не зависает.
Для повторной доставки по тайм-ауту подтверждения, партиционированных топиков, диапазонов ключей `KeyShared` и сообщений, опубликованных, пока у подписки нет консьюмера, нужен сервер.

## Тот же тест на сервере {/* #the-same-test-on-a-server */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к работающему Pulsar.
Тест читает адрес из `PULSAR_TEST_URL` через `stand`, собственную вспомогательную функцию примера, и пропускается, если переменная не задана.
`RUSTSTREAM_REQUIRE_LIVE=1` превращает этот пропуск в ошибку, поэтому задание CI не может пройти без сервера.
Тест подключается к собственной подписке, названной по процессу.
Новая подписка начинается с конца топика, поэтому ничего, что оставил прошлый прогон, не читается.
Проверки те же, что и внутри процесса, без изменений.

```bash
cargo test
PULSAR_TEST_URL=pulsar://localhost:6650 cargo test -- --test-threads=1
```

Сервер - тот же, что в руководстве [Начало работы](getting-started.md).
