---
title: Подключение к промышленному кластеру
sidebar_position: 9
description: "Подключение сервиса на Pulsar к промышленному кластеру: TLS и токен, ограниченные повторы, предел размера сообщения и тест конфигурации."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Подключение к промышленному кластеру

Промышленный кластер требует [TLS и аутентификации](../components/broker/security.md), а сервис решает, сколько ждать кластер и какого размера сообщения отправлять.

## TLS и токен {/* #tls-and-a-token */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

Адрес подключения и токен развёртывание передаёт в переменных окружения.
Клиент Pulsar подключается к одному адресу.
К кластеру из нескольких брокеров ведёт одна точка входа перед ними: DNS-имя, балансировщик нагрузки или [прокси Pulsar](https://pulsar.apache.org/docs/next/concepts-architecture-overview/#service-discovery).

Адрес `pulsar+ssl://` ведёт к TLS-порту кластера, по соглашению это порт 6651.
Клиент проверяет сертификат сервера по доверенным корневым сертификатам системы, а также его имя хоста.
[`token`](api:ruststream_pulsar::PulsarBroker::token) выполняет аутентификацию по JWT, который клиент отправляет при подключении и при переподключении.
Эту пару принимают и StreamNative Cloud, и собственный кластер с аутентификацией по токену.

## Повторы и размер сообщения {/* #retries-and-the-message-size */}

Клиент сам переподключается после перезапуска брокера и повторяет любую операцию, которую кластер ещё не принял.
По умолчанию он повторяет без ограничения, раз в пять секунд, и так пережидает перезапуск любой длительности.
[`OperationRetries`](../components/broker/operation-retries.md) это ограничивает: десять попыток с интервалом три секунды, после чего операция возвращает ошибку с ответом кластера.
Ограничение действует на поиск топиков, продюсеры и переподключение каждого консьюмера.
Оно же превращает второго консьюмера подписки [`Exclusive`](../components/subscriptions/subscription-type.md) в ошибку при запуске вместо ожидания.

[`max_message_size`](../components/broker/max-message-size.md) - наибольшая полезная нагрузка публикации, по умолчанию пять мебибайт, как на сервере.
Развёртывание, в котором изменён `maxMessageSize` сервера, задаёт здесь то же значение.
Публикация большего размера возвращает ошибку в сервисе, не доходя до кластера.

## Сервис {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

Каждая настройка брокера - шаг билдера, который никуда не подключается, поэтому `#[ruststream::app]` строит приложение так же, как локально.
Соединение открывается при запуске сервиса.
Генерируемый [документ AsyncAPI](../components/asyncapi/server.md) указывает хост и порт из адреса, но никогда не токен.
Сервису, который [перечитывает](replay.md) свои топики, нужна ещё политика хранения пространства имён на кластере.

## Проверка конфигурации {/* #testing-the-configuration */}

Тесты запускают рабочий `service` с рабочим `broker`, которому передан тестовый токен, через `TestApp::start`.
[Pulsar внутри процесса](../components/testing/index.md) проверяет адрес подключения так, как его разбирает клиент, но к кластеру не подключается.
С адресом другой схемы запуск возвращает ошибку, и она называет схемы, по которым подключается клиент:

```text
the service URL's scheme "https" is not one the client dials: pulsar or pulsar+ssl
```

TLS, токен и ограничение повторов действуют только на настоящем кластере.
