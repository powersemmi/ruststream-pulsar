---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-pulsar，启动本地 Pulsar，运行第一个订阅者，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-pulsar`，启动一个本地 Pulsar，并让第一个服务连上它运行。

## 安装 {/* #installation */}

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

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-pulsar` 是 Pulsar Broker。
它的 `testing` feature 加入测试所用的[进程内 Pulsar](../components/testing/index.md)。

这个 Broker crate 有以下两个 feature，默认都不启用：

| Feature | 提供的内容 |
|---|---|
| `testing` | `TestApp` 背后的进程内 Pulsar |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [Pulsar 绑定](../components/asyncapi/index.md) |

## 系统要求 {/* #system-requirements */}

Pulsar 客户端在构建时编译 Pulsar 的协议定义，所以构建需要 `protoc`，即 Protocol Buffers 编译器。
在 Debian 和 Ubuntu 上，它由 `protobuf-compiler` 提供；在 macOS 上，用 `brew install protobuf` 安装。
不在 `PATH` 中的 `protoc` 由 `PROTOC` 环境变量指定。

## 本地 Pulsar {/* #a-local-pulsar */}

standalone 模式的 Pulsar 在一个容器里同时运行 Broker 及其存储：

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

启动它，并等到它进入健康状态：

```bash
docker compose up -d --wait
```

Broker 监听 `localhost:6650`，管理 API 监听 `localhost:8080`。
Pulsar 在第一次使用主题时创建它，所以服务不需要任何准备工作。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 `orders` 主题读取 JSON 订单。
带金额的订单被确认，没有金额的订单被丢弃。
Pulsar 只通过具名的订阅读取主题。
[`PulsarSubscription::new`](../components/subscriptions/pulsar-subscription.md) 指定主题和订阅，这里的订阅是 `billing`。
订阅是服务器保存的一个游标，所以服务重启后会从停下的地方继续。
[`PulsarBroker::new`](../components/broker/index.md) 接收服务 URL，只把它记录下来。
连接在服务启动时才打开。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，用镜像自带的客户端发布一个订单：

```bash
docker compose exec pulsar bin/pulsar-client produce orders \
  -s ';' -m '{"id":1,"amount_cents":1250}'
```

`-s ';'` 让消息保持完整：默认情况下，客户端会在每个逗号处把它拆开。
服务记录一条带订单 id 的 `order taken` 日志。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`PulsarBroker` 连接的是进程内 Pulsar，所以测试不需要服务器：

```bash
cargo test
```

[测试 Pulsar 服务](testing.md)逐一讲解这些测试，以及进程内 Pulsar 模拟了什么。

## 从 pulsar crate 迁移 {/* #coming-from-the-pulsar-crate */}

基于 [`pulsar`](https://docs.rs/pulsar) crate 的服务要自己构建客户端、消费者循环和生产者。
下表列出每一部分在 RustStream 中的位置：

| `pulsar` crate | RustStream |
|---|---|
| `Pulsar::builder(url, TokioExecutor).build().await` | [`PulsarBroker::new(url)`](../components/broker/index.md)，由运行时建立连接 |
| `.with_auth(Authentication { name: "token", .. })` | [`PulsarBroker::token`](../components/broker/security.md) |
| `.with_operation_retry_options(..)` | [`PulsarBroker::operation_retries`](../components/broker/operation-retries.md) |
| `client.consumer().with_topic(..).with_subscription(..)` 加上对消费者的循环 | [`PulsarSubscription`](../components/subscriptions/pulsar-subscription.md) 上的一个 `#[subscriber]` 处理器 |
| `.with_subscription_type(..)` | 描述符上的 [`subscription_type`](../components/subscriptions/subscription-type.md) |
| `.with_topics([..])`、`.with_topic_regex(..)` | [`PulsarSubscription::topics`](../components/subscriptions/topics.md)、[`PulsarSubscription::pattern`](../components/subscriptions/pattern.md) |
| 反序列化 `msg.payload.data` | 处理器的参数类型，由编解码器解码 |
| `consumer.ack(&msg)`、`consumer.nack(&msg)` | 处理器的结果（[消息确认](acknowledgement.md)） |
| `.with_dead_letter_policy(..)` | 挂载点上的 `max_attempts(n)` 和 `dead_letter(topic)`（[重试与死信](retries.md)） |
| `.with_unacked_message_resend_delay(..)` | 描述符上的 [`ack_timeout`](../components/settlement/ack-timeout.md) |
| `consumer.seek(..)` | 挂载点上的 [`start_at`](../components/positions/start-at.md)，或处理器中的[定位句柄](../components/positions/seek.md)（[重放与定位](replay.md)） |
| `client.producer().with_topic(..).build()` 和 `send_non_blocking` | 回复或 `Out` 参数，经由[发布策略](../components/publishing/index.md)发送（[发布与回复](publishing.md)） |
| `producer::Message { partition_key, properties, .. }` | [`partition_key`](../components/publishing/partition-key.md) 步骤；[消息头](../components/publishing/headers.md)变成属性 |
