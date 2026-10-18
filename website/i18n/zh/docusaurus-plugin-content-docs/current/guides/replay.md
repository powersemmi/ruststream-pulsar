---
title: 重放与定位
sidebar_position: 6
description: "再次读取 Pulsar 主题保留的日志：每次启动都从头读取，或者从处理器定位到的位置读取。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重放与定位

本指南再次读取 Pulsar 主题保留的日志：每次启动都从头读取，或者从处理器定位到的位置读取。

订阅是主题日志上的一个游标，由服务器保存。
定位移动这个游标，订阅从新的位置继续读取。

示例是一个搜索服务。
它维护由账簿构建的账户余额，以及订单的搜索索引。
`Search` 代表搜索集群的客户端。

## 位置 {/* #positions */}

[`PulsarPosition`](../components/positions/index.md) 指定主题日志中的一个位置：

| 位置 | 在哪里 |
|---|---|
| `PulsarPosition::earliest()` | 保留的最旧消息 |
| `PulsarPosition::latest()` | 日志末尾：只有定位之后发布的消息 |
| `PulsarPosition::timestamp(millis)` | 在该时刻或之后发布的第一条消息，时间以自 Unix 纪元起的毫秒数表示 |
| `PulsarPosition::MessageId(..)` | 一条已投递的消息，从它的 [`Position`](../components/context/index.md) 读取 |

## 每次启动都从头开始 {/* #every-startup-from-the-start */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#start_at"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#start_at"
```

</TabItem>
</Tabs>

[`start_at`](../components/positions/start-at.md) 在服务每次启动时对订阅执行定位。
`balances` 每次启动都从保留的最旧条目开始读取账簿，所以余额从零开始重建。
没有 `start_at` 时，已有的订阅从它的消费者停下的地方继续，新订阅则从主题末尾开始。

Pulsar 有自己的初始位置，但它只在创建订阅时生效。
`start_at` 对已经存在的订阅同样生效。
每个启动的实例都会移动共用的游标，所以带 `start_at` 的服务，其任一实例启动时，各实例都会重放日志。

## 在处理器中定位 {/* #a-seek-from-the-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#seek"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#seek"
```

</TabItem>
</Tabs>

[`SeekHandle`](../components/positions/seek.md) 是订阅自己的定位器，`index` 从 [Pulsar 上下文](../components/context/index.md)读取它。
搜索集群丢失索引时，`index` 定位到保留的最旧订单，每个订单都会被重新索引。
在手写形式中，处理器把 `PulsarContext` 声明为它的上下文，并用 `ctx.context(SeekHandle)` 读取定位器。

定位移动的是整个订阅：它的每个消费者、它读取的每个主题和每个分区。
已经为订阅排队的投递会被丢弃，日志从新的位置开始读取。

另外两种定位很常用：

- `PulsarPosition::timestamp(millis)` 跳过一段坏数据：订阅从这段之后发布的第一条消息继续。
- 用 `Ctx<Position>` 读取的投递位置，会再次投递恰好这条消息，然后按顺序投递日志的其余部分。

[批处理器](../components/context/batch-context.md)以同样的方式通过 `PulsarBatchContext` 定位。
它带有定位器，但没有位置，因为一批跨越许多消息。

## 重放能读到什么 {/* #what-a-replay-reaches */}

重放读取的是主题仍然保存的内容。
默认情况下，每个订阅都确认了一条消息之后，Pulsar 就删除它。
命名空间上的[保留策略](https://pulsar.apache.org/docs/next/cookbooks-retention-expiry/)按大小或时间保存已确认的消息：

```bash
docker compose exec pulsar bin/pulsar-admin namespaces set-retention public/default \
  --size 1G --time 3d
```

非持久主题什么都不保存，所以在那里定位不会重放任何东西。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#app"
```

</TabItem>
</Tabs>

`service` 在传给它的 Broker 上构建应用，所以测试可以在应用启动之前从这个 Broker 取得一个发布者。

## 测试定位 {/* #testing-a-seek */}

示例文件中的测试在进程内运行应用。
进程内 Pulsar 保存每个主题的日志，所以在那里定位会像在服务器上一样重放日志。
测试通过 `broker.publisher()` 一次性发布三个订单，所以在第一个订单被处理之前，三个订单都在订阅中等待。
随后，`tb.settle()` 等到订阅空闲。
之前运行服务留下的积压只存在于服务器上，所以基于这种积压的 `start_at` 由本 crate 在运行中的服务器上测试。
