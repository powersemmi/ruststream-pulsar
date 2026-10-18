---
title: 订阅类型与顺序
sidebar_position: 3
description: "选择 Pulsar 订阅的消费者如何共享它，保持每个键的顺序，并把带键的消息放到分区上。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 订阅类型与顺序

本指南选择 Pulsar 订阅的消费者如何共享它，保持每个键的顺序，并把带键的消息放到分区上。

示例是一个发货服务。
它为订单打包、跟踪发货、记账付款并为订单开具发票，这几项工作各自在不同的订阅类型下读取。

## 四种类型 {/* #the-four-types */}

订阅的每个消费者，都是服务某个实例中的一个处理器。
[订阅类型](https://pulsar.apache.org/docs/next/concepts-messaging/#subscription-types)决定这些消费者如何共享消息：

| 类型 | 谁收到消息 | 顺序 |
|---|---|---|
| `Shared`（默认） | 任一消费者，轮流 | 不保证 |
| `KeyShared` | 持有该消息键的消费者 | 每个键内保持 |
| `Exclusive` | 唯一的消费者；第二个等待它离开 | 主题的顺序 |
| `Failover` | 活跃的消费者；其余待命 | 主题的顺序 |

描述符上的 [`subscription_type`](../components/subscriptions/subscription-type.md) 设置类型。
同一个订阅的每个消费者都指定相同的类型。

## Shared：任一消费者 {/* #shared-any-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#shared"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#shared"
```

</TabItem>
</Tabs>

`Shared` 把每个订单交给 `packing` 的一个消费者，轮到哪个就是哪个。
服务的实例越多，同时打包的订单就越多，两个订单可能以任意顺序完成。
这适合消息之间互不依赖的工作。

`pack` 为每个订单发布一个发货事件。
[`partition_key`](../components/publishing/partition-key.md) 步骤以订单 id 作为事件的键。
这个步骤来自 Broker 的 [prelude](../components/broker/prelude.md)，所以槽位要以 `Publisher<Options = PulsarPublishOptions>` 为约束才能提供它。
Broker 保存了消息之后，[发布](../components/publishing/index.md)才返回。

## KeyShared：每个键内有序 {/* #keyshared-in-order-per-key */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#key_shared"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#key_shared"
```

</TabItem>
</Tabs>

`KeyShared` 把同一个键的所有消息交给一个消费者，所以同一订单的事件按 `pack` 发布它们的顺序到达 `track`。
不同的键分散到各个消费者上，所以各实例分担工作。
服务器分配这些键，并在消费者加入或离开时移动其中一部分。

在一个实例内部，`workers(8, by_key)` 运行八个工作分区，同一个键的投递在同一个工作分区上依次运行。
工作分区的键就是消息的分区键，所以每个键的顺序在进程内同样保持。
在手写形式中，同一设置是 [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key)。
键以 [`partition-key` 消息头](../components/publishing/keys.md)的形式到达处理器。

## Exclusive：一个消费者 {/* #exclusive-one-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#exclusive"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#exclusive"
```

</TabItem>
</Tabs>

`Exclusive` 只让一个消费者读取订阅，所以 `book` 按收款的顺序看到各笔付款。
服务器以“忙”拒绝第二个消费者，客户端则不断重新请求，直到第一个消费者离开，或者 [`OperationRetries`](../components/broker/operation-retries.md) 的次数用完，而默认情况下次数永远不会用完。
因此，服务的第二个实例无法启动，并且在等待期间不报告任何东西。
`Exclusive` 订阅属于只运行一个实例的服务。

## Failover：一个活跃的消费者 {/* #failover-one-active-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#failover"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#failover"
```

</TabItem>
</Tabs>

`Failover` 接受每个消费者，但只向其中一个投递。
其余的是热备：活跃的消费者断开时，服务器把它的消息交给下一个。
多个实例既保持了 `Exclusive` 订阅的顺序，又没有它的单点故障。
在分区主题上，每个分区有自己的活跃消费者，所以各实例分担这些分区。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subscription_types_and_ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subscription_types_and_ordering.rs#app"
```

</TabItem>
</Tabs>

[`OperationRetries::attempts`](../components/broker/operation-retries.md) 限制客户端为服务器尚未接受的操作重新请求的次数。
设为三次尝试时，`ledger` 的第二个消费者会让服务在启动时停止，错误信息会指出主题。
这个上限作用于连接上的每个操作，包括 Broker 重启后的重新连接。
因此，它用尽早报错取代了等待故障结束。

## 分区主题 {/* #partitioned-topics */}

[分区主题](https://pulsar.apache.org/docs/next/concepts-messaging/#partitioned-topics)是同一个名称下的多个主题，每个分区单独提供服务。
管理员在创建它时指定分区数：

```bash
docker compose exec pulsar bin/pulsar-admin topics create-partitioned-topic \
  persistent://public/default/shipments -p 3
```

订阅读取主题的每个分区，服务用与其他主题相同的描述符读取它。
带键的消息进入其键的哈希所对应的分区，所以同一个键的消息留在一个分区里，并保持顺序。
不带键的消息轮流进入各个分区。

## 测试类型 {/* #testing-the-types */}

示例文件中的测试在进程内运行应用。
在那里，每个订阅收到其主题的每条消息各一次，由订阅类型选出消费者。
进程内 Pulsar 会立即拒绝第二个 `Exclusive` 消费者，所以测试会失败，而不是一直等待。
分区、`KeyShared` 的键范围以及服务器选中哪个消费者，都由服务器决定，本 crate 的测试在运行中的 Pulsar 上检查它们。
