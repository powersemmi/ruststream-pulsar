---
title: 消息确认
sidebar_position: 4
description: "按处理器的结果结算 Pulsar 消息，用确认超时限制未结算的投递，并一次结算整批消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 消息确认

本指南按处理器的结果结算 Pulsar 消息，用确认超时限制未结算的投递，并一次结算整批消息。

示例是一个库存服务。
它为订单预留库存，并记录仓库盘点扫描得到的库存数量。
`Stock` 代表库存服务的客户端，应用在启动时打开它。

## 结果即结算 {/* #the-outcome-is-the-settlement */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#reserve"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#reserve"
```

</TabItem>
</Tabs>

`reserve` 返回的值决定这条消息在 Pulsar 上的去向：

| 结果 | 在 Pulsar 上 |
|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | 消息被[确认](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment)，不再回来 |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | 消息被[否定确认](https://pulsar.apache.org/docs/next/concepts-messaging/#negative-acknowledgment)，并立即再次投递 |
| [`retry_after(delay)`](../components/settlement/retry-after.md) | 消息被扣留 `delay`，然后被否定确认 |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | 消息被确认 |

没有应答的库存服务，下一次调用时可能就会应答，所以 `reserve` 选择重试。
消息回到订阅，在 `Shared` 订阅上，可能由另一个消费者接手。
没有任何商品使用的 SKU 始终是未知的，所以 `reserve` 丢弃这次预留。
Pulsar 没有永久拒绝消息的命令，所以 `drop` 确认消息，消息就此消失。
一直失败的消息改为经由[死信主题](retries.md)离开订阅。

客户端在后台发送确认。
返回的结算先在消费者上排队，片刻之后 Broker 才记录它。
服务如果在这之间停止，下次启动时会再次收到这条消息，所以 Pulsar 对每条消息至少投递一次。
处理消息时，要让第二次投递不造成损害，比如以订单 id 作为工作的键。

## 确认超时 {/* #the-acknowledgement-timeout */}

无人结算的投递会一直留在它的消费者那里。
描述符上的 [`ack_timeout`](../components/settlement/ack-timeout.md) 为它设定期限：期限一过，客户端就请求 Broker 再次投递这条消息。
没有设置时，这样的消息只在它的消费者断开连接后才会回来。

把超时设得比处理器的最长耗时更长。
超时到期时仍在工作的处理器，会收到同一条消息的第二次投递。
和 `retry` 一样，每次重新投递都计入[重试上限](retries.md)。

## 批 {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#counts"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#counts"
```

</TabItem>
</Tabs>

接收切片的处理器是[批处理器](../components/subscriptions/batches.md)。
Pulsar 客户端一次交出一条消息，所以由服务组装批。
批满了，或者从第一条消息起已过去 [`batch_wait`](api:ruststream_pulsar::PulsarSubscription::batch_wait) 的时间（这里是 250 毫秒），批就结束。
一个结果结算批中的每条消息。
在手写形式中，`Ok(())` 确认整批，`Err(outcomes)` 用各自的结果分别结算每条消息。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

挂载点上的 [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) 是最大的批，这里是 64 条盘点记录。
`on_startup` 只打开一次客户端，处理器把它们作为应用的状态读取。

## 测试结果 {/* #testing-the-outcomes */}

示例文件中的测试[在进程内](../components/testing/index.md)运行应用，用 `TestApp::with_state` 代替 `on_startup`。
它们故意让库存服务失败，并检查每次投递的结果。
和服务器一样，进程内 Pulsar 会把被否定确认的消息交还给订阅。
确认超时之后的重新投递由服务器完成，本 crate 的测试在运行中的 Pulsar 上检查它。
