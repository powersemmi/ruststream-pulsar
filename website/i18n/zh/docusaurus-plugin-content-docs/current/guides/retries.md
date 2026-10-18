---
title: 重试与死信
sidebar_position: 5
description: "立即或稍后重试一条 Pulsar 消息，限定投递次数，并把一直失败的消息转入由处理器读取的死信主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信

本指南立即或稍后重试一条 Pulsar 消息，限定投递次数，并把一直失败的消息转入由处理器读取的[死信主题](../components/settlement/dead-letters.md)。

Pulsar 自己重新投递消息，所以重试不发布任何东西。
挂载点声明上限和死信主题，Pulsar 客户端负责执行它们。

示例是一个退款服务，它通过发卡处理商发出退款。
`Processor` 代表处理商的客户端，应用在启动时打开它。

## 订阅者 {/* #the-subscriber */}

处理商有三种拒绝退款的方式，每种需要不同的结果。
`Busy` 表示它的速率限额已经用完：立即重试仍会被拒绝，所以 [`retry_after`](../components/settlement/retry-after.md) 等待 30 秒。
`Unavailable` 表示它没有应答：`retry` 立即再试一次。
`Declined` 表示卡账户已经关闭，再试一次也是同样的答复，所以这笔退款被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 否定确认这笔退款，Broker 立即再次投递它。
[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) 在延迟期间把退款以未确认的状态扣留在服务中，然后否定确认它。
Pulsar 的否定确认本身不带延迟，所以等待由进程负责。
在等待期间停止的进程丢失的是这段等待，而不是这笔退款：Broker 会在[确认超时](../components/settlement/ack-timeout.md)之后再次投递它，消费者断开连接时则立即投递。

延迟必须短于订阅的确认超时，否则超时会先把退款带回来。
处理器返回更长的延迟时，它会被拒绝，错误信息会给出这两个值。
没有确认超时的订阅接受任意延迟。

## 上限与死信主题 {/* #the-cap-and-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 统计一笔退款的每次投递，第一次也算在内。
[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 指定投递次数用完后退款的去向。
这两项合起来成为消费者自己的[死信策略](https://pulsar.apache.org/docs/next/concepts-messaging/#dead-letter-topic)。
Pulsar 客户端统计重新投递的次数，把第五次失败的退款发布到 `refunds-dlq`，并确认原消息。
`retry`、`retry_after` 和到期的确认超时，各自都算作一次重新投递。
被丢弃的退款完成结算，不会去任何地方；死信主题只存放用完了尝试次数的退款。

两个步骤要么都写，要么都不写：有上限而没有目的地，或者有目的地而没有上限，都会让服务在启动时停止，错误信息会指出缺少的步骤。
Broker 只在 `Shared` 或 `KeyShared` [订阅](../components/subscriptions/subscription-type.md)上统计重新投递，所以上限属于这两种订阅。
`Exclusive` 或 `Failover` 订阅上的上限永远不会达到，这样的注册会让服务在启动时停止，错误信息会指出订阅类型。
以简单主题名称挂载的处理器接受同样的声明，作用在 Broker 的[默认订阅](../components/subscriptions/index.md)上。

## 读取死信主题 {/* #reading-the-dead-letter-topic */}

死信主题是一个普通主题，`review` 像读取其他主题一样读取它。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

转入死信的退款保留它到达时的载荷和消息头。
客户端加上两个属性：`REAL_TOPIC` 是退款来自的主题，`ORIGIN_MESSAGE_ID` 是它在那个主题中的位置。
`review` 把这笔退款记入日志，留给人来查看。

## 测试延迟 {/* #testing-the-delay */}

示例文件中的测试用 `TestApp::with_state` [在进程内](../components/testing/index.md)运行应用，它交给处理器一个故意失败的处理商客户端。
延迟测试在暂停的时钟上启动，所以 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
进程内 Pulsar 统计投递次数，并像客户端那样把用完次数的退款移到 `refunds-dlq`。
[测试 Pulsar 服务](testing.md)讲解了测试工具。
