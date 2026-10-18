---
title: 主题与订阅
sidebar_position: 2
description: "为 Pulsar 主题命名，在持久订阅下读取它们，覆盖一个主题、一组固定的主题，或模式匹配到的每个主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 主题与订阅

本指南为 Pulsar 主题命名，在持久订阅下读取它们，覆盖一个主题、一组固定的主题，或模式匹配到的每个主题。

示例是一个履约服务。
它发运订单、通知顾客、汇总并审计各地区的订单，并为退货补充库存。

## 主题名称 {/* #topic-names */}

Pulsar 主题名称由四部分组成：`persistent://public/default/orders`。
第一部分是持久性，接着是[租户和命名空间](https://pulsar.apache.org/docs/next/concepts-multi-tenancy/)，最后是主题本身。
`orders` 这样的简单名称是 `persistent://public/default/orders` 的简写，`tenant/namespace/topic` 三段式名称同样是持久的。
`public/default` 是每个 Pulsar 安装一开始就有的命名空间。

持久主题会保存它的消息，直到每个订阅都确认了它们。
[非持久主题](https://pulsar.apache.org/docs/next/concepts-messaging/#non-persistent-topics)把消息保存在内存中，只交给当时已连接的消费者。
因此，它适合片刻之后就失去价值的数据。
Pulsar 在第一次使用主题时创建它，所以服务只需指定它读写的主题。

服务在连接之前检查每个主题名称，格式错误的名称会让服务在启动时停止。
[`PulsarTopic`](../components/subscriptions/pulsar-topic.md) 由各部分构建名称，或者解析来自代码之外的名称。

## 一个主题，两个订阅 {/* #one-topic-two-subscriptions */}

Pulsar 只通过[订阅](https://pulsar.apache.org/docs/next/concepts-messaging/#subscriptions)读取主题。
订阅是服务器保存的一个游标：它记住消费者确认过什么，并且比服务活得更久。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#topic"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#topic"
```

</TabItem>
</Tabs>

[`PulsarSubscription::new`](../components/subscriptions/pulsar-subscription.md) 指定主题和订阅。
`fulfil` 和 `notify` 在两个订阅下读取 `orders`，所以它们各自收到每一个订单。
服务的两个实例在每个订阅上共用一个游标，所以它们分摊订单，而不是把每个订单发运两次。
订阅以拥有它的服务命名：另一个服务如果使用同一个名称，就会共享它的积压。

新订阅从主题末尾开始，只看到在它之后发布的消息。
已有的订阅从它的消费者停下的地方继续，重启之后也是如此。
[重放与定位](replay.md)讲解如何移动游标。

## 一组固定的主题 {/* #a-fixed-list-of-topics */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#topics"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#topics"
```

</TabItem>
</Tabs>

[`PulsarSubscription::topics`](../components/subscriptions/topics.md) 通过一个消费者、在一个订阅名下读取多个主题。
每个主题在这个名称下保有自己的游标。
所有主题共用一种载荷类型。

## 模式 {/* #a-pattern */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#pattern"
```

</TabItem>
</Tabs>

[`PulsarSubscription::pattern`](../components/subscriptions/pattern.md) 读取完整名称与正则表达式匹配的每个主题，所以表达式要写出完整名称。
`audit` 读取 `orders-eu` 和 `orders-us`，以及之后创建的、完整名称与该表达式匹配的任何主题。
客户端每 30 秒重新列出一次主题。
新主题从发现它的那次列出开始读取，在那之前到达它的消息不会被投递。
表达式不是合法的正则表达式时，服务会在启动时停止。

## 简写形式 {/* #the-short-form */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#short"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#short"
```

</TabItem>
</Tabs>

`#[subscriber("returns")]` 只指定主题，不指定订阅。
它以 `Shared` 订阅的身份加入 Broker 的 [`default_subscription`](../components/subscriptions/index.md)，这里是 `fulfilment`。
在没有默认订阅的 Broker 上，简单名称会让服务在启动时停止，错误信息会指出这项设置。
订阅的其他所有设置，比如它的类型或确认超时，都需要描述符。

## 测试订阅 {/* #testing-the-subscriptions */}

示例文件中的测试在进程内运行应用。
和在服务器上一样，主题上的每个订阅都收到发布到该主题的每条消息。
模式测试在暂停的时钟上启动，并用 `tb.advance` 让时钟前进 30 秒，所以下一次列出会立即到来。
[测试 Pulsar 服务](testing.md)讲解了测试工具。
