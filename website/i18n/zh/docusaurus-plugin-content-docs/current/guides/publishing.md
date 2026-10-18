---
title: 发布与回复
sidebar_position: 7
description: "回复 Pulsar 消息，通过 Out 槽位发布带键和消息头的消息，并在服务启动后发布。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 发布与回复

本指南回复 Pulsar 消息，通过 `Out` 槽位发布带键和消息头的消息，并在服务启动后发布。

示例是一个结账服务。
它用收据确认订单，回答报价请求，向仓库发送拣货单，并在启动时宣告自己上线。

## 回复到声明的主题 {/* #a-reply-to-a-declared-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#reply"
```

</TabItem>
</Tabs>

`confirm` 返回一个 `Receipt`，由框架发布它。
`#[outgoing(name = "receipts")]` 在类型上固定了主题，所以订阅者只写不带参数的 `reply` 子句；在手写形式中是 `.reply()`。
Broker 的[发布策略](../components/publishing/index.md)，即 prelude 中的 `Publish`，是它的默认策略，所以普通的 `include` 就通过它回复。
Broker 保存了消息之后，发布才返回。

## 回复到挂载点指定的主题 {/* #a-reply-to-a-topic-the-mount-site-names */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#named_reply"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#named_reply"
```

</TabItem>
</Tabs>

`Quote` 没有指定主题，所以由 `reply("quotes")` 在挂载点上指定；在手写形式中是 `.reply().to("quotes")`。
这样，同一个类型在另一个服务中可以回复到另一个主题。

请求和它的应答经由两个普通主题传递。
请求报价的服务向 `quote-requests` 发布，并在自己的订阅下读取 `quotes`。
它通过两条消息共有的 `request` id，把每个报价与它的请求对应起来。

## 带键和消息头的发布 {/* #a-keyed-publish-with-headers */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#slot"
```

</TabItem>
</Tabs>

[`Out` 槽位](api:ruststream::runtime::Out)在处理器的函数体中发布，`Picks` 声明它承载的类型。
[`partition_key`](../components/publishing/partition-key.md) 以仓库作为每张拣货单的键。
在分区主题上，键决定分区，`KeyShared` 订阅保持每个键的顺序。
这个步骤来自 Broker 的 prelude，所以槽位要以 `Publisher<Options = PulsarPublishOptions>` 为约束才能提供它。

每个[消息头](../components/publishing/headers.md)都变成一个同名的消息属性。
用其他语言编写的消费者把 `source` 当作普通的 Pulsar 属性读取，载荷外面没有任何信封。
Pulsar 以文本形式传递属性，所以不是 UTF-8 的消息头值会让发布失败。

失败的发布返回错误，`dispatch` 随后重试这个订单。
载荷超过 Broker 的 [`max_message_size`](../components/broker/max-message-size.md)（默认 5 MiB）时，发布在发送之前就会失败。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`.out(Picks, Publish)` 把槽位连接到发布策略。
`after_startup` 在各个订阅打开之后运行一次，并得到它指定的策略的一个发布者，`checkout` 用它在 `service-events` 上宣告自己上线。
失败的钩子会让服务在启动时停止。

发布者在第一次向某个主题发布时为它打开一个生产者，`shutdown` 关闭这些生产者。
应用之外的代码从 Broker 取得发布者：服务启动之前用 [`PulsarBroker::publisher`](api:ruststream_pulsar::PulsarBroker::publisher)，启动之后用 [`ConnectedPulsarBroker::publisher`](api:ruststream_pulsar::ConnectedPulsarBroker::publisher)。

## 测试发布 {/* #testing-the-publishes */}

示例文件中的测试在进程内运行应用。
`published` 检查到达某个主题的内容及其消息头，`tb.out::<Picks>()` 检查槽位的发布所带的键。
进程内 Pulsar 像客户端那样封装消息头和键，所以服务器会拒绝的消息头同样会让测试失败。
