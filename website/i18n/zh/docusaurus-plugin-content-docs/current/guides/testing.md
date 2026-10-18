---
title: 测试 Pulsar 服务
sidebar_position: 8
description: "用 TestApp 在进程内测试 Pulsar 服务：生产环境的应用、带键的付款、回复、延迟，以及在服务器上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 Pulsar 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 Pulsar](../components/testing/index.md) 或者运行中的服务器。

被测服务负责银行卡扣款。
付款到达 `card-payments`，它的分区键就是要扣款的账户。
处理器在 `card-receipts` 上以一张收据作为回复。
没有键的付款被丢弃。
超出银行即时限额的付款会被暂缓：处理器用 [`retry_after`](../components/settlement/retry-after.md) 要求一分钟后再投递它。
经过三次投递后，付款进入[死信主题](../components/settlement/dead-letters.md) `card-payments-review`。
应用由 `service(broker)` 构建，所以测试可以选择 Broker，而 `main` 使用生产环境的 Broker。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-pulsar` 的 `testing` feature。

## 带键的付款 {/* #a-keyed-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`PulsarBroker`](../components/broker/index.md) 连接到进程内 Pulsar。
服务 URL 按客户端的解析方式校验，但从不去连接它。
测试按生产类型访问 Broker：`tb.broker::<PulsarBroker>()`。
分区键以 [`PARTITION_KEY_HEADER`](../components/publishing/keys.md) 消息头传递，所以测试用 `with_headers` 设置它。
Broker 把这个消息头变成消息的键，处理器再从同一个消息头把它读回来。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card-receipts`。
测试主体把已启动的应用作为参数接收，所以下面在服务器上运行的测试也执行它。

## 没有键的付款 {/* #a-payment-without-a-key */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#keyless"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#keyless"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
一分钟的 `HOLD` 不花费任何时间。
`retry_after` 在延迟期间把付款扣留在服务中，然后否定确认它，所以 Broker 再次投递同一条消息。
重试不发布任何东西：`card-payments` 上唯一的消息就是测试自己的付款。
第三次投递是挂载点允许的最后一次，所以在它的等待结束后，这笔付款按到达时的样子进入 `card-payments-review`。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内 Pulsar 模拟了什么 {/* #what-the-in-process-pulsar-models */}

进程内 Pulsar 读取服务器会得到的配置：Broker 的设置、每个描述符，以及每个注册的重试声明。
服务器拒绝的东西，它同样拒绝，并返回真实 Broker 会返回的错误：客户端无法解析的服务 URL、不是主题名称的目的地、`Exclusive` 订阅的第二个消费者，以及关闭之后仍被使用的句柄。
它模拟的是 Pulsar，而不是一个队列：

- 主题就是它的完整名称，所以 `orders` 和 `persistent://public/default/orders` 是同一个主题。
- 每个订阅收到其主题的每条消息各一次，由它的[类型](subscription-types-and-ordering.md)选出消费者。
- 被否定确认的消息回到它的订阅，死信策略像客户端那样统计它的投递次数。
- 每个主题保存自己的日志，所以[定位](replay.md)会重放它；非持久主题不重放任何东西。
- 模式订阅从客户端的下一次列出开始读取新主题，即暂停的时钟上 30 秒之后。
- 消息头以文本属性传递，超过 `max_message_size` 的载荷会让发布失败。

服务器让第二个 `Exclusive` 消费者一直等待，而进程内 Pulsar 立即拒绝它，所以这样的测试会失败，而不是挂起。
确认超时之后的重新投递、分区主题、`KeyShared` 的键范围，以及订阅没有消费者时发布的消息，都需要服务器。

## 在服务器上运行同一个测试 {/* #the-same-test-on-a-server */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到运行中的 Pulsar。
测试用 `stand`（示例自带的辅助函数）从 `PULSAR_TEST_URL` 读取地址，未设置时跳过。
`RUSTSTREAM_REQUIRE_LIVE=1` 把这种跳过变成失败，所以 CI 任务没有服务器就无法通过。
测试加入自己的订阅，订阅以进程命名。
新订阅从主题末尾开始，所以不会读到之前运行留下的任何东西。
断言与进程内的完全相同。

```bash
cargo test
PULSAR_TEST_URL=pulsar://localhost:6650 cargo test -- --test-threads=1
```

服务器就是[快速开始](getting-started.md)中的那一个。
