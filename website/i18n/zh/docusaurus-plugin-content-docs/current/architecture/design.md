---
title: 设计
sidebar_position: 2
description: "ruststream-pulsar 的位置：在 RustStream 运行时与 pulsar 客户端之间，每个订阅一个消费者和一个驱动任务，每个主题一个生产者。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、pulsar 客户端和 Pulsar 集群",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算、批",
  "crate": "ruststream-pulsar",
  "subscriber": ["PulsarSubscriber", "及其驱动任务"],
  "publishers": ["PulsarPublisher", "每个主题一个生产者"],
  "message": "PulsarMessage",
  "seeker": "PulsarSeeker",
  "broker": ["PulsarBroker", "配置"],
  "descriptors": ["PulsarSubscription,", "PulsarTopic"],
  "client": "pulsar 客户端",
  "consumer": ["Consumer", "每个订阅一个"],
  "producer": ["Producer", "每个主题一个"],
  "cluster": "Pulsar 集群",
  "inProcess": ["启用 testing 特性时，进程内传输", "可以代替客户端"],
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-pulsar` 所处的位置：在 RustStream 运行时与 `pulsar` 客户端之间，每个订阅一个消费者和一个驱动任务，每个主题一个生产者。

<Diagram name="design" words={words} />

本 crate 在 [`pulsar`](https://docs.rs/pulsar/6.9.0/pulsar/) 客户端之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。Pulsar 特有的一切都在本 crate 里：[描述符](pulsar/subscriptions.md)、[每种结果发送什么](settlement.md)、死信策略，以及[定位可以移到哪些位置](api:ruststream_pulsar::PulsarPosition)。

已连接的 Broker 持有一个客户端。同一连接上的每个发布者都共用这个客户端及其生产者。生产者每个主题一个，在第一次向该主题发布时打开。

每个订阅打开一个客户端消费者，由一个属于它自己的任务驱动。客户端通过消费者发出确认，而消费者同一时刻只能由一个所有者使用；投递却要在处理器所在的任何地方结算。因此，消费者归驱动任务所有。驱动任务通过一个容量为 16 的通道把每个投递交出去，再收回投递发给它的确认和定位请求。[`PulsarMessage`](api:ruststream_pulsar::PulsarMessage) 带着通往其驱动任务的回路，[`PulsarSeeker`](api:ruststream_pulsar::PulsarSeeker) 也走这条回路。

客户端一次交出一条消息。批处理器收到的批由运行时在服务中组装，[工作分区](lanes.md)一页用图说明了这一点。

重试由 Broker 完成：投递得到否定确认，Broker 再次投递它。服务不需要发布任何东西把它送回来，因此这个 Broker 上的注册没有需要声明的重试发布者，[结算](settlement.md)一页用图说明了这一点。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。

## 进程内传输 {/* #the-in-process-transport */}

启用 `testing` 特性时，[`TestApp::start`](api:ruststream::testing::TestApp::start) 把同一个 Broker 连接到进程内传输，而不是服务器。描述符、策略和投递都是生产环境中的那些，它们的设置也一样。该传输模拟以下内容：完整的主题名；基于一个主题、一个列表或一个模式的订阅；四种订阅类型；否定确认；客户端的死信策略；按运行时时钟进行的延迟重试；以及定位所移动的保留日志。服务器拒绝的，它也拒绝：超过 `max_message_size` 的载荷、`Exclusive` 订阅的第二个消费者、关闭后仍被使用的句柄。

服务器自身的行为留给真实环境的运行：`KeyShared` 的哈希范围、服务器为 `Shared` 订阅挑选哪个消费者、确认超时后的重新投递、分区主题，以及订阅没有消费者时发布的消息。

不启用该特性时，这个传输不参与编译。在生产构建中，Broker 的传输与客户端大小完全相同，订阅的流和投递的结算也与它们所封装的通道大小完全相同。编译器会检查这些大小。
