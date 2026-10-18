---
title: 路径
sidebar_position: 4
description: "一条 Pulsar 消息在 RustStream 服务中的路径：消费者、驱动任务、处理器、应答和确认。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条需要应答的 Pulsar 消息的路径：从客户端消费者到它的确认",
  "text": "以文字阅读本图",
  "consumer": ["客户端消费者", "在流量许可内推送来的消息"],
  "take": ["PulsarMessage，由驱动任务构建", "载荷复制进 Bytes"],
  "dispatch": ["分发", "一个循环，或每个键一个工作分区"],
  "decode": ["解码", "从复制出的载荷读取"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "encode": ["应答", "编码进交给客户端的缓冲区"],
  "publisher": ["PulsarPublisher", "主题的生产者，等待回执"],
  "settle": ["结算", "发给驱动任务"],
  "ack": ["确认", "写入连接，没有应答"]
};

# 路径

一条 Pulsar 消息在 RustStream 服务中的路径：消费者、驱动任务、处理器、应答和确认。

<Diagram name="path" words={words} />

客户端消费者持有 Broker 在流量许可范围内预先推送来的消息。订阅的驱动任务取出下一条，构建一个 [`PulsarMessage`](api:ruststream_pulsar::PulsarMessage)：它把载荷复制进一个 `Bytes`，保留主题名和消息 id（处理器通过它的[上下文](../components/context/index.md)读取这两者），并把消息属性转换为[消息头](../components/publishing/headers.md)。消息的分区键变成 [`partition-key` 消息头](../components/publishing/keys.md)，工作分区读取的就是它。投递在一个容量为 16 的通道中等待，直到分发循环取走它。

处理器的应答编码进一个缓冲区，[发布者](../components/publishing/index.md)把这个缓冲区交给客户端，不做复制。超过 [`max_message_size`](../components/broker/max-message-size.md) 的载荷在发送之前就会失败：客户端不检查大小，而服务器遇到过大的消息会断开整个连接。Broker 存储了消息并发回回执之后，发布才返回。应答发布失败时，这次投递以重试的方式结算。

投递在应答之后[结算](../components/settlement/index.md)。结算回到驱动任务，由它交给客户端，客户端把它放入队列后调用即返回。客户端把确认写入连接，不等待任何应答。

[基准测试](benchmarks/index.md)统计这条路径在每条消息上的开销，以及其中有多少属于客户端。
