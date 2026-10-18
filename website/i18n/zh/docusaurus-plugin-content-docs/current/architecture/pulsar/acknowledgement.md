---
title: Pulsar 上的确认
sidebar_label: 确认
sidebar_position: 2
description: "Pulsar 消费者确认一条消息，或者请求再次投递它；客户端在超时后也会请求，并把回来次数过多的消息移到一边。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Pulsar 上的一次投递：客户端的死信检查，然后是 ack、否定确认或没有应答，以及后两者都会请求的重新投递",
  "text": "以文字阅读本图",
  "delivery": ["Broker 投递一条消息", "附带重新投递计数 n"],
  "check": ["客户端的死信策略：", "n 是否已达到 max_redeliver_count？"],
  "deadLetter": ["到达上限：", "移到", "死信", "主题，原消息", "已确认"],
  "handler": ["低于上限：", "处理器收到这条消息"],
  "ack": ["ack：", "游标越过", "这条消息"],
  "nack": ["nack：", "立即请求", "重新投递"],
  "timeout": ["在 ack_timeout 内", "没有 ack：", "客户端请求"],
  "again": ["Broker 再次投递它", "计数为 n + 1"],
  "counted": ["只有 Shared 和 KeyShared 统计重新投递：", "其他类型投递每条消息时计数都是 0"]
};

# 确认

Pulsar 消费者确认一条消息，或者请求再次投递它。客户端在超时后也会请求，并把回来次数过多的消息移到一边。

<Diagram name="redelivery" words={words} />

确认把一条消息标为已完成，游标越过每一条已完成的消息。Pulsar 还有累积确认，它把截至某条消息的全部消息标为已完成，用于 `Exclusive` 和 `Failover` 订阅。不论订阅类型如何，本 crate 都逐条确认消息。确认是写入连接的一条命令，Broker 不对它作出应答。

否定确认请求 Broker 再次投递一条消息；处理器的 [`retry`](../../components/settlement/index.md) 发出的就是它。Rust 客户端立即发送这个请求；Java 客户端则先等待一段可配置的延迟。Broker 再次投递这条消息，发给同一个消费者或该订阅的另一个消费者。

[确认超时](../../components/settlement/ack-timeout.md)是客户端的计时器。设置了超时时，客户端记住它交出每条消息的时间，每秒检查两次，并请求 Broker 再次投递每条等待时间超过超时的消息。没有设置时，无人应答的消息一直留在消费者那里，直到消费者断开连接，然后回到订阅。

在 `Shared` 和 `KeyShared` 订阅上，Broker 统计它重新投递一条消息的次数。[死信策略](../../components/settlement/dead-letters.md)属于客户端：消息到达时计数已到上限，客户端就把它生产到死信主题，加上 `REAL_TOPIC` 和 `ORIGIN_MESSAGE_ID` 属性，并确认原消息。处理器永远看不到它。

Pulsar 没有永久拒绝消息的命令。消息离开订阅的途径有三种：确认、死信策略，或者命名空间的存活时间（TTL）使它过期。

这一行为来自 Apache Pulsar（[确认](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment)、[否定确认](https://pulsar.apache.org/docs/next/concepts-messaging/#negative-acknowledgment)、[确认超时](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment-timeout)、[死信主题](https://pulsar.apache.org/docs/next/concepts-messaging/#dead-letter-topic)），由 `pulsar` 客户端 6.9 实现。处理器的每种结果发送什么，见[结算](../settlement.md)。
