---
title: 订阅
sidebar_position: 1
description: "Pulsar 订阅是 Broker 保存的一个具名游标；每个订阅都接收整个主题，订阅类型决定它的消费者如何共享它。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "同一主题上的两个订阅，各有自己的游标，以及在四种类型下一个订阅的消费者如何共享它",
  "text": "以文字阅读本图",
  "topic": "主题 orders",
  "audit": ["订阅 audit", "游标位于条目 1"],
  "billing": ["订阅 billing", "游标位于条目 5"],
  "next": "读取下一条",
  "types": "一个订阅的消费者，按订阅类型",
  "exclusive": "Exclusive",
  "exclusiveC1": "C1 收到每条消息",
  "exclusiveC2": "C2 等待 C1",
  "shared": "Shared",
  "sharedC1": "C1 收到条目 5",
  "sharedC2": "C2 收到条目 6",
  "failover": "Failover",
  "failoverC1": "C1 活跃，有序",
  "failoverC2": "C2 作为备用等待",
  "keyShared": "KeyShared",
  "keySharedC1": "键 A 发给 C1",
  "keySharedC2": "键 B 发给 C2",
  "durable": ["Broker 保存每个游标：订阅比它的消费者活得更久，", "并接收消费者不在时发布的每条消息"]
};

# 订阅

Pulsar 订阅是 Broker 保存的一个具名游标。每个订阅都接收整个主题，订阅类型决定它的消费者如何共享它。

<Diagram name="subscriptions" words={words} />

同一主题上的两个订阅互不影响。每个订阅都有自己的游标，接收每条消息。以同一个订阅名接入的消费者共享这一个游标。订阅是持久的：最后一个消费者离开后，它仍留在 Broker 上，积压不断增长，直到有消费者回来。

[`SubscriptionType`](api:ruststream_pulsar::SubscriptionType) 选择一个订阅的消费者如何共享它：

| 类型 | 消费者 | 顺序 |
|---|---|---|
| `Exclusive` | 一个；第二个等待 | 保持 |
| `Shared`（默认） | 全部，每条消息发给其中一个 | 不保证 |
| `Failover` | 一个活跃，其余备用 | 保持 |
| `KeyShared` | 全部，每个键发给其中一个 | 每个键内保持 |

`Exclusive` 订阅的第二个消费者会得到“consumer busy”的应答。客户端把这个应答视为可以重试的应答，默认不限次数地重试，间隔五秒。因此，第二个消费者会等待第一个离开，它所在的服务也就无法启动。[`OperationRetries`](api:ruststream_pulsar::OperationRetries) 限制尝试次数，此时订阅以 [`PulsarError::Subscribe`](api:ruststream_pulsar::PulsarError::Subscribe) 失败。

消费者第一次指定某个订阅时，服务器创建它，并把它定在最新的消息处：`pulsar` 客户端在没有其他指示时请求这个位置，本 crate 不改变它。要从别处开始的服务通过定位重新放置订阅，位置由 [`PulsarPosition`](api:ruststream_pulsar::PulsarPosition) 指定。定位移动的是保存的游标本身，因此它在每次启动时都生效，而不只是第一次。

基于主题列表或模式的订阅，在客户端内部是每个主题一个消费者，作为一个流读取。模式按完整名称匹配主题。客户端每 30 秒重新列出一次主题，因此订阅打开后才创建的主题会在下一次列出时加入。

这个模型来自 Apache Pulsar 本身（[订阅](https://pulsar.apache.org/docs/next/concepts-messaging/#subscriptions)、[订阅类型](https://pulsar.apache.org/docs/next/concepts-messaging/#subscription-types)）。消费者如何确认，见[确认](acknowledgement.md)。
