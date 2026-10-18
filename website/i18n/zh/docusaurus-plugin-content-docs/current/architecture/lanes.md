---
title: 工作分区
sidebar_position: 5
description: "一个订阅通过一个消费者读取它的全部主题和分区；workers(n, by_key) 把消息分到按分区键划分的工作分区上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：三个分区、一个消费者、一个流，以及按分区键划分的两个工作分区",
  "text": "以文字阅读本图",
  "p0": "-partition-0",
  "p1": "-partition-1",
  "p2": "-partition-2",
  "consumer": ["订阅的消费者", "它读取的每个主题和分区"],
  "stream": ["PulsarSubscriber 流", "一次一条消息"],
  "keyed": ["workers(2, by_key)", "工作分区 = partition-key 消息头的哈希"],
  "lane0": ["工作分区 1", "键 A 和 C，", "各自有序"],
  "lane1": ["工作分区 2", "键 B，有序"],
  "unkeyed": ["没有键的消息也就没有分区键：", "这样的消息轮流分配到各个工作分区"],
  "keyShared": ["在多个进程之间，KeyShared 订阅", "把每个键留在一个消费者上"],
  "pushed": "推送来的消息",
  "oneAtATime": "一次一条"
};

# 工作分区

一个订阅通过一个消费者读取它的全部主题和分区；`workers(n, by_key)` 把消息分到按分区键划分的工作分区上。

<Diagram name="lanes" words={words} />

订阅的流一次产出一条消息，不管它来自哪个主题或分区。不用 `workers` 时，一个循环依次为每条消息运行处理器，没有任何东西并行运行。

`workers(n, by_key)` 运行 `n` 个工作分区。消息进入其键的哈希所对应的工作分区，每个工作分区按顺序运行自己的消息。键就是消息的分区键，投递把它放在 `partition-key` 消息头中，因此同一个键的消息保持顺序。没有键的消息也就没有分区键，这样的消息轮流分配到各个工作分区。

工作分区在一个进程内部安排工作的顺序。在多个进程之间，[`KeyShared`](pulsar/subscriptions.md) 订阅把每个键留在一个消费者上，因此一个服务的多个实例可以共享一个主题，同时仍保持每个键的顺序。

通过 [`PulsarSeeker`](api:ruststream_pulsar::PulsarSeeker) 进行的定位会移动整个订阅：它的消费者读取的每个主题和每个分区。

批处理器收到的批由运行时在服务中组装，因为客户端一次交出一条消息。一个批最多容纳挂载处指定的大小。批在装满时关闭，或者在第一条消息到达 10 毫秒后关闭，这个时长就是描述符的 [`batch_wait`](api:ruststream_pulsar::PulsarSubscription::batch_wait)。
