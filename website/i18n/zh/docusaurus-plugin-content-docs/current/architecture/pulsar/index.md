---
title: 主题
sidebar_position: 0
description: "Pulsar 主题位于某个租户的命名空间中；持久化主题把消息保存在 BookKeeper 中，分区主题就是每个分区一个主题。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "租户包含命名空间，命名空间包含一个分区主题、一个持久化主题和一个非持久化主题，生产者向分区主题发布",
  "text": "以文字阅读本图",
  "producer": ["生产者", "由键选择分区"],
  "tenant": "租户 acme",
  "namespace": "命名空间 acme/orders",
  "payments": "persistent://acme/orders/payments",
  "p0": "-partition-0",
  "p1": "-partition-1",
  "p2": "-partition-2",
  "created": "persistent://acme/orders/created",
  "ledgers": ["保存在", "BookKeeper ledger 中"],
  "ticks": "non-persistent://acme/orders/ticks",
  "live": "不保存任何内容：只发给此刻已连接的消费者",
  "bare": ["只有名字时，就是默认命名空间中的持久化主题：", "orders 即 persistent://public/default/orders"],
  "publishes": "发布"
};

# 主题

Pulsar 主题位于某个租户的命名空间中。持久化主题把消息保存在 BookKeeper 中，分区主题就是每个分区一个主题。

<Diagram name="topics" words={words} />

主题的完整名称是 `{persistent|non-persistent}://tenant/namespace/topic`。租户和命名空间是 Pulsar 的管理单位：权限、保留策略和其他策略都设置在它们上面。[`PulsarTopic`](../../components/subscriptions/pulsar-topic.md) 在任何 I/O 之前解析名称，像 `orders` 这样只有名字的写法会变成 `persistent://public/default/orders`。

持久化主题把每条消息写入 BookKeeper ledger。消息一直保留在那里，直到每个订阅都确认了它，之后再按命名空间的保留策略保留一段时间。非持久化主题什么也不保存：消息发给此刻已连接的消费者，之后才连接的消费者永远看不到它。

分区主题是一组名为 `<topic>-partition-<n>` 的内部主题，每个都是一个独立的日志，各自归集群中的一个 Broker 所有。生产者为每条消息选择分区。本 crate 的生产者用消息[键](../../components/publishing/keys.md)的哈希选择分区，因此同一个键的消息保持顺序；没有键的消息则轮流分配到各个分区。来自分区主题的投递在它的[主题名](api:ruststream_pulsar::PulsarMessage::topic)中给出分区。

生产者或消费者第一次指定某个持久化主题时，服务器就会创建它，除非集群关闭了自动创建。分区主题由管理员创建，同时指定分区数。

这个模型来自 Apache Pulsar 本身（[主题](https://pulsar.apache.org/docs/next/concepts-messaging/#topics)、[多租户](https://pulsar.apache.org/docs/next/concepts-multi-tenancy/)、[分区主题](https://pulsar.apache.org/docs/next/concepts-messaging/#partitioned-topics)、[非持久化主题](https://pulsar.apache.org/docs/next/concepts-messaging/#non-persistent-topics)）。消费者如何读取主题，见[订阅](subscriptions.md)。
