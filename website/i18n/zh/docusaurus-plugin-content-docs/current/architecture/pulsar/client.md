---
title: 客户端
sidebar_position: 3
description: "pulsar 客户端通过服务 URL 查找每个主题，每个 Broker 保持一个连接，并在流量许可范围内向每个消费者推送消息。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "pulsar 客户端：通过服务 URL 查找、每个 Broker 一个共用连接，以及连接上的生产者和消费者",
  "text": "以文字阅读本图",
  "url": ["PulsarBroker::new(url)", "一个服务 URL"],
  "lookup": ["通过服务 URL 查找：", "哪个 Broker 拥有该主题"],
  "pool": "连接池：每个 Broker 一个连接",
  "producer": ["生产者", "每个主题一个"],
  "consumer": ["消费者", "每个订阅一个"],
  "refresh": ["模式每 30 秒", "重新列出", "它的主题"],
  "permits": ["流量许可：", "预先授予 1000 条，", "不足一半时补充"],
  "cluster": ["Pulsar Broker", "每个主题归一个 Broker 所有"],
  "retries": ["服务器暂时拒绝的操作会重试：", "默认不限次数，间隔五秒"],
  "asks": "询问",
  "owner": "所有者的地址",
  "tcp": "TCP"
};

# 客户端

`pulsar` 客户端通过服务 URL 查找每个主题，每个 Broker 保持一个连接，并在流量许可范围内向每个消费者推送消息。

<Diagram name="client" words={words} />

Pulsar 集群把它的主题分散到各个 Broker 上，每个主题归一个 Broker 所有。客户端向服务 URL 询问是哪个 Broker，然后连接到这个所有者。对分区主题，客户端先查找它的分区数，再分别查找每个分区。服务 URL 是一个地址：DNS 名称、负载均衡器，或者位于各个 Broker 前面的 Pulsar 代理，都可以代表整个集群。

客户端为每个 Broker 地址保持一个连接并共用它。本 crate 为每个主题打开一个生产者，在第一次有东西向该主题发布时打开，连接上的每个发布者都使用它。每个订阅打开一个消费者。

消费者接收 Broker 推送来的消息。它预先授予 Broker 1000 条消息的许可，每当剩余不足一半时再授予更多，因此 Broker 发来的消息永远不会超过消费者能容纳的数量。本 crate 保留这个默认值。

服务器回答“暂时不行”的操作会重试，比如 Broker 重启期间的查找，或者 `Exclusive` 订阅的第二个消费者。客户端默认不限次数地重试，间隔五秒，每次尝试限时 30 秒。[`OperationRetries`](api:ruststream_pulsar::OperationRetries) 设置尝试次数、间隔和超时，作用于连接上的每个操作，包括消费者的重新连接。

客户端把它的连接和消费者作为服务的 tokio 运行时上的任务来运行。本 crate 在 Broker 建立连接的那个运行时上打开每个消费者和生产者，因此在独立线程上运行的处理器不会随着它的运行时一起带走某个连接。

协议来自 Apache Pulsar（[二进制协议](https://pulsar.apache.org/docs/next/developing-binary-protocol/)：服务发现和流量控制），由 [`pulsar` 客户端 6.9](https://docs.rs/pulsar/6.9.0/pulsar/) 实现。
