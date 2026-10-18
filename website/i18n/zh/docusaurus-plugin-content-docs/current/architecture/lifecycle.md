---
title: 生命周期
sidebar_position: 3
description: "PulsarBroker::new 只记录配置，connect 构建客户端，启动时为每个订阅打开一个消费者，关闭时关闭生产者。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的各个状态作为类型，以及每次转换在 Pulsar 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "pulsar": "在 Pulsar 上",
  "unconnected": ["PulsarBroker::new", "(url)"],
  "connected": ["ConnectedPulsarBroker"],
  "closed": ["Closed: ()"],
  "config": ["只记录配置，", "无 I/O"],
  "client": ["构建客户端：", "URL、令牌、重试"],
  "subscriptions": ["每个订阅：", "一个消费者及", "其驱动任务"],
  "producer": ["第一次向主题", "发布时打开", "它的生产者"],
  "close": ["标为已关闭，", "关闭生产者"],
  "afterShutdown": ["比连接活得更久的发布者或定位", "返回 PulsarError::NotConnected，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`PulsarBroker::new`](../components/broker/index.md) 只记录配置，connect 构建客户端，[启动](../components/broker/startup.md)时为每个订阅打开一个消费者，[关闭](../components/broker/shutdown.md)时关闭生产者。

<Diagram name="lifecycle" words={words} />

`new` 不做任何 I/O，所以服务的构建函数保持同步。运行时在应用启动时连接 Broker。connect 用 URL、[令牌](../components/broker/security.md)和[操作重试](../components/broker/operation-retries.md)构建客户端。客户端构建失败时，启动以 [`PulsarError::Connect`](api:ruststream_pulsar::PulsarError::Connect) 失败。在 Broker 连接之前从它取得的发布者，在连接建立后共用这个连接。

每个订阅的[描述符](../components/subscriptions/index.md)先经过检查：订阅名、合法的主题名或合法的模式，以及 Pulsar 能够执行的重试声明。随后，客户端消费者在 Broker 建立连接的那个运行时上打开，旁边启动一个驱动任务。服务器拒绝时，启动以 [`PulsarError::Subscribe`](api:ruststream_pulsar::PulsarError::Subscribe) 失败，错误中给出主题名。

只消费的服务不打开任何生产者。第一次向某个主题[发布](../components/publishing/index.md)时，才打开该主题的生产者。

关闭先把连接标为已关闭，然后关闭每个生产者。客户端本身没有关闭操作，所以已关闭状态不携带任何内容。订阅被丢弃时，它的驱动任务停止。驱动任务会继续结算，直到它交出的每个投递都已结算或被丢弃，然后关闭消费者。
