---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 Pulsar 上变成什么：确认，以及 Broker 计入客户端死信策略的重新投递。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "处理器在 Pulsar 上的每种结果：本 crate 请求客户端做什么，Broker 又做什么",
  "text": "以文字阅读本图",
  "outcomeLane": "处理器",
  "callLane": "本 crate 调用",
  "brokerLane": "Broker",
  "ack": ["ack()"],
  "retry": ["retry()"],
  "retryAfter": ["retry_after(d)"],
  "drop": ["drop()"],
  "unsettled": ["没有结果"],
  "held": ["等待 d，然后"],
  "clientTimer": ["什么也不做；", "客户端的计时器", "稍后处理"],
  "ackEffect": ["已确认，", "不再发送"],
  "retryEffect": ["立即", "再次发送，", "计数 + 1"],
  "retryAfterEffect": ["d 之后", "再次发送，", "计数 + 1"],
  "dropEffect": ["已确认：", "消失"],
  "unsettledEffect": ["ack_timeout 之后", "再次发送，", "计数 + 1"],
  "counted": ["只有 Shared 和 KeyShared 订阅的计数会增加：", "其他类型不计数"]
};

export const retries = {
  "title": "Pulsar 上的一次重试：立即或等待之后的否定确认、Broker 的重新投递，以及客户端的上限",
  "text": "以文字阅读本图",
  "delivery": ["一次投递", "Broker 统计它的重新投递次数"],
  "outcome": ["处理器返回", "retry() 或 retry_after(d)"],
  "now": ["retry()：", "立即发出", "否定确认"],
  "later": ["retry_after(d)：在进程中", "扣留 d，", "然后同上"],
  "redelivered": ["Broker 再次投递它", "计数 + 1"],
  "cap": ["客户端的死信策略：", "计数是否已达到 max_attempts？"],
  "again": ["低于上限：", "处理器", "再次收到它"],
  "deadLetter": ["到达上限：生产到", "死信主题，", "原消息已确认"],
  "refused": ["Exclusive 或 Failover 订阅上的上限", "拒绝启动：这两种类型都不统计重新投递"],
  "crash": ["等待期间停止，丢失的是等待而不是消息：", "消息仍未确认"],
  "handled": "已处理",
  "below": "低于上限",
  "atCap": "到达上限"
};

# 结算

`ack`、`drop`、`retry` 和 `retry_after` 在 Pulsar 上变成什么：确认，以及 Broker 计入客户端死信策略的重新投递。

<Diagram name="settlement" words={table} />

每种[结果](../components/settlement/index.md)都是对订阅的客户端消费者的一次调用，由订阅的驱动任务发出。`ack` 和 `drop` 都确认消息，因为 Pulsar 没有永久拒绝消息的命令：不应再回来的消息被确认，一再失败的消息则因上限进入[死信主题](../components/settlement/dead-letters.md)。`retry` 请求 Broker 立即再次投递它。

[`retry_after(d)`](../components/settlement/retry-after.md) 在服务中把投递保持在未确认状态 `d` 这么久，然后发出同样的否定确认。等待运行在 Broker 建立连接的那个运行时上。不短于订阅 [`ack_timeout`](../components/settlement/ack-timeout.md) 的延迟会被拒绝，因为客户端的计时器会先把消息送回来。

没有处理器结算的投递保持未确认状态。设置了 `ack_timeout` 时，超时一过，客户端就会再次请求它，[确认](pulsar/acknowledgement.md)一页用图说明了这一点。没有设置时，只有消费者断开连接后它才会回来。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

送回消息不需要发布任何东西：Broker 再次投递同一条消息，并为它计数。注册的 [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 和死信主题变成客户端的死信策略。消息的计数低于上限时，客户端交出它，因此处理器会看到它 `max_attempts` 次。到达上限时，客户端把它生产到死信主题，并确认原消息。

这两项设置要一起使用：只声明其中一项的注册会拒绝启动；`Exclusive` 或 `Failover` [订阅](../components/subscriptions/subscription-type.md)上的上限也会拒绝启动，因为这两种订阅的消息到达时计数总是零。

计数归客户端自己所有，投递不会报告它。在 `retry_after` 等待期间停止的进程，丢失的是等待而不是消息：消息仍未确认，Broker 会在确认超时之后，或在消费者的连接断开后再次投递它。
