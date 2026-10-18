---
title: 连接生产集群
sidebar_position: 9
description: "把 Pulsar 服务连接到生产集群：TLS 和令牌、有上限的重试、消息大小限制，以及对配置的测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 连接生产集群

生产集群要求 [TLS 和认证](../components/broker/security.md)，服务则要决定等待集群多久、发送多大的消息。

## TLS 和令牌 {/* #tls-and-a-token */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

部署环境通过环境变量提供服务 URL 和令牌。
Pulsar 客户端只连接一个服务 URL。
由多个 Broker 组成的集群，通过它们前面的一个入口点访问：DNS 名称、负载均衡器，或者 [Pulsar 代理](https://pulsar.apache.org/docs/next/concepts-architecture-overview/#service-discovery)。

`pulsar+ssl://` URL 连接集群的 TLS 监听端口，按惯例是 6651。
客户端用系统信任的根证书校验服务器的证书，并校验它的主机名。
[`token`](api:ruststream_pulsar::PulsarBroker::token) 用 JWT 认证，客户端在连接和重新连接时发送它。
StreamNative Cloud 和启用了令牌认证的自建集群都使用这一组设置。

## 重试与消息大小 {/* #retries-and-the-message-size */}

Broker 重启之后，客户端会自己重新连接，并为集群尚未接受的操作重新请求。
默认情况下，它不限次数地请求，间隔五秒，因此不论重启持续多久都能等过去。
[`OperationRetries`](../components/broker/operation-retries.md) 给这个过程设定上限：尝试十次，间隔三秒，之后操作以集群的应答失败。
这个上限覆盖查找、生产者以及每个消费者的重新连接。
它还把 [`Exclusive`](../components/subscriptions/subscription-type.md) 订阅的第二个消费者从启动时的等待变成启动时的错误。

[`max_message_size`](../components/broker/max-message-size.md) 是一次发布能携带的最大载荷，默认 5 MiB，与服务器相同。
修改过服务器 `maxMessageSize` 的部署，在这里设置相同的值。
更大的发布在服务内部就会失败，不会到达集群。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

Broker 的每项设置都是一个不发起连接的构建器步骤，所以 `#[ruststream::app]` 像在本地一样构建应用。
连接在服务启动时才打开。
生成的 [AsyncAPI 文档](../components/asyncapi/server.md)报告 URL 中的主机和端口，从不包含令牌。
[重放](replay.md)主题的服务还需要集群上命名空间的保留策略。

## 测试配置 {/* #testing-the-configuration */}

测试在 `TestApp::start` 下启动生产环境的 `service`，配上生产环境的 `broker` 和测试令牌。
[进程内 Pulsar](../components/testing/index.md) 像客户端那样解析校验服务 URL，但不会去连接集群。
使用其他协议的 URL 会让启动失败，错误信息会列出客户端支持连接的协议：

```text
the service URL's scheme "https" is not one the client dials: pulsar or pulsar+ssl
```

TLS、令牌和重试上限只在真实集群上生效。
