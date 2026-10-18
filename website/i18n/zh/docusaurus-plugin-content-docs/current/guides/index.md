---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 Apache Pulsar Broker ruststream-pulsar 构建 Pulsar 服务的指南。"
---

# 指南

`ruststream-pulsar` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 Apache Pulsar Broker，
基于 StreamNative 维护的 [`pulsar`](https://docs.rs/pulsar) 客户端构建。
这些指南带你把一个 Pulsar 服务从安装一路推进到生产环境。

在框架旁边添加这个 crate：

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

构建时 `PATH` 中需要有 `protoc`：客户端会编译 Pulsar 的协议定义。
