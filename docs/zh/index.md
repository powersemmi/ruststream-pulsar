# ruststream-pulsar { #ruststream-pulsar }

**`ruststream-pulsar`** 在 Apache Pulsar 上运行 [RustStream](https://powersemmi.github.io/ruststream/)
服务。主题是一份保留下来的日志，订阅可以在它上面回放。你从四种订阅类型里挑一种，订阅一组主题或者
一个主题模式，并用一个死信主题给消息的重试次数封顶。`testing` feature 提供一个进程内 Broker。

传输建立在 StreamNative 维护的 [`pulsar`](https://docs.rs/pulsar) 客户端之上。

构建需要 `PATH` 里有 `protoc`：客户端会编译 Pulsar 的协议定义。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:app"
```

## 这个 crate 提供什么 { #what-the-crate-offers }

一个订阅描述符记录了这条订阅的形态和它的设置：一个主题、一组固定的主题，或者查询命名空间里的一个
主题模式。四种订阅类型是一个枚举；挂载时声明的重试上限和死信主题会变成消费者自己的策略；主题是一
份保留下来的日志，所以处理器可以自己给订阅重新定位；分区键是发布时唯一能逐条消息调整的设置。这个
crate 的参考文档逐个讲了它们：

- [订阅](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#subscribing)：描述符、确认、
  [重试与死信](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#retries-and-dead-lettering)、
  [批](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#batches)和
  [定位](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#seeking)。
- [发布](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#publishing)：发布策略、回复，以及
  [逐条消息的设置](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#per-message-settings)。
- [生成的文档](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#the-generated-document)
  和[测试](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#testing)：AsyncAPI 会报告什么，
  以及进程内 Broker 能做什么。
- [运维](https://docs.rs/ruststream-pulsar/latest/ruststream_pulsar/index.html#operations)：服务地址、认证和客户端的限制。

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-language-rust: **[API 参考](https://docs.rs/ruststream-pulsar)** - 该 crate 的指南和它在 docs.rs 上的 rustdoc。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。

</div>

## 本站与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站只记录 Pulsar Broker。在每个 Broker 上表现一致的内容，都在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
