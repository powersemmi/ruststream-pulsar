# ruststream-pulsar { #ruststream-pulsar }

**`ruststream-pulsar`** 在 Apache Pulsar 上运行 [RustStream](https://powersemmi.github.io/ruststream/)
服务。主题是一份保留下来的日志，订阅可以在它上面回放。你从四种订阅类型里挑一种，订阅一组主题或者
一个主题模式，并设定消费者侧的死信策略。`testing` feature 提供一个进程内 Broker。

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

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Pulsar 指南](pulsar.md)** - 订阅描述符、定位、确认、发布和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-pulsar)** - 该 crate 在 docs.rs 上的 rustdoc。

</div>

## 本站与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站只记录 Pulsar Broker。在每个 Broker 上表现一致的内容，都在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
