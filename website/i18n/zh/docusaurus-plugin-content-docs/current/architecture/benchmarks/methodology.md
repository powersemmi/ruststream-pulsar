---
title: 方法论
description: "ruststream-pulsar 如何测量自己：三个循环、在真实 Broker 上的运行、受 Broker 限制的判定计算，以及代码计数。"
---

# 方法论

`ruststream-pulsar` 如何测量自己：三个循环、在真实 Broker 上的运行、受 Broker 限制的判定计算，以及代码计数。流程就是框架自己的流程（[RustStream 方法论](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)），本页说明它在 Pulsar 上是什么样子。

## 三个循环 {/* #the-three-loops */}

一个进程把每个场景跑三遍，作为三个循环，它们只有一处不同：承载消息的是什么。

- **裸客户端。** 一个 `pulsar` crate 的消费者，直接驱动：接收、解码、确认。
- **Broker crate。** 本 crate，上面什么也没有：Broker 建立连接，[`PulsarSubscription`](api:ruststream_pulsar::PulsarSubscription) 打开订阅，一个循环从它的流中取出投递、解码并 ack。
- **RustStream。** 用户写的服务：一个 `#[subscriber]` 处理器及其外围的应用。

其余一切在三个循环中都相同：订阅及其类型、ack 的位置、解码成同一个结构体、载荷字节、带四个工作线程的 tokio 运行时，以及二进制程序。

消息体是 128 字节的 JSON。它解码成一个包含两个整数字段的结构体，因此解码不做内存分配，数字反映的是本 crate，而不是 JSON 解析器。

## 运行 {/* #the-run */}

- **每次运行使用新的主题和新的订阅。** 主题是持久化的、不分区的，位于 `public/default` 命名空间中，运行结束后通过管理 API 删除，因此一次运行永远看不到之前的运行留下的东西。
- **消费者先接入。** 然后由第二个连接上的生产者向它发送消息，最多领先消费者已处理的消息 20 000 条。
- **在三个循环中，计时窗口都从第一次投递开始，到最后一次确认结束。** 建立连接、订阅和创建主题都在窗口之外。确认是写入套接字的一条命令，不等待应答，因此任何循环都看不到服务器记录它。
- **消息条数来自一次探测**，它在 50 000 条消息上测量裸循环的速率，因此在任何机器上，测量的运行都至少持续五秒。
- **各循环交替进行**：裸、crate、RustStream，共三轮。每个循环报告它最佳、中位和最差的一轮。

当裸客户端在运行中一直在等传输时，该行标为 `受 Broker 限制`。这是算出来的：在各轮之外，一次探测为一次主题查找及其应答计时，这是客户端要等待的最便宜的请求。消费者本身不等待任何往返：它预先授予接收许可，确认也不等应答。但每次投递背后确实有一次往返，即它之前的那次发布，生产者会一直等到 Broker 存储了消息。这次往返达到每条消息测得时间的一半时，该行就被标记。往返时间与机器信息一起公布，因此这个计算可以核对。

## 代码计数 {/* #the-code-count */}

[开销图](index.md#cost-of-the-code)的每个场景都是用户写的服务，基于 [`PulsarBroker`](api:ruststream_pulsar::PulsarBroker)，连接同一个 Broker，构建时不带 `testing` 特性。一个 `Shared` 订阅读取本次运行专用的持久化主题。另一个线程用自己的运行时和连接，在测量区间打开之前通过本 crate 的发布者填充主题，因此在服务取走第一条消息之前，每条消息都已存储。

指令数来自 callgrind，内存分配来自 DHAT。一个场景依次在一次投递、1000 次、2000 次上运行。后两次之差是一条消息的稳态开销，单次投递就是冷启动。

与保存的基线对照时，任何场景的指令数多出百分之二就算失败。内存分配在每次运行中都受每个场景声明的下限约束。

## 自己运行 {/* #running-it-yourself */}

```bash
just bench
```

这条命令按 `docker-compose.test.yml` 启动测试环境，运行两个对照场景，停止测试环境，并重写 `docs/benchmarks/results.json`。它需要几分钟，并且需要独占机器。它会清空 `RUSTFLAGS`，因此二进制程序不会绑定到构建它的 CPU 上。两条命令都按发布版本的方式构建基准测试，跨 crate 启用完整的链接时优化（fat LTO），因此这些数字描述的正是服务实际使用的代码。

```bash
just bench-code
```

代码计数需要一两分钟，并且需要 valgrind。`just bench-code 5000` 在更大的条数上测量，得到更稳定的数字。在 `main` 上运行 `just bench-code --save-baseline=main` 记录基线，在分支上运行 `just bench-code --baseline=main` 与它对照。
