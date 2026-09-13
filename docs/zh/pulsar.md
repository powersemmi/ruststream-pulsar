# Apache Pulsar { #apache-pulsar }

`ruststream-pulsar` 在 Apache Pulsar 上运行 RustStream 服务。主题是一份保留下来的日志，因此订阅可以
在它上面回放。你可以从四种订阅类型里选一种，订阅一组主题或者一个主题模式，并用一个死信主题给消息
的重试次数封顶。`testing` feature 提供一个进程内 Broker。框架本身的概念（编写订阅者、路由、编解码器和中间件）
参见 [RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

构建需要 `PATH` 里有 `protoc`：客户端会编译 Pulsar 的协议定义。

## 能力 { #capabilities }

这个 Broker 原生实现了框架的哪些可选能力 trait。它没有实现的能力，在挂载点编译不过。

| 能力 | 原生 | 原因 |
| --- | --- | --- |
| `Subscribe` | 是 | 连接后的 Broker 按主题名订阅，打开一条名为 `ruststream` 的 `Shared` 订阅 |
| `BatchSubscriber` | 客户端侧 | 客户端一次只交出一条投递，因此批在客户端拼装（见[批](#batches)） |
| `TransactionalPublisher` | 否 | 客户端没有实现 Pulsar 事务 |
| `OwnedTransactions` | 否 | 客户端没有实现 Pulsar 事务 |
| `RequestReply` | 否 | Pulsar 没有回复信箱，回复就是向另一个主题的普通发布 |
| `Partitioned` | 是 | 投递会报告消息的分区键，`KeyShared` 订阅按它排序；发布时用 `partition_key` 步骤点名这个键（见[逐条消息的设置](#per-message-settings)） |
| `Seekable` / `Positioned` | 是 | 订阅按 `PulsarPosition` 定位，处理器通过 `Position` 和 `SeekHandle` 两个上下文键读取当前位置和定位句柄（见[定位](#seeking)） |
| `DescribeServer` | 是 | 框架的 AsyncAPI 文档写的是 `PulsarBroker` 从自己 URL 里读出的主机、端口和 `pulsar` 协议；URL 里带的凭据从不进入文档 |

## 生命周期 { #the-lifecycle }

每个状态都是独立的类型，每次转换都消费它之前的那个状态：

```text
PulsarBroker::new(url)     只有配置，同步，没有 I/O
  .connect()   ->  ConnectedPulsarBroker    活的客户端；订阅和发布者
  .shutdown()  ->  ()                       终态转换
```

`new` 只记下 URL，不做 I/O，因此 Pulsar 服务用普通的 `#[ruststream::app]` 宏就能组装。运行时在启动
时拨号一次，然后才打开订阅。

URL 写成 `pulsar://` 或 `pulsar+ssl://`，你可以用 `token(jwt)` 附上 JWT 认证。Broker 重启之后，
客户端自己重连消费者和生产者。

`shutdown` 消费连接后的 Broker，因此在它之后发布或订阅都编译不过。它关闭这个 crate 打开的那些
生产者。客户端自己没有关闭动作，所以终态不带诊断信息。先前发出的发布者在关闭之后返回
`PulsarError::NotConnected` 错误，而不是重连。

## 主题寻址 { #topic-addressing }

一个 Pulsar 主题名同时给出四样互不相干的东西：持久性、租户、命名空间和主题。客户端把这个名字当
普通字符串收下，拒绝错误的名字是 Broker 的事。`PulsarTopic` 就是同一个名字，在构造时先做校验：

- `PulsarTopic::persistent(tenant, namespace, topic)` 和 `PulsarTopic::non_persistent(..)` 从各
  部分拼出名字。某一部分为空，或者含有 ASCII 字母、数字、`-`、`_` 和 `.` 之外的字符，它们就
  panic，因此这两个构造函数用于代码里的字面量。
- `PulsarTopic::parse(name)` 返回错误而不 panic，适合不受信任的输入。它接受完全限定名
  （`persistent://acme/orders/created`）、`tenant/namespace/topic` 三元组（默认持久），或者
  光秃秃的主题名（默认 `persistent://public/default/`）。

订阅描述符在任何 I/O 之前先解析它点名的每一个主题；发布也一样，在打开该主题的生产者时解析自己的
目的地。

## 订阅描述符 { #subscription-descriptors }

`PulsarSubscription` 是一条订阅的描述符：它写出订阅名、要读的主题，以及消费者打开时用的设置。

| 方法 | 作用 | 默认值 |
| --- | --- | --- |
| `PulsarSubscription::new(topic, subscription)` | 一个主题 | - |
| `PulsarSubscription::topics([..], subscription)` | 一组固定的主题 | - |
| `PulsarSubscription::pattern(regex, subscription)` | 查找命名空间里每一个匹配的主题 | - |
| `subscription_type(SubscriptionType)` | 竞争的消费者如何共享这条订阅 | `Shared` |
| `ack_timeout(Duration)` | 超过这个时长仍未确认的消息，重新投递 | 无 |
| `batch_wait(Duration)` | 未满的批等待更多投递多久（见[批](#batches)） | 10 毫秒 |

这四种类型是 Pulsar 自己的：

| 变体 | 含义 |
| --- | --- |
| `Exclusive` | 一个消费者独占这条订阅，第二次接入会被拒绝 |
| `Shared` | 竞争的消费者，轮流分发 |
| `Failover` | 一个活跃消费者，其余热备 |
| `KeyShared` | 竞争的消费者，按键保序 |

订阅先校验描述符：订阅名为空、主题列表为空、主题名不合法，以及模式解析不出正则表达式，都返回
错误，不会调用 Broker。

描述符直接写在 `#[subscriber(..)]` 属性里，同一份声明也能挂到进程内 Broker 上（见[测试](#testing)）。
一个路由文件只要一次导入：`ruststream_pulsar::prelude::*` 带来框架自己的 prelude，以及这个 crate
的描述符、发布策略和逐条消息的设置。

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:handler"
```

把它挂到 Broker 上。`include` 之后的两个步骤给消息的重试次数封顶，并写出重试用尽之后消息去哪里；
它们见[重试与死信](#retries-and-dead-lettering)。

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:app"
```

主题也可以直接用字符串写出：`#[subscriber("orders")]` 在这个主题上打开一条名为 `ruststream` 的
`Shared` 订阅。

### 主题模式订阅 { #pattern-subscriptions }

`PulsarSubscription::pattern` 读取查找命名空间里名字匹配该正则表达式的每一个主题，包括消费者接入
之后才创建的主题。用 `::topics([..])` 写出的主题列表是成员固定的另一种写法：它正好覆盖你点名的
那些主题。

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_pattern.rs:pattern"
```

被同一个模式匹配到的主题由不同的生产者写入，因此它们的载荷不是同一套 schema。示例把它们当字节
收下：`&'a [u8]` 之上带 `#[derive(Deserialized)]` 的 newtype 只点出载荷而不解码，因此 Broker 和
处理器之间没有编解码器在跑。

### 批 { #batches }

接收切片的处理器拿到整个批，并一次结算它：

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_batches.rs:handler"
```

挂载点只写一个数字，也就是批的大小：

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_batches.rs:app"
```

Pulsar 的客户端一次只交出一条投递，因此批由框架自己的缓冲区在客户端拼装。一个批正好是订阅者投递
的那些消息，不会是其中的一部分，装的条数也从不超过注册时写下的大小。

批在攒够那么多条投递时关闭，或者在第一条投递之后过了 `batch_wait` 时关闭，以先到者为准。空闲的
订阅会一直等第一条投递。调大 `batch_wait`，是用延迟换更满的批。

批量订阅和其他订阅一样可以定位：它用 `start_at(..)` 在选定的位置打开，批量函数体通过
`PulsarBatchContext` 给它重新定位（见[在处理器里重新定位](#repositioning-from-a-handler)）。

## 确认 { #acknowledgement }

| 处理器结果 | Pulsar 操作 |
| --- | --- |
| `HandlerOutcome::ack()` | 确认消息 |
| `HandlerOutcome::retry()` | 否定确认，请求重新投递 |
| `HandlerOutcome::retry_after(delay)` | 把投递按 `delay` 扣住不确认，到点再做否定确认 |
| `HandlerOutcome::drop()` | 确认消息 |

drop 之所以也是确认，是因为 Pulsar 没有终态拒绝这个动作。毒消息由死信策略送走，反复的重新投递
会走到那里。

客户端把确认排进队列，因此一次结算返回 `Ok`，意思是确认已经排在消费者的队列上，而不是 Broker
已经确认。订阅结束时会关闭自己的消费者。

### 重试与死信 { #retries-and-dead-lettering }

一条消息重试多少次、用尽之后去哪里，就是上面那个挂载点写的两个步骤。`max_attempts(nonzero!(5))`
是一条消息送到处理器的次数，第一次投递也算在内。`dead_letter("orders-dlq")` 是它用尽之后去的
主题。两者合起来成为消费者自己的死信策略：Pulsar 客户端数一条消息被重新投递了多少次，把用尽的
那条发到这个主题，并确认原来那条。服务本身什么都不发布，所以 Pulsar 上的一次注册根本没有重试
位置：在 Pulsar 订阅上写 `out_retry(..)` 是编译错误，错误文本会说明原因。

两个步骤要么都写，要么都不写。有上限却没有地方安置用尽的消息，或者有主题却没有东西会走到那里，
都只是半条策略，客户端两者都不会应用，因此只写了一个步骤的注册拒绝启动，错误里会写出缺的是哪
一个。

这份声明经描述符送到消费者，所以处理器需要一个描述符：`#[subscriber("orders")]` 按名字打开框架
那条服务级订阅，此外什么都不带，写在这种注册上的上限到不了任何消费者。处理器需要这条策略时，
请点名 `PulsarSubscription`。

`HandlerOutcome::retry()` 就是推动这个计数的否定确认。`ack_timeout` 不靠否定确认也能推动它：
超过该超时仍未确认的消息，它都重新投递。

`HandlerOutcome::retry_after(delay)` 这个结果把否定确认往后推。Pulsar 的否定确认本身不带延迟，
因此等待是这个进程的：投递在 `delay` 这段时间里一直不被确认，延迟过去之后这个 crate 才对它做
否定确认。没有任何东西被重新发布，所以随后的重新投递就是一次普通的重新投递，死信策略照常把它
计入。

等待放在这里，带来两个后果。进程在等待途中退出，丢的是等待，不是消息：投递一直没有被确认，
因此消费者的 `ack_timeout` 到点时 Broker 会重新投递它，消费者断开时则立刻重新投递。还有，延迟
必须短于这个 `ack_timeout`，否则消费者会在等待结束之前就重新投递；不短于它的延迟，在调用处就被
拒绝，错误里会写出这两个值。没有设 `ack_timeout` 的订阅接受任何延迟。

## 生成的文档 { #the-generated-document }

框架从服务的各次注册生成一份 AsyncAPI 文档。这个 crate 的 `asyncapi` feature 把只有 Pulsar 知道
的东西填进去：

```toml
ruststream-pulsar = { version = "0.7", features = ["asyncapi"] }
```

读单个主题的订阅，在规范的 `pulsar` 信道绑定里写出该主题的命名空间和持久化。规范给 Pulsar 的
operation 对象是空的，因此消费者本身走扩展键 `x-ruststream-pulsar`：它加入的订阅、决定这条订阅
如何分享自己那份流的类型，以及确认超时。

```json
--8<-- "crates/ruststream-pulsar/tests/data/pulsar_document.json"
```

这里的每一个值都是在任何连接之前从描述符上读出来的。读多个主题的订阅，只有在它的所有主题都落在
同一个命名空间时才写出命名空间；按模式订阅则一个都不写，因为模式在服务器上解析之前它没有主题。
两者改为在扩展里写出自己的主题或自己的模式。按光秃秃的主题名打开的订阅没有描述符可读，什么都
不描述。

服务器只带主机和端口。服务 URL 里可能有令牌，而发布出去的文档是给很多人看的，因此凭据不会进入
文档。协议版本同样留在外面：Pulsar 客户端在每条连接上协商它，读者没有东西要对照。

## 定位 { #seeking }

`PulsarPosition` 点出主题保留日志里的一个地方。`start_at(..)` 子句在这个位置打开订阅，处理器也
定位到这个位置：

| 位置 | 含义 |
| --- | --- |
| `PulsarPosition::earliest()` | 日志的开头：每条保留下来的消息都会重新投递 |
| `PulsarPosition::latest()` | 日志的末尾：只有定位之后发布的消息 |
| `PulsarPosition::timestamp(millis)` | 发布时间，单位是自 Unix 纪元起的毫秒 |
| `PulsarPosition::MessageId(..)` | 已投递消息的位置，通过 `Position` 键读到 |

定位到一次投递上取得的位置，会把那一条消息原样重新投递，随后按顺序投递日志里余下的部分。时间戳
这种形式沿用 Pulsar 自己的发布时间语义。

`start_at(..)` 子句在选定的位置打开订阅，上面的主题模式示例就是这么写的。该子句本身是一次定位，
每次启动都会执行。它不是 Pulsar 服务端的初始位置，那个只在订阅首次创建时生效。Pulsar 的订阅是
Broker 侧的持久状态，因此 `start_at(PulsarPosition::earliest())` 会回拨它存下的游标，每次服务
启动都重放保留下来的积压。不写这个子句，订阅就从那个游标续上。

### 在处理器里重新定位 { #repositioning-from-a-handler }

`PulsarContext` 是 Pulsar 订阅交给自己处理器的上下文。它带两个键：`Position` 是这条消息在日志里
的位置，`SeekHandle` 是这条订阅的定位句柄。处理器用框架的 `Ctx` 提取器把某个键绑成参数；已经声明
了上下文时，用 `ctx.context(..)` 读它。挂载点不用附加任何东西。

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:delivery"
```

批量函数体点的是 `PulsarBatchContext`：有定位句柄，没有位置。一个批横跨多次投递，因此定位到哪里
由元素自己给出：`&[Message<H, T>]` 批从每个元素的消息头契约里读到它。在批量函数体里要 `Position`
编译不过。

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:batch"
```

批在客户端拼装（见[批](#batches)），下面的定位句柄就是这条订阅自己的。

一次定位覆盖这条订阅的消费者的全部主题和全部分区。定位落地时已经排在队列里的投递会被丢弃，不会
交给处理器，因此消息流从目标位置续上。正在拼装的批是唯一的例外：它带着已经取到的那些投递关闭。

## 发布 { #publishing }

`PulsarPublish` 是构造发布者 `PulsarPublisher` 的策略，并在启动时于连接后的 Broker 上实例化它。
它是该 Broker 的默认发布策略，因此用普通 `include` 挂载的处理器就通过它回复。回复类型用
`#[outgoing(name = "receipts")]` 写出回复要去的主题。没有写出主题的回复类型，去的是
`publish("receipts")` 子句点名的那一个。同一份策略也在进程内 Broker 上实例化，因此挂载点只写一次
`Publish`，两个 Broker 上都能跑（见[测试](#testing)）。

路由文件导入 `ruststream_pulsar::prelude::*`，策略在那里以去掉前缀的概念名出现：无论服务跑在哪个
Broker 上，`out_reply(Publish)` 读起来都一样。处理器函数体改为导入 `ruststream::prelude::*`，
只点名框架的东西，并用它需要的那个 Broker 能力 trait 约束注入的槽位（`Out<impl Publisher>`）；
唯一的例外是设置分区键的函数体（见[逐条消息的设置](#per-message-settings)）。带前缀的
`PulsarPublish` 留在 crate 根上，供一次挂载两个 Broker 的文件使用。

发布者为每个主题保持一个生产者：生产者在第一次向该主题发布时创建，由 `shutdown` 关闭。每次发布都
等 Broker 的发送回执，因此发布成功意味着 Broker 已经存下这条消息。你也可以在应用启动之前从 Broker
取一个发布者，用 `PulsarBroker::publisher()`，或者从连接后的形态用
`ConnectedPulsarBroker::publisher()`。

### 逐条消息的设置 { #per-message-settings }

分区键就是一次发布与下一次发布的差别所在。按键路由用它放置消息，`KeyShared` 订阅也按它排序。
`partition_key` 步骤为正在发出的这条消息点名它：

```rust
--8<-- "crates/ruststream-pulsar/tests/publish_options.rs:handler"
```

这个步骤写 `PulsarPublishOptions` 的一个字段（它是这个 crate 的设置类型），然后返回发布构建器。
因此消息仍然经由挂载点接好的槽位发出，用的是该槽位的编解码器，而测试套件的槽位视图记录下调用点
要的那个键。挂载点为此不用声明任何东西：`.out(Ledger, Publish)` 就是全部声明，而没有点名步骤的
发布不带键发出。

点名这个步骤的函数体导入 `ruststream_pulsar::prelude::*`，并在自己的槽位上写出设置类型，就像
上面那样。其他任何函数体都只导入 `ruststream::prelude::*`。

`partition-key` 消息头以每个 Broker 都读得懂的写法存着同一个键，因此自己写消息头的函数体不用点名
任何 Pulsar 类型就能给消息设键。一次发布两者都带时，以步骤为准。

## 载荷与消息头 { #payloads-and-headers }

Pulsar 的消息属性直接存放消息头，一个属性对应一个消息头。非 Rust 的对端看到的是一条普通的 Pulsar
消息，没有框架自己的信封格式。

`partition-key` 消息头是例外。发布时它变成消息自己的分区键，按键路由用它放置消息，`KeyShared`
订阅按它排序。投递回来时它还是那个消息头。`partition_key` 步骤设置同一个键，而不碰消息头
（见[逐条消息的设置](#per-message-settings)）。

## 本地开发 { #local-development }

仓库里带一个跑 Pulsar standalone 的 compose 文件，以及围绕它的 just 配方：

```bash
just brokers-up                  # Pulsar standalone，监听 127.0.0.1:6650（管理端口 8080）
cargo run --example pulsar_service -- run
cargo run --example pulsar_pattern -- run
cargo run --example pulsar_batches -- run
just brokers-down
```

只有设置了 `PULSAR_TEST_URL`，真实 Broker 的测试套件才会跑，否则跳过：

```bash
just test-brokers                # 起 Broker，跑集成测试和 conformance，关 Broker
```

或者对着已经跑起来的 Broker：

```bash
PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --workspace --all-features -- --test-threads=1
```

CI 跑的是同一套：集成测试、生命周期检查（`new` -> `connect` -> 订阅 -> 发布 -> 接收 -> ack ->
`shutdown`，并断言关闭之前创建的发布者在关闭之后返回错误），以及定位和批的检查。

## 测试 { #testing }

`testing` feature 提供 `PulsarTestBroker`：一个进程内 Broker，不用服务器也不用网络就重现这个
crate 的核心路由。它的生命周期和真实 Broker 一样，`TestApp` 套件在它上面运行服务，因此处理器在
进程内就能做单元测试。参见
[用 `TestApp` 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

`PulsarSubscription` 对它和对真实 Broker 一样，都是订阅来源，因此测试挂的就是服务交付的那份声明，
而不是改写成光秃秃主题的版本。下面这个描述符和[订阅描述符](#subscription-descriptors)一节里的那个
一模一样：

```rust
--8<-- "crates/ruststream-pulsar/tests/descriptor_sources.rs:descriptor"
```

它的寻址完整生效：单个主题、`topics([..])` 的列表，以及 `pattern(..)` 的正则表达式。正则会对着
每一个被发布到的主题做匹配，因此订阅打开之后才首次出现的主题，也像在服务器上那样到达处理器。

发布这一半照样搬得过来。`PulsarPublish` 在这个进程内 Broker 上也构造发布者，而且是它的默认策略，
因此挂载点就是生产环境的那一个（`out_reply(Publish)`，或者用 Broker 默认策略时什么都不写），
回复从发布日志里读回来：

```rust
--8<-- "crates/ruststream-pulsar/tests/descriptor_sources.rs:reply_mount"
```

它在保留日志之上路由，而不是在一根管道上，正因为如此，会给自己重新定位的服务才测得了。这个进程内
Broker 交出同样的 `PulsarContext` 和 `PulsarBatchContext`，带同样的 `Position` 和 `SeekHandle`
键。订阅用 `start_at(..)` 在保留日志上打开，一次定位会丢掉排队的内容，并从目标位置重新装填：

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:delivery"
```

框架对具备这些能力的 Broker 所做的全部检查，在这里和在真实服务器上都会跑：路由、从 `connect` 到
`shutdown` 的生命周期、定位和批。服务对着这个 Broker 做单元测试，因此衡量它的是契约，而不是这个
传输碰巧做了什么；两者是否一致，由真实服务器那一侧的运行来回答。正因为有生命周期这项检查，比
`shutdown` 活得更久的发布者在这里报 `NotConnected`，而不是悄悄收下消息，这和指向已关闭连接的句柄
完全一样。请求-响应和事务在两个 Broker 上都不检查：这个 crate 两种能力都没实现，原因见
[能力矩阵](#capabilities)。进程内 Broker 攒批的方式也和真实订阅者完全一致（同一个客户端缓冲区，
架在一次一条的队列之上），因此被测的批量处理器走的就是它在生产中要走的那条代码路径。

订阅类型是生效的，因为服务正是围着它写测试。一条消息会到达其主题上的每一条订阅，而在一条订阅
内部，由类型挑出接收它的那个消费者：`Exclusive` 让一个消费者独占订阅并拒绝第二次接入，`Failover`
投给活跃消费者，它离开时提升一个热备，`Shared` 轮流分发，`KeyShared` 按分区键切分。因此一条共享
订阅上的两个处理器，在这里也像在生产中一样分摊一次运行，而 `nack(requeue = true)` 会回到订阅，
于是重试可能落到兄弟消费者上。

还有三件事没有做到服务器那一步，依赖它们的测试就是在依赖错的 Broker：

- `KeyShared` 按键的哈希对消费者数量取模来分配，而不是按 Pulsar 的哈希区间。一个键始终落在一个
  消费者上，这才是值得测的性质；但具体是哪个消费者，与服务器上不同，消费者加入或离开时重新洗牌
  的结果也不同。
- 一次定位只移动提出请求的那个消费者；在服务器上游标属于订阅，因此共享订阅里一个消费者的定位也会
  移动它的兄弟消费者。
- `ack_timeout` 在这里和在服务器上一样给延迟重试封顶，但它本身不重新投递任何东西；信用额度和
  服务器自己的重新投递计时器属于服务器的时钟，不属于传输。

最后一条是真实 Broker 的测试套件覆盖的产品行为；前两条是这个模型比服务器粗的地方。

注册的重试声明在这里是有行为的。消费者数一条消息被重新投递了多少次，到 `max_attempts(..)` 这个
上限时，把它发到声明的 `dead_letter(..)` 主题，地点和算法都与真实客户端一致，因为这条策略是客户端
自己应用的，不是服务器。于是上限和死信去处在测试夹具上就能验证，而真实 Broker 上的运行回答客户端
是否与之一致。延迟重试也一样：它按夹具的时钟登记，因此测试用 `advance(..)` 推动它，并且看得到
不到点什么都不会回来。

主题名按字面路由，没有命名空间去解析它们：`orders` 和 `persistent://public/default/orders` 在这里
是两个地址，在服务器上是一个主题。主题模式也对着同一个字面名字匹配，而服务器拿它匹配完全限定名，
因此不带锚点的 `orders-.*` 两边选出的主题相同，而对光秃秃名字加了 `^` 锚点的模式只在这里匹配得上，
别处都匹配不上。
