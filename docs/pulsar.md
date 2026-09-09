# Apache Pulsar

`ruststream-pulsar` runs a RustStream service on Apache Pulsar. A topic is a retained log, so a
subscription rewinds over it. You can choose one of the four subscription types, subscribe to a
list of topics or to a pattern, and set the consumer-side dead-letter policy. The `testing` feature
ships an in-process broker. For framework concepts (writing subscribers, routing, codecs,
middleware), see the [RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

Building requires `protoc` on the path: the client compiles the Pulsar protocol definitions.

## Capabilities

Which of the framework's optional capability traits this broker implements natively. A capability
this broker does not implement does not compile at the mount site.

| Capability | Native | Why |
| --- | --- | --- |
| `Subscribe` | yes | the connected broker subscribes by topic name, opening a `Shared` subscription named `ruststream` |
| `BatchSubscriber` | client-side | the client hands over one delivery at a time, so batches are assembled on the client (see [Batches](#batches)) |
| `TransactionalPublisher` | no | the client does not implement Pulsar transactions |
| `OwnedTransactions` | no | the client does not implement Pulsar transactions |
| `RequestReply` | no | Pulsar has no reply inbox; a reply is an ordinary publish to another topic |
| `Partitioned` | yes | the `partition-key` header is the message's partition key, which `KeyShared` subscriptions order by (see [Payloads and headers](#payloads-and-headers)) |
| `Seekable` / `Positioned` | yes | a subscription seeks over `PulsarPosition`, and a handler reads the current position and the seeker through the `Position` and `SeekHandle` context keys (see [Seeking](#seeking)) |
| `DescribeServer` | yes | `PulsarBroker` reports its service host and the `pulsar` protocol, which the framework's AsyncAPI document names |

## The lifecycle

Each state is a distinct type, and every transition consumes the state before it:

```text
PulsarBroker::new(url)     configuration only, synchronous, no I/O
  .connect()   ->  ConnectedPulsarBroker    the live client; subscriptions and publishers
  .shutdown()  ->  ()                       the terminal transition
```

`new` records the URL and does no I/O, so a Pulsar service is assembled with the plain
`#[ruststream::app]` macro. The runtime dials once at startup, before it opens any subscription.

The URL is `pulsar://` or `pulsar+ssl://`, and you can attach JWT authentication with `token(jwt)`.
The client reconnects consumers and producers on its own after a broker restart.

`shutdown` consumes the connected broker, so publishing or subscribing after it does not compile.
It closes the producers this crate opened. The client has no close of its own, so the terminal
state carries no diagnostics. A publisher handed out earlier returns `PulsarError::NotConnected`
after shutdown instead of reconnecting.

## Topic addressing

A Pulsar topic name carries four independent meanings: persistence, tenant, namespace and topic.
The client takes the name as a plain string and leaves the broker to reject it. `PulsarTopic` is
that name validated on construction:

- `PulsarTopic::persistent(tenant, namespace, topic)` and `PulsarTopic::non_persistent(..)` build
  a name from its parts. They panic on a component that is empty or holds anything but ASCII
  letters, digits, `-`, `_` and `.`, so they are for literals in code.
- `PulsarTopic::parse(name)` is the fallible form for untrusted input. It accepts a fully
  qualified name (`persistent://acme/orders/created`), a `tenant/namespace/topic` triple
  (defaulting to persistent), or a bare topic name (defaulting to `persistent://public/default/`).

A subscription descriptor parses every topic it names before any I/O, and a publish parses its
destination the same way, when it opens that topic's producer.

## Subscription descriptors

`PulsarSubscription` is the descriptor of one subscription: it names the subscription, the topics
it reads, and the settings its consumer opens with.

| Method | Meaning | Default |
| --- | --- | --- |
| `PulsarSubscription::new(topic, subscription)` | one topic | - |
| `PulsarSubscription::topics([..], subscription)` | a fixed list of topics | - |
| `PulsarSubscription::pattern(regex, subscription)` | every matching topic in the lookup namespace | - |
| `subscription_type(SubscriptionType)` | how competing consumers share the subscription | `Shared` |
| `dead_letter(DeadLetter)` | the consumer-side dead-letter policy | none |
| `ack_timeout(Duration)` | redeliver messages left unacknowledged for longer than this | none |
| `batch_wait(Duration)` | how long a partial batch waits for more deliveries (see [Batches](#batches)) | 10 ms |

The four types are Pulsar's own:

| Variant | Meaning |
| --- | --- |
| `Exclusive` | one consumer holds the subscription; a second attach is rejected |
| `Shared` | competing consumers, round-robin |
| `Failover` | one active consumer with hot standbys |
| `KeyShared` | competing consumers with per-key ordering |

Subscribing validates the descriptor first: an empty subscription name, an empty topic list, a
malformed topic name and a pattern that is not a valid regular expression all return an error with
no call to the broker.

A descriptor sits inline in the `#[subscriber(..)]` decorator. One import covers a routes file:
`ruststream_pulsar::prelude::*` carries the framework's own prelude along with this crate's
descriptors, publish policy and publish arguments.

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:handler"
```

Mount it on the broker:

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:app"
```

You can also name the topic as a string: `#[subscriber("orders")]` opens a `Shared` subscription
named `ruststream` on that topic.

### Pattern subscriptions

`PulsarSubscription::pattern` reads every topic in the lookup namespace whose name matches the
regular expression, including topics created after the consumer attached. A topic list built with
`::topics([..])` is the fixed-membership alternative: it spans exactly the topics you name.

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_pattern.rs:pattern"
```

Topics matched by a pattern are written by different producers, so their payloads are not one
schema. The example takes them as bytes: a `#[derive(Deserialized)]` newtype over `&'a [u8]` names
the payload without decoding it, so no codec runs between the broker and the handler.

### Batches

A handler that takes a slice receives a whole batch and settles it at once:

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_batches.rs:handler"
```

The mount site names one number, the batch size:

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_batches.rs:app"
```

Pulsar's client hands over one delivery at a time, so the batch is assembled on the client, by the
framework's own buffer. A batch is exactly what the subscriber delivered, never a slice of it, and
it never holds more than the size the registration named.

A batch closes when it holds that many deliveries, or when `batch_wait` has elapsed since its first
one, whichever comes first. An idle subscription waits indefinitely for that first delivery. Raising
`batch_wait` trades latency for fuller batches.

A batch subscription seeks like any other: it opens at a chosen position with `start_at(..)`, and a
batch body repositions it through `PulsarBatchContext` (see
[Repositioning from a handler](#repositioning-from-a-handler)).

### Dead-lettering

`DeadLetter::new("orders-dlq").max_deliveries(5)` sends a message to `orders-dlq` after five
redeliveries of it. A negative acknowledgement advances the delivery count. `ack_timeout` advances
it without one, by redelivering anything left unacknowledged for longer than the timeout. Both are
settings on the Pulsar consumer, not machinery this crate runs.

## Acknowledgement

| Handler outcome | Pulsar operation |
| --- | --- |
| `HandlerOutcome::ack()` | acknowledge the message |
| `HandlerOutcome::retry()` | negative acknowledgement, asking for redelivery |
| `HandlerOutcome::drop()` | acknowledge the message |

Dropping acknowledges because Pulsar has no terminal reject verb. Poison messages are routed by the
dead-letter policy, which repeated redeliveries reach.

The client queues acknowledgements, so a settle that returns `Ok` means the acknowledgement is
queued on the consumer, not that the broker confirmed it. A negative acknowledgement carries no
delay, so a `HandlerOutcome::retry_after(delay)` outcome takes the framework's own deferred
re-publish path. A subscription that ends closes its consumer.

## Seeking

`PulsarPosition` names a place in a topic's retained log. It is what the `start_at(..)` clause
opens a subscription at, and what a handler seeks to:

| Position | Meaning |
| --- | --- |
| `PulsarPosition::earliest()` | the beginning of the log: every retained message is redelivered |
| `PulsarPosition::latest()` | the tip of the log: only messages published after the seek |
| `PulsarPosition::timestamp(millis)` | a publish time, in milliseconds since the Unix epoch |
| `PulsarPosition::MessageId(..)` | the position of a delivered message, read through the `Position` key |

Seeking to a position taken from a delivery redelivers exactly that message, then the rest of the
log in order. The timestamp form keeps Pulsar's own publish-time semantics.

The `start_at(..)` clause opens a subscription at a position, as the pattern example above does.
The clause is a seek, and it runs on every startup. That is not Pulsar's server-side initial
position, which applies only when a subscription is first created. A Pulsar subscription is durable
broker-side state, so `start_at(PulsarPosition::earliest())` rewinds its stored cursor and replays
the retained backlog each time the service starts. Without the clause, the subscription resumes
from that cursor.

### Repositioning from a handler

`PulsarContext` is the context a Pulsar subscription hands its handlers. It carries two keys:
`Position`, where this message sits in the log, and `SeekHandle`, the subscription's seeker. A
handler binds a key as a parameter through the framework's `Ctx` extractor, or reads it with
`ctx.context(..)` when it already declares a context. The include site attaches nothing.

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:delivery"
```

A batch body names `PulsarBatchContext` instead: the seeker without a position. A batch spans many
deliveries, so where to seek comes from the elements themselves - a `&[Message<H, T>]` batch reads
it off each element's header contract. Asking a batch body for `Position` does not compile.

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:batch"
```

The batches are assembled on the client (see [Batches](#batches)), and the seeker underneath is the
subscription's own.

One seek covers every topic and every partition of the subscription's consumer. Deliveries already
queued when the seek lands are discarded rather than handed to the handler, so the stream resumes
at the target position. A batch still being assembled is the one exception: it closes with the
deliveries it had already taken.

## Publishing

`PulsarPublish` is the policy that constructs the publisher `PulsarPublisher`, and instantiates it
at startup on the connected broker. It is that broker's default publish policy, so a handler
mounted with plain `include` replies through it. The reply type names the topic the reply goes to,
with `#[outgoing(name = "receipts")]`. A reply type that names none goes to the topic the
`publish("receipts")` clause names.

A routes file imports `ruststream_pulsar::prelude::*`, where the policy appears under its concept
name with the prefix stripped: `.out(Reply, Publish)` reads the same whichever broker a service
runs on. A handler body imports `ruststream::prelude::*` instead and names framework things only,
bounding an injected slot with the broker capability trait it needs (`Out<impl Publisher>`). The
prefixed `PulsarPublish` stays at the crate root for a file that mounts two brokers at once.

The publisher keeps one producer per topic, created on the first publish to it and closed by
`shutdown`. Each publish awaits the broker's send receipt, so a successful publish means the broker
stored the message. You can also take a publisher from the broker before the application starts,
with `PulsarBroker::publisher()`, or from the connected form with
`ConnectedPulsarBroker::publisher()`.

### Per-message publish arguments

`PulsarPublishExt` attaches an argument to the publisher, ahead of the publish builder. This crate
names one, the partition key:

`publisher.with_partition_key("user-42").message(&order).publish()`

The key is sent as the `partition-key` header, under the publish's own headers. A publish that
names `partition-key` itself overrides the argument, a publish that names other headers keeps it,
and a message with a declared header contract can carry both.

## Payloads and headers

Message properties carry headers directly, one property per header. A non-Rust peer sees a plain
Pulsar message, with no envelope format of the framework's own.

The `partition-key` header is the exception. On publish it becomes the message's own partition key,
which keyed routing uses to place the message and which `KeyShared` subscriptions order by. On
delivery it comes back as that header. `PulsarPublishExt::with_partition_key` sets the same key as
a publish argument rather than a header.

## Local development

The repository ships a compose file running Pulsar standalone, and the just recipes around it:

```bash
just brokers-up                  # Pulsar standalone on 127.0.0.1:6650 (admin on 8080)
cargo run --example pulsar_service -- run
cargo run --example pulsar_pattern -- run
cargo run --example pulsar_batches -- run
just brokers-down
```

The live test suite runs only when `PULSAR_TEST_URL` is set, and skips otherwise:

```bash
just test-brokers                # broker up, integration + conformance, broker down
```

or, against an already running broker:

```bash
PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --workspace --all-features -- --test-threads=1
```

CI runs the same suite: the integration tests, the lifecycle check (`new` -> `connect` -> subscribe
-> publish -> receive -> ack -> `shutdown`, with a publisher created before shutdown asserted to
return an error afterwards), and the seeking and batching capability suites.

## Testing

The `testing` feature ships `PulsarTestBroker`: an in-process broker that reproduces the crate's
core routing with no server and no network. It has the same lifecycle as the real broker, and the
`TestApp` harness runs a service on it, so a handler is unit-tested in process. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

It routes by exact topic name over a retained log, so a handler that names its topic as a string
mounts here unchanged. A `PulsarSubscription` descriptor resolves against the real connected broker
only, so a handler declared with one does not mount on the stand-in. The stand-in hands out the
same `PulsarContext` and `PulsarBatchContext`, with the same `Position` and `SeekHandle` keys. A
subscription opens with `start_at(..)` over the retained log, and a seek discards what was queued
and refills from the target:

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:delivery"
```

The stand-in batches as the real subscriber does, through the same client-side buffer over a
one-at-a-time queue, so a batch handler under test runs the code path it will in production.

The stand-in does not simulate Pulsar's product behaviour: subscription types, dead-lettering, ack
timeouts and redelivery timing come from a real server, and the live suite covers them.
