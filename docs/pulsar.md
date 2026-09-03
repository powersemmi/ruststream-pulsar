# Apache Pulsar

`ruststream-pulsar` is the Apache Pulsar broker. It covers the four subscription types,
multi-topic and pattern subscriptions, the consumer-side dead-letter policy, seeking over the
retained log, and ships an in-process test broker under its `testing` feature. For framework
concepts (writing subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

Building requires `protoc` on the path: the client compiles the Pulsar protocol definitions.

Transactions, consumer-side batch receive, and the schema registry are out of scope: the client
does not implement them, and the capability traits they would back are optional by design.

## Capabilities

Which of the framework's optional capability traits this broker implements natively. A capability
that is not implemented does not compile at the mount site, rather than failing at runtime.

| Capability | Native | Why |
| --- | --- | --- |
| `Subscribe` | yes | the connected broker subscribes by topic name, opening a `Shared` subscription named `ruststream` |
| `BatchSubscriber` | no | the client exposes no consumer-side batch receive |
| `TransactionalPublisher` | no | the client does not implement Pulsar transactions |
| `OwnedTransactions` | no | the client does not implement Pulsar transactions |
| `RequestReply` | no | Pulsar has no reply inbox; a reply is an ordinary publish to another topic |
| `Partitioned` | yes | the `partition-key` header is the message's partition key, which `KeyShared` subscriptions order by (see [Payloads and headers](#payloads-and-headers)) |
| `Seekable` / `Positioned` | yes | topics are a retained log: the subscriber seeks over `PulsarPosition`, a delivery carries its own message id back as one, and handlers reach both through the `Position` and `SeekHandle` context keys (see [Seeking](#seeking)) |
| `DescribeServer` | yes | `PulsarBroker` reports its service host and the `pulsar` protocol, which the framework's AsyncAPI generation consumes |

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
PulsarBroker::new(url)     configuration only, synchronous, no I/O
  .connect()   ->  ConnectedPulsarBroker    the live client; subscriptions and publishers
  .shutdown()  ->  ()                       the terminal transition
```

`new` performs no I/O, so a Pulsar service is assembled with the same `#[ruststream::app]` macro
as any other broker: the runtime dials once at startup, before opening subscriptions. The URL is
`pulsar://` or `pulsar+ssl://`, and `token(jwt)` attaches JWT authentication - both are recorded
on the synchronous builder. Below the crate, the client reconnects consumers and producers on its
own after a broker restart.

Because `shutdown` consumes the connected broker, publishing or subscribing after it does not
compile. Shutdown marks the shared client state closed and closes the producers the crate opened,
which are the handles holding broker-side state worth a clean goodbye; the client itself has no
close, so the transition carries no diagnostics. A publisher handed out earlier still aliases the
connection and reports `PulsarError::NotConnected` afterwards rather than reconnecting behind the
application's back.

## Topic addressing

A Pulsar topic name carries four independent meanings - persistence, tenant, namespace, topic -
and the client treats it as a plain string, deferring errors to the broker. `PulsarTopic`
validates on construction instead:

- `PulsarTopic::persistent(tenant, namespace, topic)` and `PulsarTopic::non_persistent(..)` build
  a name from its parts. They panic on a component that is empty or carries characters Pulsar
  rejects, so they are for literals in code.
- `PulsarTopic::parse(name)` is the fallible form for untrusted input. It accepts a fully
  qualified name (`persistent://acme/orders/created`), a `tenant/namespace/topic` triple
  (defaulting to persistent), or a bare topic name (defaulting to `persistent://public/default/`).

Every topic named in a subscription descriptor goes through the same parse before any I/O, and
publishing resolves the destination name the same way, so a malformed name fails in one place with
one message.

## Subscription descriptors

`PulsarSubscription` is the subscription descriptor. It names the topics and the Pulsar
subscription they share, and carries the consumer settings the product owns:

| Method | Meaning | Default |
| --- | --- | --- |
| `PulsarSubscription::new(topic, subscription)` | one topic | - |
| `PulsarSubscription::topics([..], subscription)` | a fixed list of topics | - |
| `PulsarSubscription::pattern(regex, subscription)` | every matching topic in the lookup namespace | - |
| `subscription_type(SubscriptionType)` | how competing consumers share the subscription | `Shared` |
| `dead_letter(DeadLetter)` | the consumer-side dead-letter policy | none |
| `ack_timeout(Duration)` | redeliver messages left unacknowledged for longer than this | none |

The subscription type is an enum with per-variant meaning, so combinations that do not exist are
unrepresentable:

| Variant | Meaning |
| --- | --- |
| `Exclusive` | one consumer holds the subscription; a second attach is rejected |
| `Shared` | competing consumers, round-robin |
| `Failover` | one active consumer with hot standbys |
| `KeyShared` | competing consumers with per-key ordering |

A descriptor is validated before any I/O: an empty subscription name, an empty topic list, a
malformed topic name, or a pattern that is not a valid regular expression fails with
`PulsarError::Invalid` at subscribe time, without a call to the broker.

`PulsarSubscription` implements `SubscriptionSource`, so it sits inline in the `#[subscriber(..)]`
decorator. The one import is `ruststream_pulsar::prelude::*`, which carries the framework's own
prelude along with this crate's descriptors, publish policy and publish arguments:

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_service.rs:app"
```

The plain string form `#[subscriber("orders")]` also works: it opens a `Shared` subscription named
`ruststream` on that topic, which is the competing-consumer default.

### Pattern subscriptions

`PulsarSubscription::pattern` follows every topic in the lookup namespace whose name matches the
regular expression, including topics created after the consumer attached. A topic list built with
`::topics([..])` is the fixed-membership alternative: it spans exactly the named topics, and a
seek or a settlement covers all of them.

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_pattern.rs:pattern"
```

Because a pattern spans topics written by different producers, the payloads are not one schema.
The example puts them on the framework's byte lane instead of naming a model: a
`#[derive(Deserialized)]` newtype over `&'a [u8]` is a named payload that already carries its
own bytes, so no codec stands between the broker and the handler.

### Dead-lettering

`DeadLetter::new("orders-dlq").max_deliveries(5)` is the consumer-side dead-letter policy the
broker enforces: after that many redeliveries of the same message it goes to the dead-letter
topic. The delivery count advances on negative acknowledgement, and `ack_timeout` makes it advance
without one as well, by redelivering anything left unacknowledged for longer than the timeout.
Both are Pulsar product features configured on the consumer, not machinery this crate runs.

## Acknowledgement

| Handler outcome | Pulsar operation |
| --- | --- |
| `HandlerOutcome::ack()` | acknowledge the message |
| `HandlerOutcome::retry()` | negative acknowledgement, asking for redelivery |
| `HandlerOutcome::drop()` | acknowledge the message |

Dropping acknowledges because Pulsar has no terminal reject verb: poison-message routing belongs
to the dead-letter policy, reached by the repeated redeliveries a negative acknowledgement drives.

The client queues acknowledgements asynchronously, so a successful settle means the
acknowledgement was queued on the consumer, not that the broker confirmed it. Delayed redelivery
has no native form here either: a negative acknowledgement carries no delay, so a
`HandlerOutcome::retry_after(delay)` outcome takes the framework's broker-agnostic deferred
re-publish path instead.

Settlement travels from the message handle to the subscription's driver task, which owns the
client consumer, because the client's acknowledgement API needs `&mut Consumer` while a settle
token must be `Send + 'static`. Dropping the subscriber stops that task, which closes the
consumer.

## Seeking

Pulsar topics are a retained log, so `PulsarSubscriber` implements the framework's `Seekable`
capability, and `PulsarPosition` is the position type on both surfaces the framework offers:

| Position | Meaning |
| --- | --- |
| `PulsarPosition::earliest()` | the beginning of the log: every retained message is redelivered |
| `PulsarPosition::latest()` | the tip of the log: only messages published after the seek |
| `PulsarPosition::timestamp(millis)` | a publish time, in milliseconds since the Unix epoch |
| `PulsarPosition::MessageId(..)` | the position of a delivered message, captured through `Positioned` |

A position captured from a delivered message carries the framework's pinned contract: seeking to
it redelivers exactly that message, then the rest of the log in order. The timestamp form keeps
Pulsar's own publish-time semantics.

A subscription opens at a chosen position with the `start_at(..)` clause, as the pattern example
above does. The clause is a seek, and it runs on every startup - which is not the same thing as
Pulsar's server-side initial position, applied only when a subscription is first created. A
subscription is durable broker-side state, so `start_at(PulsarPosition::earliest())` rewinds an
existing subscription's cursor and replays the retained backlog each time the service starts.
Where that is not wanted, leave the clause off and let the subscription resume from its stored
cursor.

### Repositioning from a handler

A live subscription is repositioned from inside a handler, through the delivery's own context.
`PulsarContext` is what a Pulsar subscription hands its bodies, and it carries two keys:
`Position`, where this message sits (its message id), and `SeekHandle`, the subscription's
seeker. Both bind as parameters through the framework's `Ctx` extractor, or read through
`ctx.context(..)` when the handler already declares a context; nothing is attached at the include
site, and a key a broker does not carry is a compile error rather than a runtime miss.

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:delivery"
```

A page body gets the subscription-scoped half instead. Pulsar's client has no consumer-side batch
receive, so pages here come from the framework's own buffer (the `buffered(..)` clause), and their
context is `PulsarBatchContext`: the seeker, without a position. A page spans many deliveries, so
where to seek rides the elements themselves - a `&[Message<H, T>]` page reads it off each
element's header contract. Asking a page body for `Position` does not compile.

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:page"
```

One seek covers every topic and every partition of the subscription's consumer, and the broker
redelivers from the new position, so no per-message acknowledgement state needs resetting.
Deliveries that were already buffered when the seek landed are discarded rather than handed to the
handler, so the stream resumes at the target position. The framework's `capabilities::seeking`
conformance suite covers the capability, and this crate runs it against a live broker.

## Publishing

A publisher is a policy plus the live connection. The policy holds no connection, so it is
constructed anywhere - in a router, in configuration, at a mount site - and the runtime pairs it
with the broker at startup. `PulsarPublish` pairs into `PulsarPublisher`, and it is the connected
broker's default publish policy, so a `#[subscriber(.., publish("dest"))]` handler mounted without
an explicit publisher replies through it.

The prelude carries it under its own name, so a mount site that names one writes
`.publisher(PulsarPublish)`. The bare `Publish` is the framework's slot capability trait - the
bound a handler puts on an out slot - and this crate does not alias over it.

The publisher keeps one producer per topic, created on first publish and shared through the broker
core so `shutdown` closes them. Each publish awaits the broker's send receipt, so success means
the broker stored the message. A publisher can also be taken from the broker before the
application starts, with `PulsarBroker::publisher()`, or from the connected form with
`ConnectedPulsarBroker::publisher()`.

### Per-message publish arguments

`PulsarPublishExt` attaches an argument to the publisher, ahead of the publish builder. The
partition key is the one this crate names that way:

`publisher.with_partition_key("user-42").message(&order).publish()`

It travels as the `partition-key` header, sent under the publish's own headers: a publish naming
`partition-key` itself overrides the argument, one naming other keys keeps it, and a message with a
declared header contract can carry both.

## Payloads and headers

Message properties carry headers directly, one property per header, so no envelope format is
invented and non-Rust peers see plain Pulsar messages.

The `partition-key` header is the exception: it becomes the message's own partition key on
publish, which keyed routing uses to place the message and which `KeyShared` subscriptions order
by, and it comes back as the same header on delivery. `PulsarMessage` also implements the
framework's `Partitioned` capability over it, and `PulsarPublishExt::with_partition_key` names
it as a publish argument rather than a header. The convention matches the in-memory broker's, so
switching brokers does not change a service's headers.

## Local development

The repository ships a compose file running Pulsar standalone, and the just recipes around it:

```bash
just brokers-up                  # Pulsar standalone on 127.0.0.1:6650 (admin on 8080)
cargo run --example pulsar_service -- run
cargo run --example pulsar_pattern -- run
just brokers-down
```

The live test suite is gated on `PULSAR_TEST_URL`, and skips when it is unset:

```bash
just test-brokers                # broker up, integration + conformance, broker down
```

or, against an already running broker:

```bash
PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --workspace --all-features -- --test-threads=1
```

The same suite runs in CI: the integration tests, the framework's conformance lifecycle check
(`new` -> `connect` -> subscribe -> publish -> receive -> ack -> `shutdown`, with a publisher
created before shutdown asserted to error afterwards), and the seeking capability suite.

## Testing

The `testing` feature ships `PulsarTestBroker`: an in-process broker that reproduces the crate's
core routing with no server and no network. It follows the same ladder as the real broker, and its
connected form implements `ruststream::testing::TestableBroker`, so the same broker drives the
`TestApp` harness and the framework's conformance suite in process; inject traffic with
`broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

It routes by exact address match and does not simulate Pulsar product behaviour: subscription
types, dead-lettering, ack timeouts, redelivery timing, and seeking over a retained log are
covered by the live suite against a real broker instead.

Seeking is the one place where that shows up at compile time. There is no retained log in
process, so the test broker carries no seek context, and a handler that binds `Ctx<SeekHandle>`
or names `PulsarContext` does not mount on it - the mount site says so, rather than the handler
silently seeking nowhere. Split such a handler so the part worth unit-testing takes no seek key,
and let the live suite cover the repositioning itself.
