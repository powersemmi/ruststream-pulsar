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

Transactions and the schema registry are out of scope: the client does not implement them, and the
capability traits they would back are optional by design.

## Capabilities

Which of the framework's optional capability traits this broker implements natively. A capability
that is not implemented does not compile at the mount site, rather than failing at runtime.

| Capability | Native | Why |
| --- | --- | --- |
| `Subscribe` | yes | the connected broker subscribes by topic name, opening a `Shared` subscription named `ruststream` |
| `BatchSubscriber` | client-side | the client exposes no consumer-side batch receive, so batches are assembled from single deliveries (see [Batches](#batches)) |
| `TransactionalPublisher` | no | the client does not implement Pulsar transactions |
| `OwnedTransactions` | no | the client does not implement Pulsar transactions |
| `RequestReply` | no | Pulsar has no reply inbox; a reply is an ordinary publish to another topic |
| `Partitioned` | yes | the `partition-key` header is the message's partition key, which `KeyShared` subscriptions order by (see [Payloads and headers](#payloads-and-headers)) |
| `Seekable` / `Positioned` | yes | topics are a retained log: the subscriber seeks over `PulsarPosition`, a delivery carries its own message id back as one, and handlers reach both through the `Position` and `SeekHandle` context keys (see [Seeking](#seeking)) |
| `DescribeServer` | yes | `PulsarBroker` reports only the host and port from its URL, with the `pulsar` protocol, which the framework's AsyncAPI generation consumes |

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
| `batch_wait(Duration)` | how long a partial batch waits for more deliveries (see [Batches](#batches)) | 10 ms |

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

`PulsarSubscription` implements `SubscriptionSource` for the real broker and, behind the `testing`
feature, for the in-process stand-in, so it sits inline in the `#[subscriber(..)]` decorator and
the same declaration mounts on either (see [Testing](#testing)). The one import is
`ruststream_pulsar::prelude::*`, which carries the framework's own prelude along with this crate's
descriptors, publish policy and publish arguments:

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

### Batches

A handler that takes a slice is handed a whole batch and settles it at once:

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_batches.rs:handler"
```

Its mount site names one number, the batch size, which is the only thing about a batch the
framework carries down to the broker:

```rust
--8<-- "crates/ruststream-pulsar/examples/pulsar_batches.rs:app"
```

Pulsar's client has no consumer-side batch receive - its `batch_size` is a flow-control window,
not a receive size - so the batch is assembled from single deliveries, by the framework's own
client-side buffer which `PulsarSubscriber` carries. Nothing at the mount site says so, and
nothing in the body can tell: the batch is exactly what the subscriber delivered, never a slice of
it, and it never carries more than the size the registration named.

A batch closes when it holds that many deliveries, or when `batch_wait` has elapsed since its
first one, whichever comes first; an idle subscription waits indefinitely for that first delivery.
The default is 10 ms, short so that a batch which is already useful does not wait on a sparse
topic; raising it trades latency for fuller batches. The size is not a descriptor option beside
it, because it belongs to the registration rather than to the subscription - one subscription
descriptor can be mounted twice with different batch sizes.

Buffering does not move the subscription, so a batch subscription seeks like any other: it opens
at a chosen position with `start_at(..)`, and a batch body repositions it through
`PulsarBatchContext` (see [Repositioning from a handler](#repositioning-from-a-handler)).

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

A batch body gets the subscription-scoped half instead: `PulsarBatchContext`, the seeker without a
position. A batch spans many deliveries, so where to seek rides the elements themselves - a
`&[Message<H, T>]` batch reads it off each element's header contract. Asking a batch body for
`Position` does not compile.

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:batch"
```

That the batches are assembled on the client (see [Batches](#batches)) costs the seek nothing:
buffering does not move the subscription, so the handle underneath is the subscription's own, and
a batch subscription carries `start_at(..)` like any other.

One seek covers every topic and every partition of the subscription's consumer, and the broker
redelivers from the new position, so no per-message acknowledgement state needs resetting.
Deliveries that were already buffered when the seek landed are discarded rather than handed to the
handler, so the stream resumes at the target position. A batch still being assembled is the one
exception: it keeps what it had already pulled, which was pulled before the seek, and closes with
it. The framework's `capabilities::seeking`
conformance suite covers the capability, and this crate runs it both against a live broker and
against the in-process stand-in.

## Publishing

A publisher is a policy plus the live connection. The policy holds no connection, so it is
constructed anywhere - in a router, in configuration, at a mount site - and the runtime pairs it
with the broker at startup. `PulsarPublish` pairs into `PulsarPublisher`, and it is the connected
broker's default publish policy, so a handler mounted with plain `include` replies through it. The
reply type names the topic the reply goes to, with `#[outgoing(name = "receipts")]`. A reply type
that names none goes to the topic the `publish("receipts")` clause names. The same policy pairs
against the in-process stand-in, where it becomes that broker's publisher, so a mount site names
`Publish` once and runs on either (see [Testing](#testing)).

Which name you write depends on which prelude the file writes, and the two do not overlap. A
routes file imports `ruststream_pulsar::prelude::*` and gets the mount-site vocabulary, where each
publishing mode this broker supports appears under its concept name with the prefix stripped:
`.out(Reply, Publish)` reads the same whichever broker a service runs on, and the absence of a
`TransactionalPublish` name is the statement that Pulsar's client has no transactions. A
handler body imports `ruststream::prelude::*` instead and names framework things only, bounding an
injected slot with the broker capability trait it needs (`Out<impl Publisher>`). The prefixed
`PulsarPublish` stays at the crate root for a file that mounts two brokers at once.

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
cargo run --example pulsar_batches -- run
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
created before shutdown asserted to error afterwards), and the seeking and batching capability
suites.

## Testing

The `testing` feature ships `PulsarTestBroker`: an in-process broker that reproduces the crate's
core routing with no server and no network. It follows the same ladder as the real broker, terminal
state included, and it drives the `TestApp` harness. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

`PulsarSubscription` is a subscription source for it as well as for the real broker, so a test
mounts the declaration the service ships instead of a bare-topic rewrite of it - the descriptor
below is the one from [Subscription descriptors](#subscription-descriptors), unchanged:

```rust
--8<-- "crates/ruststream-pulsar/tests/descriptor_sources.rs:descriptor"
```

Its addressing is honoured in full: a single topic, the list of `topics([..])`, and the regular
expression of `pattern(..)`, matched against every topic published to, so a topic that first
appears after the subscription opened reaches the handler as it does on a server.

The publishing half carries over the same way. `PulsarPublish` pairs against the stand-in as well,
and it is that broker's default policy, so the include site is the production one - `.out(Reply,
Publish)`, or nothing at all for the broker default - and the reply is read back off the publish
log:

```rust
--8<-- "crates/ruststream-pulsar/tests/descriptor_sources.rs:reply_mount"
```

It routes over a retained log, so it is a log broker like the real one, not a pipe. That is what
lets a service that repositions itself be unit-tested at all: the
stand-in carries the same `PulsarContext` and `PulsarBatchContext` with the same `Position` and
`SeekHandle` keys, a subscription opens with `start_at(..)` over the retained log, and a seek
really discards what was queued and refills from the target. A handler that seeks therefore
mounts on `PulsarTestBroker` unchanged, and the assertions are the harness's own:

```rust
--8<-- "crates/ruststream-pulsar/tests/seek_context.rs:delivery"
```

Every framework suite this crate's capabilities justify runs against the stand-in as well as
against a real broker: the routing suite, `harness::lifecycle`, `capabilities::seeking` and
`capabilities::batches`. A service is unit-tested against this broker, so it is held to the
contract rather than to whatever it happens to do, and the server legs are what say the two
agree. `harness::lifecycle` is the reason a publisher that outlives `shutdown` reports
`NotConnected` here rather than quietly accepting the message, exactly as a handle aliasing a
closed connection does. Request-reply and transactions have no leg either way: the crate
implements neither capability, for the reasons in the [capability matrix](#capabilities), so
neither suite applies to either broker. The stand-in also batches exactly as the
real subscriber does - the same client-side buffer over a one-at-a-time queue - so a batch
handler under test runs the code path it will in production.

The subscription type is honoured, because it is the thing a service writes tests about. A
message reaches every subscription over its topic, and within one subscription the type picks the
consumer that takes it: `Exclusive` holds the subscription for one consumer and refuses a second
attach, `Failover` delivers to the active consumer and promotes a standby when it leaves, `Shared`
rotates, and `KeyShared` splits by partition key. Two handlers on one shared subscription
therefore split a run between them here as they do in production, and a `nack(requeue = true)`
goes back to the subscription, so a retry can land on a sibling.

Four things still stop short of a server, and a test that leans on them is leaning on the wrong
broker:

- `KeyShared` assigns by the key's hash modulo the consumer count rather than by Pulsar's hash
  ranges. One key stays on one consumer, which is the property worth testing, but which consumer
  that is differs from a server's, and so does what a consumer joining or leaving reshuffles.
- A seek moves the consumer that asked for it; on a server the cursor belongs to the subscription,
  so a seek from one consumer of a shared subscription moves its siblings too.
- A delivery nacked past `max_deliveries` keeps coming back instead of moving to the dead-letter
  topic: `dead_letter` needs the server's per-message delivery count, which this transport does
  not keep.
- `ack_timeout`, credit and redelivery timing carry no behaviour here; they are the server's
  clock, not the transport's.

The last two are product behaviour the live suite covers against a real broker; the first two are
where this model is coarser than the server's.

Topic names route literally, with no namespace to resolve them against: `orders` and
`persistent://public/default/orders` are two addresses here and one topic on a server. A pattern
is matched against that same literal name, while a server matches it against the fully qualified
one, so an unanchored `orders-.*` selects the same topics either way and a `^`-anchored pattern
over a bare name matches here and nowhere else.
