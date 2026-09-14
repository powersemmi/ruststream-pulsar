Apache Pulsar transport for `RustStream`, built on the [`pulsar`](https://docs.rs/pulsar) client
maintained by `StreamNative`.

A Pulsar topic is a retained log and a subscription is durable state the broker keeps, so a
service here rewinds over what it has already read. This crate maps that onto the framework: one
descriptor per subscription form, the four subscription types as an enum, a position a handler
seeks to, and the registration's retry cap turned into the consumer's own dead-letter policy.
Handlers, routers, codecs and middleware come from [`ruststream`](https://docs.rs/ruststream) and
read the same on every broker.

Building needs `protoc` on the path: the client compiles the Pulsar protocol definitions.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-pulsar = "0.7"
serde = { version = "1", features = ["derive"] }
```

Both features are additive and off by default: `testing` ships the in-process broker
([Testing](#testing)), and `asyncapi` fills in the Pulsar half of the generated document
([The generated document](#the-generated-document)).

# A service

One handler, one subscription, a cap on how often a message comes back, and the topic it moves to
when the cap is spent.

```
# mod demo {
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(
    PulsarSubscription::new("orders", "workers")
        .subscription_type(SubscriptionType::Shared)
        .ack_timeout(Duration::from_secs(30))
)]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(handle)
                .max_attempts(nonzero!(5))
                .dead_letter("orders-dlq");
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`PulsarBroker::new`] records the URL and does no I/O, so the
synchronous `#[ruststream::app]` builder composes here like any other broker; the runtime dials
once at startup, before it opens a subscription. [`ConnectedPulsarBroker`] is the witness that it
did, and `shutdown` consumes that, so publishing or subscribing afterwards does not compile. A
publisher handed out earlier outlives the connection and reports
[`PulsarError::NotConnected`](PulsarError) instead.

Three runnable programs are in `examples/`: `pulsar_service`, `pulsar_pattern` and
`pulsar_batches`.

# Subscribing

[`PulsarSubscription`] is the descriptor of one subscription: the name it joins, the topics it
reads, and the settings its consumer opens with. It sits inline in the `#[subscriber(..)]`
decorator, and it is a source for the in-process broker as well, so the declaration a service
ships is the one its tests run.

| Form | Reads |
|---|---|
| [`PulsarSubscription::new(topic, subscription)`](PulsarSubscription::new) | one topic |
| [`PulsarSubscription::topics([..], subscription)`](PulsarSubscription::topics) | a fixed list of topics |
| [`PulsarSubscription::pattern(regex, subscription)`](PulsarSubscription::pattern) | every matching topic in the lookup namespace, including topics created later |

| Setting | Meaning | Default |
|---|---|---|
| [`subscription_type`](PulsarSubscription::subscription_type) | how competing consumers share the subscription: `Exclusive` (one consumer, a second attach is rejected), `Shared` (round-robin), `Failover` (one active consumer with hot standbys), `KeyShared` (per-key ordering) | [`SubscriptionType::Shared`] |
| [`ack_timeout`](PulsarSubscription::ack_timeout) | redeliver what a handler left unacknowledged for longer than this | none |
| [`batch_wait`](PulsarSubscription::batch_wait) | how long a partial batch waits for more deliveries | 10 ms |

```
# mod demo {
use std::time::Duration;

use ruststream_pulsar::prelude::*;

pub fn sources() -> (PulsarSubscription, PulsarSubscription, PulsarSubscription) {
    let one = PulsarSubscription::new("persistent://acme/orders/created", "workers");
    let several = PulsarSubscription::topics(["orders-eu", "orders-us"], "regional");
    let matching = PulsarSubscription::pattern("orders-.*", "audit")
        .subscription_type(SubscriptionType::KeyShared)
        .ack_timeout(Duration::from_secs(30));
    (one, several, matching)
}
# }
# fn main() {}
```

`#[subscriber("orders")]` is the short form: a bare name opens a `Shared` subscription called
`ruststream` on that topic, which is what makes two instances of a service competing consumers
rather than two independent readers. It carries nothing else, so a handler that needs any setting
above names a descriptor.

Subscribing validates first: an empty subscription name, an empty topic list, a malformed topic
name and a pattern that is not a regular expression each return
[`PulsarError::Invalid`](PulsarError) with no call to the broker.

Both forms report `Copies = BrokerMoves`: a delivery that has to come back is moved by the broker
and the client, never republished by the service. There is no retry position on a Pulsar
registration, so `.out_retry(policy)` over one does not compile.

The per-delivery vocabulary is [`PulsarContext`], carrying two keys a handler reads with `Ctx` or
`ctx.context(..)`: [`Position`], where this message sits in the log, and [`SeekHandle`], the
subscription's own seeker. A batch body names [`PulsarBatchContext`] instead, which carries the
seeker alone; asking a batch body for [`Position`] does not compile, because a batch spans many
deliveries. [`PulsarMessage::topic`] is the fully resolved topic a delivery arrived on, partition
suffix included.

## Topic names

A Pulsar topic name carries four independent meanings: persistence, tenant, namespace and topic.
The client takes it as a string and leaves the broker to reject it; [`PulsarTopic`] is that name
validated on construction. [`PulsarTopic::persistent`] and [`PulsarTopic::non_persistent`] build
one from its parts and panic on a component that is empty or holds anything but ASCII letters,
digits, `-`, `_` and `.`, so they are for literals in code. [`PulsarTopic::parse`] is the fallible
form for untrusted input: it accepts a fully qualified name, a `tenant/namespace/topic` triple
(persistent by default), or a bare name (under `persistent://public/default/`).

Descriptors and publishes parse their topics through it, so a bad name is refused before any I/O.

## Acknowledgement

| Handler outcome | Pulsar operation |
|---|---|
| `HandlerOutcome::ack()` | acknowledge the message |
| `HandlerOutcome::retry()` | negative acknowledgement, asking for redelivery |
| `HandlerOutcome::retry_after(delay)` | hold the delivery unacknowledged for `delay`, then negatively acknowledge it |
| `HandlerOutcome::drop()` | acknowledge the message |

Dropping acknowledges because Pulsar has no terminal reject verb; poison messages leave through
the dead-letter policy below. The client queues acknowledgements, so a settlement that returns
`Ok` is queued on the consumer, not confirmed by the broker.

`retry_after(delay)` is delayed redelivery with no copy published, but the wait is this process's:
Pulsar's negative acknowledgement carries no delay of its own, so the delivery stays
unacknowledged for `delay` and is negatively acknowledged when it is over. A process that exits
mid-wait loses the wait, not the message, and the broker redelivers once `ack_timeout` elapses or
at once when the consumer disconnects. A delay that is not shorter than the subscription's
`ack_timeout` is refused at the call, with an error naming both values; a subscription with no
`ack_timeout` accepts any delay.

## Retries and dead-lettering

`max_attempts(n)` is how many times one message reaches the handler, counting the first delivery.
`dead_letter(topic)` is where it goes once those are spent. The pair becomes the consumer's own
dead-letter policy: the Pulsar client counts a message's redeliveries, produces the spent one to
that topic and acknowledges the original. The service publishes nothing.

Write both steps or neither. A limit with nowhere to send the message, and a topic nothing ever
reaches, are each half a policy the client cannot apply, so a registration that writes one without
the other refuses to start and the error names the missing step.

The declaration travels through the descriptor, so a handler that needs it names a
[`PulsarSubscription`]; a cap written on a bare-name registration reaches no consumer. What
advances the count is `HandlerOutcome::retry()`, or `ack_timeout` expiring on a delivery nobody
settled. What no transport shows the handler is the count itself: the client keeps the broker's
redelivery count for its own decision and does not put it on the message, so
`IncomingMessage::redelivery_count` answers nothing here and nothing in production.

## Batches

A handler that takes a slice receives a whole batch and settles it at once.

```
# mod demo {
use std::time::Duration;

use ruststream_pulsar::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Reading {
    value: f64,
}

#[subscriber(
    PulsarSubscription::new("readings", "aggregator")
        .batch_wait(Duration::from_millis(250))
)]
async fn aggregate(readings: &[Reading]) -> HandlerOutcome {
    let total: f64 = readings.iter().map(|reading| reading.value).sum();
    println!("settled {} readings, total {total}", readings.len());
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(aggregate.batch(nonzero!(64)));
        },
    )
}
# }
# fn main() {}
```

Pulsar's client hands over one delivery at a time, so the batch is assembled on the client by the
framework's own buffer: the size belongs to the registration, and
[`batch_wait`](PulsarSubscription::batch_wait) is the deadline that closes a partial one. A batch
holds no more than the size the registration named, and closes when it is full or when the wait
has elapsed since its first delivery, whichever comes first. An idle subscription waits
indefinitely for that first delivery, and raising `batch_wait` trades latency for fuller batches.

## Seeking

[`PulsarPosition`] names a place in a topic's retained log. It is what `start_at(..)` opens a
subscription at, and what [`SeekHandle`] moves it to.

| Position | Meaning |
|---|---|
| [`PulsarPosition::earliest()`](PulsarPosition::earliest) | the beginning of the log: every retained message is redelivered |
| [`PulsarPosition::latest()`](PulsarPosition::latest) | the tip of the log: only messages published after the seek |
| [`PulsarPosition::timestamp(millis)`](PulsarPosition::timestamp) | a publish time, in milliseconds since the Unix epoch |
| [`PulsarPosition::MessageId`] | the position of a delivered message, read through the [`Position`] key |

```
# mod demo {
use ruststream_pulsar::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
    resume_at: Option<u64>,
}

/// Reads the whole retained backlog on every startup, and skips forward past a region the
/// producer marked poisoned.
#[subscriber(
    PulsarSubscription::pattern("orders-.*", "audit"),
    start_at(PulsarPosition::earliest())
)]
async fn audit(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    println!("job {}", job.id);
    if let Some(millis) = job.resume_at
        && seeker.seek(PulsarPosition::timestamp(millis)).await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
# }
# fn main() {}
```

`start_at(..)` is a seek and it runs on every startup, which is not Pulsar's server-side initial
position: that one applies only when a subscription is first created. A subscription is durable
broker-side state, so `start_at(PulsarPosition::earliest())` rewinds its stored cursor and
replays the backlog each time the service starts. Without the clause, the subscription resumes
from that cursor.

Seeking to a position taken from a delivery redelivers exactly that message, then the rest of the
log in order; the timestamp form keeps Pulsar's own publish-time semantics. One seek covers every
topic and every partition of the subscription's consumer, and deliveries already queued when it
lands are discarded rather than handed to the handler. A batch still being assembled is the one
exception: it closes with the deliveries it had already taken.

# Publishing

[`PulsarPublish`] is the policy, [`PulsarPublisher`] the live form it pairs into, and it is the
connected broker's default publish policy, so a handler mounted with a plain `include` replies
through it. Under its prelude name it is `Publish`. It is the crate's only one: the client
implements neither Pulsar transactions nor a reply inbox, so there is no `TransactionalPublish`
and no `Request`, and a request/reply exchange here is an ordinary publish to another topic.

A reply needs a destination, and it comes from the reply type: `#[outgoing(name = "receipts")]`
fixes it and the subscriber writes the bare `publish` clause, while a type that names none takes
`publish("receipts")` at the mount site.

```
# mod demo {
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber(PulsarSubscription::new("orders", "workers"), publish)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(confirm).out_reply(Publish);
        },
    )
}
# }
# fn main() {}
```

The publisher keeps one producer per topic, opened on the first publish to it and closed by
`shutdown`. Every publish awaits the broker's send receipt, so a successful publish means the
broker stored the message. Outside a handler, [`PulsarBroker::publisher`] hands one out before the
application starts and [`ConnectedPulsarBroker::publisher`] after it has.

Message properties carry headers directly, one property per header, so a non-Rust peer sees a
plain Pulsar message with no envelope format of the framework's own.

## Per-message settings

The partition key is the one value one publish differs from the next in: keyed routing places the
message by it and a `KeyShared` subscription orders by it. The
[`partition_key`](PulsarPublishSteps::partition_key) step names it for the message being
assembled.

```
# mod demo {
use ruststream_pulsar::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Ledger;

#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn record(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = PulsarPublishOptions>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(&Receipt { id: order.id })
        .to("receipts")
        .partition_key(format!("user-{}", order.id))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        PulsarBroker::new("pulsar://localhost:6650"),
        |b| {
            b.include(record).out(Ledger, Publish).build();
        },
    )
}
# }
# fn main() {}
```

The step writes one field of [`PulsarPublishOptions`] and returns the builder, so the message
still leaves through the slot the mount site wired, in that slot's codec and through that slot's
transforms. The mount site declares nothing for it, and a publish that names no step goes
unkeyed.

[`PARTITION_KEY_HEADER`] is the portable spelling of the same value: a body that writes its own
headers keys the message without naming a Pulsar type, and on delivery the key comes back as that
header. Where a publish carries both, the step wins.

# The prelude

`use ruststream_pulsar::prelude::*;` is what a routes file writes. It carries the framework's own
prelude along with this crate's broker, descriptor, positions, contexts and keys, the publish
policy under the name `Publish` with its settings type and step, and the framework capability
traits `Positioned` and `Seeker`.

A handler body imports `ruststream::prelude::*` instead and names framework things only, bounding
an injected slot with the capability trait it needs. The one exception is a body that adjusts a
per-message setting: it imports this glob too and bounds its slot
`Out<impl Publisher<Options = PulsarPublishOptions>, Marker>`, as the example above does. The
prefixed [`PulsarPublish`] stays at the crate root for a file that mounts two brokers at once and
has to tell their policies apart.

# The generated document

With the `asyncapi` feature the crate fills in the Pulsar half of the document the framework
builds out of a service's registrations. A subscription over one topic reports that topic's
namespace and persistence in the specification's `pulsar` channel binding. The specification
leaves the Pulsar operation object empty, so the consumer travels under the extension key
`x-ruststream-pulsar`.

```json
{
  "channels": {
    "persistent://acme/orders/created": {
      "bindings": {
        "pulsar": {
          "bindingVersion": "0.1.0",
          "namespace": "orders",
          "persistence": "persistent"
        }
      }
    }
  },
  "operations": {
    "receive_persistent___acme_orders_created": {
      "bindings": {
        "x-ruststream-pulsar": {
          "subscription": "workers",
          "subscriptionType": "Shared",
          "ackTimeoutMillis": 30000
        }
      }
    }
  }
}
```

Every value is read off the descriptor, before anything connects. What the document cannot say it
leaves out: a subscription over several topics reports a namespace only when all of them agree on
one, a pattern subscription reports none at all because it has no topic until it resolves against
a server, and a subscription opened by a bare topic name has no descriptor to read and describes
nothing. The server carries the host and the port alone, since a service URL may hold a token and
a published document is shared; the protocol version stays out too, because a Pulsar client
negotiates it per connection. Replies go to a declared destination, so there is no reply address
for a client to read out of a message.

# Testing

The `testing` feature ships [`PulsarTestBroker`](testing::PulsarTestBroker), an in-process broker
that reproduces the crate's routing over a retained log with no server and no network. It has the
real lifecycle, terminal state included, and [`PulsarSubscription`] and [`PulsarPublish`] are a
source and a policy for it as well, so a test mounts the declaration the service ships rather than
a rewrite of it.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_pulsar::prelude::*;
use ruststream_pulsar::testing::PulsarTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[subscriber(PulsarSubscription::new("orders", "workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

pub async fn an_order_reaches_its_handler() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(PulsarTestBroker::new(), |b| {
            b.include(handle);
        });
    let tb = TestApp::start(app).await.expect("start the harness");

    tb.broker::<PulsarTestBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<PulsarTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
}
# }
# fn main() {}
```

The harness itself is the framework's, and
<https://docs.rs/ruststream/latest/ruststream/testing/index.html> documents it.

What the stand-in emulates: every addressing form of the descriptor, the sharing rule of the
subscription type, seeking over the retained log, client-side batching, the retry cap and its
dead-letter topic, and a delayed retry driven by the harness clock. What it does not: `KeyShared`
assigns by the key's hash modulo the consumer count rather than by Pulsar's hash ranges, a seek
moves only the consumer that asked for it, `ack_timeout` redelivers nothing on its own, and topic
names route literally, so `orders` and `persistent://public/default/orders` are two addresses here
and one topic on a server. [`testing`] states each of those with its consequence for a test.

# Operations

* The URL is `pulsar://` or `pulsar+ssl://`, and several brokers are a comma-separated list.
* [`token`](PulsarBroker::token) attaches JWT authentication; the client reconnects consumers and
  producers on its own after a broker restart.
* Credentials in the URL never reach the generated document, which carries the host and the port.
* `shutdown` closes the producers this crate opened; the client has no close of its own, so the
  terminal state carries no diagnostics.
* Pulsar transactions, a reply inbox and the schema registry are out of scope: the client
  implements none of them, and the capability traits behind them are optional.
* The client delivers one message at a time, so every batch on this broker is assembled in the
  service.
* `just brokers-up` runs Pulsar standalone on `127.0.0.1:6650` for the examples, and
  `just test-brokers` runs the live suites against it.
