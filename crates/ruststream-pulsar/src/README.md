Apache Pulsar broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, backed by the [`pulsar`](https://docs.rs/pulsar) client maintained by `StreamNative`.

A Pulsar topic is a retained log, and a subscription is a durable cursor the server keeps on it.
A subscription here is one descriptor, its type picks how its consumers share it, and a handler
moves it over the log with a seek. Building the crate needs `protoc` on the path.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the
application object, and the attribute writes the `main`:

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
    tracing::info!(order.id, "accepted");
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

`cargo run -- run` starts it. [`PulsarBroker::new`] only records the service URL; the runtime
connects the broker at startup and shuts it down when the service stops.

# Where things are

The [guides][guides] take a Pulsar service from installation to production, in this order:
[getting started][g-start], [topics and subscriptions][g-topics],
[subscription types and ordering][g-types], [acknowledgement][g-ack],
[retries and dead letters][g-retries], [replay and seeking][g-replay],
[publishing and replies][g-publishing], [testing][g-testing] and
[connecting to a production cluster][g-production]. The [architecture][architecture] pages show
how the crate sits between the `pulsar` client and the runtime, and the
[benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [Pulsar broker][c-broker]: [`PulsarBroker`], connected into [`ConnectedPulsarBroker`].
- [Startup][c-startup]: the dial of the service URL and a consumer per subscription.
- [Shutdown][c-shutdown]: the producers closed, and [`PulsarError::NotConnected`] for a handle
  used afterwards.
- [Security][c-security]: [`PulsarBroker::token`] and the `pulsar+ssl://` listener.
- [OperationRetries][c-operation-retries]: [`OperationRetries`], how long the client retries an
  operation a server has not accepted.
- [max_message_size][c-max-message-size]: [`PulsarBroker::max_message_size`], the largest payload
  a publish may carry.
- [Prelude][c-prelude]: [`prelude`], the one glob a Pulsar service imports.
- [Errors][c-errors]: [`PulsarError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare topic name, read under the broker's
  [`default_subscription`](PulsarBroker::default_subscription).
- [PulsarSubscription][c-pulsar-subscription]: [`PulsarSubscription::new`], one durable
  subscription on one topic.
- [topics][c-topics]: [`PulsarSubscription::topics`], a fixed list of topics.
- [pattern][c-pattern]: [`PulsarSubscription::pattern`], every topic whose full name a regular
  expression matches.
- [SubscriptionType][c-subscription-type]: [`SubscriptionType`], how the consumers of one
  subscription share it.
- [PulsarTopic][c-pulsar-topic]: [`PulsarTopic`], a topic name checked on construction.
- [Batches][c-batches]: a slice handler and [`PulsarSubscription::batch_wait`].

## Settlement

- [Acknowledgement][c-settlement]: what `ack`, `retry` and `drop` do on a [`PulsarMessage`].
- [ack_timeout][c-ack-timeout]: [`PulsarSubscription::ack_timeout`], the redelivery of an
  unsettled message.
- [retry_after][c-retry-after]: a negative acknowledgement held back for a delay.
- [Dead letters][c-dead-letters]: `max_attempts` with `dead_letter`, the consumer's own
  dead-letter policy.

## Context

- [PulsarContext][c-context]: [`PulsarContext`] and its keys [`Position`] and [`SeekHandle`].
- [PulsarBatchContext][c-batch-context]: [`PulsarBatchContext`], a batch handler's context.

## Positions

- [PulsarPosition][c-positions]: [`PulsarPosition`], a place in a topic's retained log.
- [start_at][c-start-at]: a subscription opened at a position on every startup.
- [Seek][c-seek]: [`SeekHandle`] and [`PulsarSeeker`], moving a running subscription.

## Publishing

- [Publish][c-publishing]: [`PulsarPublish`], the publish policy, and its live
  [`PulsarPublisher`].
- [partition_key][c-partition-key]: [`PulsarPublishSteps::partition_key`] and
  [`PulsarPublishOptions`] key one publish.
- [Keys][c-keys]: [`PARTITION_KEY_HEADER`] becomes the message key.
- [Headers][c-headers]: one message property per header.

## Tooling

- [AsyncAPI][c-asyncapi]: the Pulsar bindings of the generated document (feature `asyncapi`):
  [server][c-asyncapi-server], [channels][c-asyncapi-channels] and
  [operations][c-asyncapi-operations].
- [Testing][c-testing]: the in-process Pulsar
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running Pulsar.

# Cargo features

Both features are additive and off by default. `testing` adds the in-process mode
`TestApp::start` connects the broker through, and `asyncapi` contributes the Pulsar bindings to
the generated document. The codecs come from `ruststream`'s own features.

[guides]: https://powersemmi.github.io/ruststream-pulsar/latest/
[g-start]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/getting-started/
[g-topics]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/topics-and-subscriptions/
[g-types]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/subscription-types-and-ordering/
[g-ack]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/acknowledgement/
[g-retries]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/retries/
[g-replay]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/replay/
[g-publishing]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/publishing/
[g-testing]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-pulsar/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-pulsar/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-pulsar/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/
[c-startup]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/shutdown/
[c-security]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/security/
[c-operation-retries]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/operation-retries/
[c-max-message-size]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/max-message-size/
[c-prelude]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/
[c-pulsar-subscription]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/pulsar-subscription/
[c-topics]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/topics/
[c-pattern]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/pattern/
[c-subscription-type]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/subscription-type/
[c-pulsar-topic]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/pulsar-topic/
[c-batches]: https://powersemmi.github.io/ruststream-pulsar/latest/components/subscriptions/batches/
[c-settlement]: https://powersemmi.github.io/ruststream-pulsar/latest/components/settlement/
[c-ack-timeout]: https://powersemmi.github.io/ruststream-pulsar/latest/components/settlement/ack-timeout/
[c-retry-after]: https://powersemmi.github.io/ruststream-pulsar/latest/components/settlement/retry-after/
[c-dead-letters]: https://powersemmi.github.io/ruststream-pulsar/latest/components/settlement/dead-letters/
[c-context]: https://powersemmi.github.io/ruststream-pulsar/latest/components/context/
[c-batch-context]: https://powersemmi.github.io/ruststream-pulsar/latest/components/context/batch-context/
[c-positions]: https://powersemmi.github.io/ruststream-pulsar/latest/components/positions/
[c-start-at]: https://powersemmi.github.io/ruststream-pulsar/latest/components/positions/start-at/
[c-seek]: https://powersemmi.github.io/ruststream-pulsar/latest/components/positions/seek/
[c-publishing]: https://powersemmi.github.io/ruststream-pulsar/latest/components/publishing/
[c-partition-key]: https://powersemmi.github.io/ruststream-pulsar/latest/components/publishing/partition-key/
[c-keys]: https://powersemmi.github.io/ruststream-pulsar/latest/components/publishing/keys/
[c-headers]: https://powersemmi.github.io/ruststream-pulsar/latest/components/publishing/headers/
[c-asyncapi]: https://powersemmi.github.io/ruststream-pulsar/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-pulsar/latest/components/asyncapi/server/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-pulsar/latest/components/asyncapi/channels/
[c-asyncapi-operations]: https://powersemmi.github.io/ruststream-pulsar/latest/components/asyncapi/operations/
[c-testing]: https://powersemmi.github.io/ruststream-pulsar/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-pulsar/latest/components/testing/live/
