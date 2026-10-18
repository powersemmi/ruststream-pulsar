---
sidebar_position: 4
description: "Settle Pulsar messages by the handler's outcome, bound an unsettled delivery with an acknowledgement timeout, and settle a batch at once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledging messages

This guide settles Pulsar messages by the handler's outcome, bounds an unsettled delivery with an acknowledgement timeout, and settles a batch at once.

The example is an inventory service.
It reserves stock for orders and records the stock counts of warehouse scans.
`Stock` stands in for the client of the stock service, and the app opens it at startup.

## The outcome is the settlement

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#reserve"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#reserve"
```

</TabItem>
</Tabs>

The value `reserve` returns decides what happens to the message on Pulsar:

| Outcome | On Pulsar |
|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | the message is [acknowledged](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment) and never comes back |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | the message is [negatively acknowledged](https://pulsar.apache.org/docs/next/concepts-messaging/#negative-acknowledgment) and delivered again at once |
| [`retry_after(delay)`](../components/settlement/retry-after.md) | the message is held for `delay`, then negatively acknowledged |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | the message is acknowledged |

A stock service that does not answer may answer the next call, so `reserve` retries.
The message goes back to the subscription, and on a `Shared` subscription another consumer may take it.
A SKU that no item carries stays unknown, so `reserve` drops the reservation.
Pulsar has no verb to reject a message for good, so `drop` acknowledges it, and the message is gone.
A message that keeps failing leaves the subscription through a [dead-letter topic](retries.md) instead.

The client sends acknowledgements in the background.
A settlement that returns is queued on the consumer, and the broker records it a moment later.
A service that stops in between gets the message again on its next start, so Pulsar delivers each message at least once.
Handle a message so that a second delivery does no harm, for example by keying the work on the order's id.

## The acknowledgement timeout

A delivery that nobody settles stays with its consumer.
[`ack_timeout`](../components/settlement/ack-timeout.md) on the descriptor gives it a limit: the client asks the broker to deliver the message again once the limit has passed.
Without it, such a message comes back only when its consumer disconnects.

Set the timeout above the longest time the handler takes.
A handler still at work when the timeout passes gets a second delivery of the same message.
Each redelivery counts toward the [retry cap](retries.md), like a `retry`.

## Batches

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#counts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#counts"
```

</TabItem>
</Tabs>

A handler over a slice is a [batch handler](../components/subscriptions/batches.md).
The Pulsar client hands over one message at a time, so the service assembles the batch.
A batch closes when it is full or when [`batch_wait`](api:ruststream_pulsar::PulsarSubscription::batch_wait) has passed since its first message, here 250 milliseconds.
One outcome settles every message of the batch.
In the manual form, `Ok(())` acknowledges the batch, and `Err(outcomes)` settles each message with its own outcome.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

[`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is the largest batch, 64 counts here.
`on_startup` opens the clients once, and the handlers read them as the app's state.

## Testing the outcomes

The tests in the example file run the app [in process](../components/testing/index.md), with `TestApp::with_state` in place of `on_startup`.
They make the stock service fail on purpose and check the outcome of each delivery.
The in-process Pulsar hands a negatively acknowledged message back to the subscription, as a server does.
The redelivery after the acknowledgement timeout is the server's, and the crate's live tests check it against a running Pulsar.
