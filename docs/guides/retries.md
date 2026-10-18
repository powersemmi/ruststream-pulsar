---
sidebar_position: 5
description: "Retry a Pulsar message now or later, cap the deliveries, and move what keeps failing to a dead-letter topic that a handler reads."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a Pulsar message now or later, caps the deliveries, and moves what keeps failing to a [dead-letter topic](../components/settlement/dead-letters.md) that a handler reads.

Pulsar redelivers a message itself, so a retry publishes nothing.
The mount site declares the cap and the dead-letter topic, and the Pulsar client applies them.

The example is a refunds service that sends refunds through a card processor.
`Processor` stands in for the processor's client, and the app opens it at startup.

## The subscriber

The processor refuses a refund in three ways, and each needs a different outcome.
`Busy` means its rate limit is used up: a retry right now would be refused again, so [`retry_after`](../components/settlement/retry-after.md) waits 30 seconds.
`Unavailable` means it did not answer: `retry` tries again at once.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) negatively acknowledges the refund, and the broker delivers it again at once.
[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) holds the refund unacknowledged in the service for the delay, then negatively acknowledges it.
Pulsar's negative acknowledgement carries no delay of its own, so the wait belongs to the process.
A process that stops during the wait loses the wait, not the refund: the broker delivers it again after the [acknowledgement timeout](../components/settlement/ack-timeout.md), or at once when the consumer disconnects.

The delay has to be shorter than the subscription's acknowledgement timeout, or the timeout would bring the refund back first.
A longer delay is refused when the handler returns it, with an error that names both values.
A subscription without an acknowledgement timeout accepts any delay.

## The cap and the dead-letter topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) counts every delivery of one refund, the first included.
[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) names where a refund goes once the deliveries are spent.
The pair becomes the consumer's own [dead-letter policy](https://pulsar.apache.org/docs/next/concepts-messaging/#dead-letter-topic).
The Pulsar client counts the redeliveries, publishes the fifth failed refund to `refunds-dlq` and acknowledges the original.
A `retry`, a `retry_after` and an expired acknowledgement timeout each count as a redelivery.
A dropped refund settles and goes nowhere; the dead-letter topic holds only the refunds that ran out of attempts.

Write both steps or neither: a cap without a destination, or a destination without a cap, stops the service at startup, and the error names the missing step.
The broker counts redeliveries only on a `Shared` or a `KeyShared` subscription, so the cap belongs there.
A cap on an `Exclusive` or a `Failover` subscription would never be reached, and such a registration stops the service at startup with an error that names the type.
A handler mounted by a bare topic name takes the same declaration, on the broker's [default subscription](../components/subscriptions/index.md).

## Reading the dead-letter topic

The dead-letter topic is an ordinary topic, and `review` reads it like any other.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

A dead-lettered refund keeps the payload and the headers it arrived with.
The client adds two properties: `REAL_TOPIC`, the topic the refund came from, and `ORIGIN_MESSAGE_ID`, its position there.
`review` logs the refund for a person to look at.

## Testing the delay

The tests in the example file run the app in process with `TestApp::with_state`, which hands the handlers a processor that fails on purpose.
The delay test starts on a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
The in-process Pulsar counts the deliveries and moves the spent refund to `refunds-dlq` the way the client does.
[Testing a Pulsar service](testing.md) covers the harness.
