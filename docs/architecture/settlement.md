---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on Pulsar: acknowledgements and redeliveries the broker counts toward the client's dead-letter policy."
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "Each outcome of a handler on Pulsar: what the crate asks the client for, and what the broker does",
  "text": "The diagram as text",
  "outcomeLane": "Handler",
  "callLane": "The crate calls",
  "brokerLane": "Broker",
  "ack": ["ack()"],
  "retry": ["retry()"],
  "retryAfter": ["retry_after(d)"],
  "drop": ["drop()"],
  "unsettled": ["no outcome"],
  "held": ["waits d, then"],
  "clientTimer": ["nothing; the", "client's timer", "acts later"],
  "ackEffect": ["acknowledged,", "never sent", "again"],
  "retryEffect": ["sent again", "at once,", "count + 1"],
  "retryAfterEffect": ["sent again", "after d,", "count + 1"],
  "dropEffect": ["acknowledged:", "gone"],
  "unsettledEffect": ["sent again after", "ack_timeout,", "count + 1"],
  "counted": ["The count rises on Shared and KeyShared subscriptions only:", "the other types keep none"]
};

export const retries = {
  "title": "A retry on Pulsar: a negative acknowledgement now or after a wait, the broker's redelivery, and the client's cap",
  "text": "The diagram as text",
  "delivery": ["A delivery", "the broker counts its redeliveries"],
  "outcome": ["The handler returns", "retry(), or retry_after(d)"],
  "now": ["retry():", "negative acknowledgement", "at once"],
  "later": ["retry_after(d): held", "in the process for d,", "then the same"],
  "redelivered": ["The broker delivers it again", "count + 1"],
  "cap": ["The client's dead-letter policy:", "has the count reached max_attempts?"],
  "again": ["Below the cap:", "the handler", "gets it again"],
  "deadLetter": ["At the cap: produced", "to the dead-letter", "topic, original acked"],
  "refused": ["A cap on an Exclusive or a Failover subscription", "refuses to start: neither counts redeliveries"],
  "crash": ["A stop during the wait loses the wait, not the message:", "the message stays unacknowledged"],
  "handled": "handled",
  "below": "below",
  "atCap": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and `retry_after` become on Pulsar: acknowledgements and redeliveries
the broker counts toward the client's dead-letter policy.

<Diagram name="settlement" words={table} />

Every outcome is a call on the subscription's client consumer, which its driver task makes. `ack`
and `drop` acknowledge the message, because Pulsar has no command that rejects one for good: a
message that should not come back is acknowledged, and one that keeps failing reaches the
dead-letter topic by the cap. `retry` asks the broker to deliver it again at once.

`retry_after(d)` holds the delivery unacknowledged in the service for `d`, then sends the same
negative acknowledgement. The wait runs on the runtime the broker connected on. A delay not shorter
than the subscription's [`ack_timeout`](api:ruststream_pulsar::PulsarSubscription::ack_timeout) is
refused, because the client's timer would bring the message back first.

A delivery no handler settles stays unacknowledged. With an `ack_timeout`, the client asks for it
again once the timeout passes, as [Acknowledgement](pulsar/acknowledgement.md) draws. Without
one, it comes back only when the consumer disconnects.

## Retries

<Diagram name="retries" words={retries} />

Nothing is published to bring a message back: the broker delivers the same message again and
counts it. A registration's [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) and
dead-letter topic become the client's dead-letter policy. The client delivers a message while its
count is below the cap, so the handler sees it `max_attempts` times. At the cap the client produces
it to the dead-letter topic and acknowledges the original.

The two settings go together: a registration that declares one without the other refuses to
start, and so does a cap on an `Exclusive` or a `Failover` subscription, whose messages always
arrive with a count of zero.

The count is the client's own, and a delivery does not report it. A process that stops during a
`retry_after` wait loses the wait and not the message: it is still unacknowledged, and the broker
delivers it again after the acknowledgement timeout or once the consumer's connection is gone.
