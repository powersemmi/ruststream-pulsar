---
title: Subscriptions
sidebar_position: 1
description: "A Pulsar subscription is a named cursor the broker stores; every subscription receives the whole topic, and its type decides how its consumers share it."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Two subscriptions on one topic, each with its own cursor, and how the consumers of one subscription share it under each of the four types",
  "text": "The diagram as text",
  "topic": "Topic orders",
  "audit": ["Subscription audit", "cursor at entry 1"],
  "billing": ["Subscription billing", "cursor at entry 5"],
  "next": "reads next",
  "types": "The consumers of one subscription, by its type",
  "exclusive": "Exclusive",
  "exclusiveC1": "C1 gets every message",
  "exclusiveC2": "C2 waits for C1",
  "shared": "Shared",
  "sharedC1": "C1 gets entry 5",
  "sharedC2": "C2 gets entry 6",
  "failover": "Failover",
  "failoverC1": "C1 active, in order",
  "failoverC2": "C2 waits as standby",
  "keyShared": "KeyShared",
  "keySharedC1": "key A goes to C1",
  "keySharedC2": "key B goes to C2",
  "durable": ["The broker stores each cursor: a subscription outlives its consumers", "and receives every message published while they are away"]
};

# Subscriptions

A Pulsar subscription is a named cursor the broker stores. Every subscription receives the whole
topic, and its type decides how its consumers share it.

<Diagram name="subscriptions" words={words} />

Two subscriptions on one topic never affect each other. Each has its own cursor and receives every
message. Consumers that attach under the same subscription name share that one cursor. The
subscription is durable: it stays on the broker when its last consumer leaves, and the backlog
grows until a consumer returns.

[`SubscriptionType`](api:ruststream_pulsar::SubscriptionType) picks how the consumers of one
subscription share it:

| Type | Consumers | Order |
|---|---|---|
| `Exclusive` | one; a second waits | kept |
| `Shared` (default) | all, each message to one of them | none |
| `Failover` | one active, the others on standby | kept |
| `KeyShared` | all, each key to one of them | kept per key |

A second consumer of an `Exclusive` subscription is answered "consumer busy". The client treats
that answer as one to retry, and by default it retries without a limit, five seconds apart. So the
second consumer waits for the first to leave, and its service does not start.
[`OperationRetries`](api:ruststream_pulsar::OperationRetries) bounds the tries, and subscribing then
fails with [`PulsarError::Subscribe`](api:ruststream_pulsar::PulsarError::Subscribe).

The server creates a subscription the first time a consumer names it, positioned at the newest
message: the `pulsar` client asks for that position unless told otherwise, and this crate leaves
it. A service that starts somewhere else repositions the subscription with a seek, which
[`PulsarPosition`](api:ruststream_pulsar::PulsarPosition) names. A seek moves the stored cursor
itself, so it applies on every start, not only the first.

A subscription over a list of topics, or over a pattern, is one consumer per topic inside the
client, read as one stream. A pattern matches topics by their full names. The client lists the
topics again every 30 seconds, so a topic created after the subscription opened joins it on the
next listing.

The model is Apache Pulsar's own
([subscriptions](https://pulsar.apache.org/docs/next/concepts-messaging/#subscriptions),
[subscription types](https://pulsar.apache.org/docs/next/concepts-messaging/#subscription-types)).
How a consumer acknowledges is on [Acknowledgement](acknowledgement.md).
