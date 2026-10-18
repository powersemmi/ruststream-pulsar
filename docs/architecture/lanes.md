---
title: Lanes
sidebar_position: 5
description: "One subscription reads all its topics and partitions through one consumer; workers(n, by_key) spreads the messages over lanes keyed by their partition key."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: three partitions, one consumer, one stream and two lanes keyed by the partition key",
  "text": "The diagram as text",
  "p0": "-partition-0",
  "p1": "-partition-1",
  "p2": "-partition-2",
  "consumer": ["The subscription's consumer", "every topic and partition it reads"],
  "stream": ["PulsarSubscriber stream", "one message at a time"],
  "keyed": ["workers(2, by_key)", "lane = hash of the partition-key header"],
  "lane0": ["Lane 1", "keys A and C,", "each in order"],
  "lane1": ["Lane 2", "key B, in order"],
  "unkeyed": ["A message without a key has no lane key:", "such messages rotate over the lanes"],
  "keyShared": ["Across processes, a KeyShared subscription", "keeps each key on one consumer"],
  "pushed": "pushed messages",
  "oneAtATime": "one at a time"
};

# Lanes

One subscription reads all its topics and partitions through one consumer; `workers(n, by_key)`
spreads the messages over lanes keyed by their partition key.

<Diagram name="lanes" words={words} />

A subscription's stream yields one message at a time, whichever topic or partition it came from.
Without `workers`, one loop runs the handler for each message in turn, and nothing runs side by
side.

`workers(n, by_key)` runs `n` lanes. A message goes to the lane its key hashes to, and a lane runs
its messages in order. The key is the message's [partition key](../components/publishing/keys.md), which a delivery
carries in the `partition-key` header, so the messages of one key keep their order. A message without a key has
no lane key, and such messages rotate over the lanes.

Lanes order the work inside one process. Across processes, a
[`KeyShared`](../components/subscriptions/subscription-type.md) subscription keeps each key on one consumer, so several
instances of a service share a topic and still keep each key's order.

A [seek](../components/positions/seek.md) through a [`PulsarSeeker`](api:ruststream_pulsar::PulsarSeeker) moves the whole
subscription: every topic and every partition its consumer reads.

A [batch handler](../components/subscriptions/batches.md) gets batches the runtime assembles in the service, because the client hands over
one message at a time. A batch holds at most the size the mount site names. It closes when it is
full, or 10 milliseconds after its first message, the descriptor's
[`batch_wait`](api:ruststream_pulsar::PulsarSubscription::batch_wait).
