---
title: Path
sidebar_position: 4
description: "The way of one Pulsar message through a RustStream service: the consumer, the driver task, the handler, the reply and the acknowledgement."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one Pulsar message that is answered, from the client consumer to its acknowledgement",
  "text": "The diagram as text",
  "consumer": ["The client consumer", "messages pushed under flow permits"],
  "take": ["PulsarMessage, built by the driver task", "the payload copied into Bytes"],
  "dispatch": ["Dispatch", "one loop, or a lane per key"],
  "decode": ["Decode", "read from the copied payload"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into a buffer the client takes"],
  "publisher": ["PulsarPublisher", "the topic's producer, receipt awaited"],
  "settle": ["Settlement", "sent to the driver task"],
  "ack": ["Acknowledgement", "written to the connection, no answer"]
};

# Path

The way of one Pulsar message through a RustStream service: the consumer, the driver task, the
handler, the reply and the acknowledgement.

<Diagram name="path" words={words} />

The client consumer holds the messages the broker pushed ahead, under its flow permits. The
subscription's driver task takes the next one and builds a
[`PulsarMessage`](api:ruststream_pulsar::PulsarMessage): it copies the payload into a `Bytes`, keeps
the topic name and the message id, and turns the message properties into headers. A message's
partition key becomes the `partition-key` header, which the lanes read. The delivery waits in a
channel of 16 until the dispatch loop takes it.

The handler's reply is encoded into a buffer, and the
[publisher](api:ruststream_pulsar::PulsarPublisher) hands that buffer to the client without a copy.
A payload over [`max_message_size`](api:ruststream_pulsar::PulsarBroker::max_message_size) fails
before it is sent: the client does not check the size, and the server drops the whole connection
on an oversized message. The publish returns once the broker has stored the message and sent its
receipt. A reply that fails to publish settles the delivery with a retry.

The delivery settles after the reply. The settlement goes back to the driver task, which hands it
to the client, and the call returns once the client has queued it. The client writes the
acknowledgement to the connection and waits for no answer.

[Benchmarks](benchmarks/index.md) counts what this path costs per message, and how much of it is
the client's.
