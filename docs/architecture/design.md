---
title: Design
sidebar_position: 2
description: "Where ruststream-pulsar sits: between the RustStream runtime and the pulsar client, one consumer and one driver task per subscription, one producer per topic."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the pulsar client and the Pulsar cluster, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement, batches",
  "crate": "ruststream-pulsar",
  "subscriber": ["PulsarSubscriber", "and its driver task"],
  "publishers": ["PulsarPublisher", "a producer per topic"],
  "message": "PulsarMessage",
  "seeker": "PulsarSeeker",
  "broker": ["PulsarBroker", "configuration"],
  "descriptors": ["PulsarSubscription,", "PulsarTopic"],
  "client": "The pulsar client",
  "consumer": ["Consumer", "one per subscription"],
  "producer": ["Producer", "one per topic"],
  "cluster": "Pulsar cluster",
  "inProcess": ["Under the testing feature, an in-process transport", "can take the client's place"],
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-pulsar` sits: between the RustStream runtime and the `pulsar` client, one consumer
and one driver task per subscription, one producer per topic.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over the
[`pulsar`](https://docs.rs/pulsar/6.9.0/pulsar/) client. The runtime above it is the same for every
broker. What is particular to Pulsar is in this crate: the
[descriptors](pulsar/subscriptions.md), [what each outcome sends](settlement.md), the dead-letter
policy and the [positions a seek moves to](api:ruststream_pulsar::PulsarPosition).

A connected broker holds one client. Every publisher of the connection shares that client and its
producers, one per topic, opened by the first publish to the topic.

Each subscription opens one client consumer, and a task of its own drives it. The client
acknowledges through the consumer, which only one owner can use at a time, while a delivery has to
settle from wherever the handler runs. So the driver task owns the consumer. It passes each
delivery on through a channel of 16, and takes back the acknowledgements and the seeks the
deliveries send it. A [`PulsarMessage`](api:ruststream_pulsar::PulsarMessage) carries the way back
to its driver, and a [`PulsarSeeker`](api:ruststream_pulsar::PulsarSeeker) uses the same way.

The client hands over one message at a time. A batch handler gets batches the runtime assembles in
the service, as [Lanes](lanes.md) draws.

A retry is the broker's: the delivery is negatively acknowledged and the broker delivers it again.
Nothing is published from the service to bring it back, so a registration on this broker has no
retry publisher to declare, as [Settlement](settlement.md) draws.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).

## The in-process transport

With the `testing` feature, [`TestApp::start`](api:ruststream::testing::TestApp::start) connects
the same broker to an in-process transport instead of a server. The descriptors, the policies and
the deliveries are the production ones, and so are their settings. The transport models full topic
names; subscriptions over one topic, a list or a pattern; the four subscription types; negative
acknowledgement; the client's dead-letter policy; delayed retries on the runtime's clock; and the
retained log a seek moves over. It refuses what a server refuses: a payload over
`max_message_size`, a second consumer of an `Exclusive` subscription, a handle used after shutdown.

What is the server's own is left to a live run: `KeyShared` hash ranges, which consumer of a
`Shared` subscription the server picks, the redelivery after the acknowledgement timeout,
partitioned topics, and messages published while a subscription has no consumer.

Without the feature the transport is not compiled. In a production build the broker's transport is
exactly the size of the client, and a subscription's stream and a delivery's settlement are exactly
the size of the channel they wrap. The compiler checks those sizes.
