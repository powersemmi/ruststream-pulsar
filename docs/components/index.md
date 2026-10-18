---
sidebar_position: 0
description: "The parts of ruststream-pulsar a Pulsar service uses, one page per component."
---

# Components

The parts of `ruststream-pulsar` a Pulsar service uses, one page per component, grouped by the
part of the service they belong to.
The [broker](./broker/index.md), [`PulsarBroker`](api:ruststream_pulsar::PulsarBroker), holds the
connection.
A [subscription](./subscriptions/index.md) names its topics with a descriptor, and a handler's
outcome becomes an [acknowledgement](./settlement/index.md).
A handler reads where its delivery sits through its [context](./context/index.md), and moves its
subscription to a [position](./positions/index.md) in the log.
A service [publishes](./publishing/index.md) through a policy, with a key and headers per message.
The [Pulsar bindings](./asyncapi/index.md) describe the service in its AsyncAPI document, and a
[`TestApp`](./testing/index.md) test runs it on an in-process transport.
