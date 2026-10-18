---
title: Lifecycle
sidebar_position: 3
description: "PulsarBroker::new records configuration, connect builds the client, startup opens a consumer per subscription, and shutdown closes the producers."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's states as types, and what each transition does on Pulsar",
  "text": "The diagram as text",
  "states": "Broker",
  "pulsar": "On Pulsar",
  "unconnected": ["PulsarBroker::new", "(url)"],
  "connected": ["ConnectedPulsarBroker"],
  "closed": ["Closed: ()"],
  "config": ["Configuration only,", "no I/O"],
  "client": ["The client is built:", "URL, token, retries"],
  "subscriptions": ["Each subscription:", "a consumer and", "its driver task"],
  "producer": ["The first publish", "to a topic opens", "its producer"],
  "close": ["Marked closed,", "producers closed"],
  "afterShutdown": ["A publisher or a seek that outlives the connection", "answers PulsarError::NotConnected, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`PulsarBroker::new`](api:ruststream_pulsar::PulsarBroker::new) records configuration, connect
builds the client, startup opens a consumer per subscription, and shutdown closes the producers.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. The runtime connects the broker when
the app starts. Connect builds the client from the URL, the token and the
[operation retries](pulsar/client.md), and a client that cannot be built fails the start with
[`PulsarError::Connect`](api:ruststream_pulsar::PulsarError::Connect). A publisher taken from the
broker before it connected shares its connection once it has.

Each subscription's descriptor is checked first: a subscription name, valid topic names or a valid
pattern, and a retry declaration Pulsar can apply. Then the client consumer opens, on the runtime
the broker connected on, and a driver task starts beside it. A refusal from the server fails the
start with [`PulsarError::Subscribe`](api:ruststream_pulsar::PulsarError::Subscribe), naming the
topic.

A service that only consumes opens no producer. The first publish to a topic opens that topic's.

Shutdown marks the connection closed, then closes every producer. The client has no close of its
own, so the closed state carries nothing. A subscription that is dropped stops its driver task.
The driver keeps settling until every delivery it handed out has settled or been dropped, then
closes the consumer.
