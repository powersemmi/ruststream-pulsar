---
title: Client
sidebar_position: 3
description: "The pulsar client looks every topic up through the service URL, keeps one connection per broker, and feeds each consumer under flow permits."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The pulsar client: a lookup through the service URL, one pooled connection per broker, and the producers and consumers on it",
  "text": "The diagram as text",
  "url": ["PulsarBroker::new(url)", "one service URL"],
  "lookup": ["Lookup through the service URL:", "which broker owns the topic"],
  "pool": "Connection pool: one connection per broker",
  "producer": ["Producer", "one per topic"],
  "consumer": ["Consumer", "one per subscription"],
  "refresh": ["A pattern lists", "its topics again", "every 30 s"],
  "permits": ["Flow permits:", "1000 granted ahead,", "refilled below half"],
  "cluster": ["Pulsar brokers", "each topic owned by one"],
  "retries": ["An operation the server refuses for now is tried again:", "without a limit by default, five seconds apart"],
  "asks": "asks",
  "owner": "the owner's address",
  "tcp": "TCP"
};

# Client

The `pulsar` client looks every topic up through the service URL, keeps one connection per
broker, and feeds each consumer under flow permits.

<Diagram name="client" words={words} />

A Pulsar cluster spreads its topics over its brokers, and one broker owns each topic. The client
asks the service URL which broker that is, then connects to the owner. A partitioned topic is
looked up for its number of partitions first, then each partition on its own. The service URL is
one address: a DNS name, a load balancer or the Pulsar proxy in front of the brokers serves a
cluster. A `pulsar+ssl://` URL dials the brokers' TLS listener, as
[Security](../../components/broker/security.md) shows.

The client keeps one connection per broker address and shares it. This crate opens one producer per
topic, the first time something publishes to it, and every publisher of the connection uses it. Each
subscription opens one consumer.

A consumer receives messages the broker pushes. It grants the broker permits for 1000 messages in
advance and grants more each time fewer than half remain, so the broker never sends more than the
consumer can hold. This crate keeps that default.

An operation the server answers with "not now", such as a lookup while a broker restarts or a
second consumer of an `Exclusive` subscription, is tried again. By default the client tries without
a limit, five seconds apart, and gives each try 30 seconds.
[`OperationRetries`](../../components/broker/operation-retries.md) sets the number of tries, the delay
and the timeout, for every operation of the connection, the consumer's reconnect included.

The client runs its connections and its consumers as tasks of the service's tokio runtime. This
crate opens every consumer and producer on the runtime the broker connected on, so a handler on a
thread of its own does not take a connection down with its runtime.

The protocol is Apache Pulsar's
([binary protocol](https://pulsar.apache.org/docs/next/developing-binary-protocol/): service
discovery and flow control), as the
[`pulsar` client 6.9](https://docs.rs/pulsar/6.9.0/pulsar/) implements it.
