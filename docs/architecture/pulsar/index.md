---
title: Topics
sidebar_position: 0
description: "A Pulsar topic lives in a namespace of a tenant; a persistent topic keeps its messages in BookKeeper, and a partitioned topic is one topic per partition."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A tenant holds a namespace, the namespace holds a partitioned, a persistent and a non-persistent topic, and a producer publishes to the partitioned one",
  "text": "The diagram as text",
  "producer": ["Producer", "a key picks the partition"],
  "tenant": "Tenant acme",
  "namespace": "Namespace acme/orders",
  "payments": "persistent://acme/orders/payments",
  "p0": "-partition-0",
  "p1": "-partition-1",
  "p2": "-partition-2",
  "created": "persistent://acme/orders/created",
  "ledgers": ["kept in", "BookKeeper ledgers"],
  "ticks": "non-persistent://acme/orders/ticks",
  "live": "nothing kept: only consumers connected now",
  "bare": ["A bare name is a persistent topic of the default namespace:", "orders means persistent://public/default/orders"],
  "publishes": "publishes"
};

# Topics

A Pulsar topic lives in a namespace of a tenant. A persistent topic keeps its messages in
BookKeeper, and a partitioned topic is one topic per partition.

<Diagram name="topics" words={words} />

A topic's full name is `{persistent|non-persistent}://tenant/namespace/topic`. Tenants and
namespaces are the units Pulsar administers: permissions, retention and other policies are set on
them. [`PulsarTopic`](api:ruststream_pulsar::PulsarTopic) parses a name before any I/O, and a bare
name such as `orders` becomes `persistent://public/default/orders`.

A persistent topic writes every message to BookKeeper ledgers. A message stays there until every
subscription has acknowledged it, and then for as long as the namespace's retention policy says.
A non-persistent topic stores nothing: a message goes to the consumers connected at that moment,
and a consumer that connects later never sees it.

A partitioned topic is a set of internal topics named `<topic>-partition-<n>`, each a log of its
own, each owned by one broker of the cluster. The producer picks the partition for every message.
This crate's producers hash a message's key to its partition, so the messages of one key keep their
order, and rotate over the partitions for a message without a key. A delivery from a partitioned
topic reports the partition in its [topic name](api:ruststream_pulsar::PulsarMessage::topic).

The server creates a persistent topic the first time a producer or a consumer names it, unless the
cluster turned automatic creation off. A partitioned topic is created by an administrator, with its
number of partitions.

The model is Apache Pulsar's own
([topics](https://pulsar.apache.org/docs/next/concepts-messaging/#topics),
[multi-tenancy](https://pulsar.apache.org/docs/next/concepts-multi-tenancy/),
[partitioned topics](https://pulsar.apache.org/docs/next/concepts-messaging/#partitioned-topics),
[non-persistent topics](https://pulsar.apache.org/docs/next/concepts-messaging/#non-persistent-topics)).
How consumers read a topic is on [Subscriptions](subscriptions.md).
