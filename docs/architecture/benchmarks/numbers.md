---
title: Numbers
description: "What the benchmark numbers of ruststream-pulsar measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-pulsar` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one broker. A row is not comparable with a row another broker crate publishes: the
transports do different work per message.

The broker runs on the same machine, on the loopback. With no network latency, the same cost per
message is a larger share of the time than it would be against a cluster across a network. Read the
percentage as an upper bound on what a deployed service pays.

A `broker-bound` row is one where the publish receipt behind each delivery already takes half of
what a delivery took. The work of the crate and the runtime then happens inside that wait, and the
difference comes out near zero. That is a lower bound on the cost, not a measurement of it.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the work of the `pulsar`
client on the service's thread. Most of each figure is the client's. The count moves a little
between runs, because the broker is real. The same count costs more time where it misses the cache,
so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Connecting, subscribing and the first allocations behind them are the cold start, paid once.

## What they do not mean

A row is one consumer reading one persistent, unpartitioned topic in the default namespace, a small
body and a standalone Pulsar in one container. It measures what a delivery costs inside this crate,
not what Pulsar can carry.

A partitioned topic, several consumers of one subscription and a pool of worker lanes each answer a
different question, and the comparison measures none of them. The code chart counts a [batch
handler](../lanes.md) and a reply on the same stand.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
