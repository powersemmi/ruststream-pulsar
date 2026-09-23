# Benchmarks

Everything between the Pulsar client and your handler costs time on every message: the subscription
stream, the decode, the settle, the dispatch. This page says how much, measured against the same
work written by hand on the `pulsar` crate.

One scenario, three loops in one process, each carrying the same messages a different way. The
first drives the client directly. The second drives this crate's own consumer and publisher, with
the loop in the benchmark. The third is the service a user writes, with its handler and the
runtime. Everything else is held equal - the connection, the subscription and its type, the
position of the ack, the decode into the same type, the payload bytes, the tokio runtime and the
build. The procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

The best of three interleaved rounds, with the slowest round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "Full service", "adapterOverhead": "Crate overhead", "overhead": "Service overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

Three columns, three questions. **Raw client** is the `pulsar` crate driven by hand, the floor
everything else is measured against. **This crate** is the same loop over the types this repository
ships - the broker, the subscription descriptor, the subscriber stream, the delivery and its ack,
the publisher - and the gap to the floor is what those types cost. **Full service** adds the
handler, the router and the runtime, and the gap from the second column to the third is what the
framework costs on top of this broker in particular.

The two rows differ in one setting, the subscription type. An exclusive subscription is held by a
single consumer and the server keeps no redelivery count for it; a shared subscription dispatches
to competing consumers and counts what each message was handed out for. Both rows run one consumer,
so what separates them is the bookkeeping the server does, not the cost of sharing a subscription.

A figure reported as `indistinguishable` is one whose two sides differ by less than the spread
between runs of either. That is the honest outcome wherever the transport costs far more than the
code above it, and a figure below the run-to-run noise would read as precision that was never
measured, so none is published.

A row marked `broker-bound` is one the transport paced. It is decided by arithmetic, not by a
guess: a probe outside the rounds times a request the client waits for an answer to, and the row
is marked when the round trips a delivery costs already account for half of what a delivery took.
The probe's result is published with the machine below, so the check can be repeated.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-pulsar/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one topic, a small body and a server on the loopback. It measures what a
delivery costs in this crate, not what Pulsar can carry, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last settle is issued,
on all three loops alike. Pulsar acknowledges by writing a command to the socket and waits for no
answer, so where the server records it is outside the number everywhere.

Each run reads a persistent topic of its own in the default namespace, unpartitioned, fed over a
second connection. The stand is Pulsar standalone in a single container, so the broker, its storage
and the benchmark share one machine. A cluster on real hardware answers a different question, and
answers it about the deployment rather than about this crate.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about a quarter of an
hour and wants the machine to itself. The message count is not fixed: a probe run sets it so that
every measured run lasts at least five seconds on whatever machine it is taken on. Every run owns a
fresh topic and drops it when it is done, so a long session does not leave the stand carrying its
history.
