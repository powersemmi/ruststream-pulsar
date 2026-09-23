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

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "Full service", "adapterOverhead": "Crate overhead", "overhead": "Service overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "versions": "Versions", "measured": "Measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

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

## The crate's own code

<div id="benchmark-code"></div>

The second table is this crate's own cost per message, counted rather than timed: instructions
under callgrind and allocations under DHAT. Each scenario is the service a user writes, on
`PulsarBroker` against the same stand as the comparison: a shared subscription, the crate's
default, over a persistent topic, which keeps what is published to it until the service takes it.
The messages are published from a thread of their own before the count starts, and by then the
broker has stored every one of them. The batch scenario raises the batch wait so that every batch
fills: with the default of ten milliseconds, how full a batch gets under valgrind would depend on
the machine rather than on the code.

What is counted is everything on the service's thread while it starts and while it drains the
topic: the framework's dispatch, this crate's code, and the work of the `pulsar` client on that
thread, whose connection and consumer run on the service's runtime. Threads the client runs on its
own are not counted, and neither is the broker. Waiting on the socket costs no instructions. Most
of each figure is the client's: of the nineteen allocations a delivery costs, four are this
crate's.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what connecting, subscribing and taking the first
delivery cost once. The numbers are absolute, the framework's own cost included; the core publishes
that cost alone on its [benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

A socket is in the loop, so a count moves a little with timing. Over five runs of one binary the
per-message figures stayed within 0.03 percent in instructions, an allocation count moved by at
most two blocks over a whole run, and the cold start moved by up to 0.5 percent. Each allocation
floor is the highest count seen, and the limit sits a tenth of a percent above it, well short of
one more allocation per message. `just bench-code` fails when a scenario allocates above that
limit, and with `--baseline=main` on more than two percent more instructions, and a pull request
that changes the cost cites its numbers.

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

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes a few minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that
every measured run lasts at least five seconds on whatever machine it is taken on. Every run owns a
fresh topic and drops it when it is done, so a long session does not leave the stand carrying its
history.

```bash
just bench-code
```

The recipe starts the same stand, counts the code table under valgrind, stops the stand and
rewrites the `code` section of the same document. It takes about four minutes. It needs valgrind
and the benchmark runner: `cargo install --locked gungraun-runner --version =0.19.4`.
