---
title: Methodology
description: "How ruststream-pulsar measures itself: the three loops, the run on a real broker, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-pulsar` measures itself: the three loops, the run on a real broker, the
broker-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on Pulsar.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** A consumer of the `pulsar` crate, driven directly: receive, decode, acknowledge.
- **Broker crate.** This crate and nothing above it: the broker connects, a
  [`PulsarSubscription`](api:ruststream_pulsar::PulsarSubscription) opens the subscription, and a
  loop pulls deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the subscription and its type, the place of the ack, the
decode into the same struct, the payload bytes, the tokio runtime with four worker threads, and the
binary.

The body is 128 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh topic and a fresh subscription per run.** The topic is persistent and unpartitioned, in
  the `public/default` namespace, and it is deleted through the admin API after the run, so a run
  never sees what an earlier one left.
- **The consumer attaches first.** A producer on a second connection then feeds it, at most 20 000
  messages ahead of what the consumer has handled.
- **The window opens at the first delivery and closes at the last acknowledgement**, in all three
  loops. Connecting, subscribing and creating the topic are outside it. An acknowledgement is a
  command written to the socket that awaits no answer, so no loop can see the server record it.
- **The message count comes from a probe**, which measures the raw loop's rate over 50 000
  messages, so a measured run lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when the raw client spent the run waiting on the transport. That is
arithmetic: a probe outside the rounds times a topic lookup and its answer, the cheapest request the
client waits for. The consumer itself waits on no round trip: it grants receive permits in advance
and acknowledges without an answer. Each delivery does carry one, the publish behind it, which the
producer holds open until the broker has stored the message. The row is flagged when that round
trip reaches half the measured time per message. The round trip is published with the machine, so
the arithmetic can be checked.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on
[`PulsarBroker`](api:ruststream_pulsar::PulsarBroker) against the same broker, built without the
`testing` feature. A `Shared` subscription reads a persistent topic of the run's own. Another thread,
with its own runtime and connection, fills the topic through this crate's publisher before the
measured region opens, so every message is stored before the service takes the first.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery, then
1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

A run against a saved baseline fails on two percent more instructions in any scenario. Allocations
are held to a floor each scenario declares, on every run.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both comparison scenarios, stops
the stand and rewrites `docs/benchmarks/results.json`. It takes a few minutes and wants the machine
to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on. Both recipes
build the benchmarks the way a release is built, with fat link-time optimization across crates, so
the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes a minute or two and needs valgrind. `just bench-code 5000` measures over a
larger count for a steadier number. `just bench-code --save-baseline=main` on `main` records a
baseline, and `just bench-code --baseline=main` on a branch compares with it.
