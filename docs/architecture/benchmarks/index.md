---
title: Benchmarks
description: "What ruststream-pulsar costs on every message: throughput against the raw pulsar client, and instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {pulsar} from '@site/src/components/Benchmarks/pulsar';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-pulsar` costs on every message: throughput against the raw `pulsar` client, and
instructions and allocations of its code.

## Against a raw client

The first chart runs one scenario three ways on a real Pulsar broker. `Raw client` drives a
consumer of the `pulsar` crate directly. `Broker crate` drives this crate's subscription with a loop
of its own and no runtime. `RustStream` is the service a user writes, a `#[subscriber]` handler in
an app. The first gap is what this crate costs over the client it wraps, and the second is what the
runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

The two scenarios differ in the [subscription type](../pulsar/subscriptions.md). The server keeps a
redelivery count for every message of a `Shared` subscription and none for an `Exclusive` one. Both
run one consumer, so what separates them is that bookkeeping, not the cost of sharing.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
[`PulsarBroker`](api:ruststream_pulsar::PulsarBroker) against the same broker, with a `Shared`
subscription over a persistent topic. The count includes the framework's dispatch, this crate's code
and the work of the `pulsar` client, whose connection and consumer run on the service's runtime. The
broker is another process and is not counted. `Cold start` is what connecting, subscribing and
taking the first message cost once.

## Results

<Results entry={pulsar} labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={pulsar} labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
