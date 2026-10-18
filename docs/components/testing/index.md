---
title: Testing
description: "TestApp::start connects PulsarBroker in process: topics, the four subscription types, dead letters, delayed retries and seeking."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`PulsarBroker`](../broker/index.md) in process: topics, the four subscription types, dead letters, delayed retries and seeking.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<PulsarBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.

The in-process mode has no settings of its own.
The default subscription, each descriptor's settings, the retry cap and its dead-letter topic, and the payload limit are the broker's and the registration's.
So a test runs the configuration production runs.
It refuses what a server refuses, with the same error: a service URL the client cannot parse, a destination that is no topic name, a payload over the limit, and a handle used after shutdown.
A second consumer of an `Exclusive` subscription is refused at once, where a server would keep it waiting.

The mode keeps Pulsar's semantics:

- `orders` and `persistent://public/default/orders` are one topic.
- A message reaches every subscription over its topic once, and the [subscription type](../subscriptions/subscription-type.md) picks the consumer within one.
- A negatively acknowledged delivery goes back to its subscription, and the dead-letter policy counts deliveries as the client does.
- A subscription starts at the end of its topics, and a [seek](../positions/seek.md) moves it over the log the test published.
- A [pattern](../subscriptions/pattern.md) reads a topic created later from the next listing, thirty seconds on.
- A header value that is not UTF-8 text fails the publish, as the client frames headers as text.

A publish through the harness returns once every handler it woke has settled, a reply and a dead-letter move included.
The assertions that follow read finished state.
A delayed retry and a pattern's listing run on the runtime's clock, so [`tb.advance`](api:ruststream::testing::TestApp::advance) on a paused clock drives both.
What only a server has is for a [live test](./live.md).

[Testing a Pulsar service](../../guides/testing.md) tests a card payments service in process.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-pulsar = { version = "0.7", features = ["testing"] }
```

The first test publishes an empty payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_testing.rs#testapp"
```

</TabItem>
</Tabs>

A payment over the review limit is retried after a minute.
On a paused clock the minute passes when the test advances it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_testing.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_testing.rs#advance"
```

</TabItem>
</Tabs>
