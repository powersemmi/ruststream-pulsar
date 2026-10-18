---
sidebar_position: 7
title: Errors
description: "PulsarError is the crate's one error type: connect, subscribe, receive, publish, an invalid descriptor and a closed connection."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`PulsarError`](api:ruststream_pulsar::PulsarError) is the crate's one error type: connect, subscribe, receive, publish, an invalid descriptor and a closed connection.

Each variant names the step that failed.
The client's error behind it is its `source`, so the client library's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the client could not parse the service URL or connect with it |
| `Subscribe` | the server refused a consumer, naming the topic or the pattern |
| `Receive` | an open consumer's stream failed or ended |
| `Publish` | a producer could not open, the server refused a message, the payload is over [`max_message_size`](./max-message-size.md), or a header value is not UTF-8 text |
| `NotConnected` | a publisher or a seek was used after the broker [shut down](./shutdown.md) |
| `Invalid` | a descriptor or a destination is not valid: an empty name, a malformed topic, a pattern that is no regular expression, a bare name with no default subscription |

A service meets the error in two places.
A failed [startup](./startup.md) carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A shipment the broker refuses is logged with its cause, and the order is settled as a retry.
After three deliveries the order moves to its dead-letter topic.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/pulsar_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/pulsar_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
