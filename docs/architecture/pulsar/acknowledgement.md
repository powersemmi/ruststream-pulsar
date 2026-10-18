---
title: Acknowledgement
sidebar_position: 2
description: "A Pulsar consumer acknowledges a message or asks for it again; the client also asks after the timeout, and moves a message that came back too often aside."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One delivery on Pulsar: the client's dead-letter check, then an ack, a negative acknowledgement or no answer, and the redelivery both of the last two ask for",
  "text": "The diagram as text",
  "delivery": ["The broker delivers a message", "with its redelivery count n"],
  "check": ["The client's dead-letter policy:", "is n at max_redeliver_count?"],
  "deadLetter": ["At the limit:", "moved to the", "dead-letter", "topic, original", "acked"],
  "handler": ["Below the limit:", "the handler gets the message"],
  "ack": ["ack:", "the cursor", "moves past it"],
  "nack": ["nack:", "redelivery", "asked at once"],
  "timeout": ["no ack within", "ack_timeout:", "the client asks"],
  "again": ["The broker delivers it again", "with count n + 1"],
  "counted": ["Redeliveries are counted on Shared and KeyShared only:", "the other types deliver every message with a count of 0"]
};

# Acknowledgement

A Pulsar consumer acknowledges a message or asks for it again. The client also asks after the
timeout, and moves a message that came back too often aside.

<Diagram name="redelivery" words={words} />

An acknowledgement marks one message as done, and the cursor moves past every message that is
done. Pulsar also has a cumulative acknowledgement, which marks every message up to one, on
`Exclusive` and `Failover` subscriptions. This crate acknowledges each message on its own, whatever
the subscription type. The acknowledgement is a command written to the connection, and the broker
sends no answer to it.

A negative acknowledgement asks the broker to deliver a message again. The Rust client sends the
request at once; the Java client waits a configurable delay first. The broker delivers the message
again, to the same consumer or to another one of the subscription.

The acknowledgement timeout is a timer of the client. With a timeout set, the client remembers
when it handed over each message, checks twice a second, and asks the broker to deliver again
every message that waited longer than the timeout. Without one, a message nobody answers stays
with the consumer until it disconnects, and then goes back to the subscription.

The broker counts how many times it delivered a message again, on `Shared` and `KeyShared`
subscriptions. The dead-letter policy is the client's: when a message arrives with a count at the
limit, the client produces it to the dead-letter topic, adds the `REAL_TOPIC` and
`ORIGIN_MESSAGE_ID` properties, and acknowledges the original. The handler never sees it.

Pulsar has no command that rejects a message for good. A message leaves a subscription by an
acknowledgement, by the dead-letter policy, or when the
namespace's time to live expires it.

The behaviour is Apache Pulsar's
([acknowledgement](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment),
[negative acknowledgement](https://pulsar.apache.org/docs/next/concepts-messaging/#negative-acknowledgment),
[acknowledgement timeout](https://pulsar.apache.org/docs/next/concepts-messaging/#acknowledgment-timeout),
[dead-letter topic](https://pulsar.apache.org/docs/next/concepts-messaging/#dead-letter-topic)),
as the `pulsar` client 6.9 implements it. What each outcome of a handler sends is on
[Settlement](../settlement.md).
