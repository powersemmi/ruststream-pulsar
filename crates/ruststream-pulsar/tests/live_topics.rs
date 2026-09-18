//! What a topic name selects on a real broker.
//!
//! Two of the descriptor's claims have no in-process counterpart worth trusting. A pattern
//! subscription resolves against the server's own namespace listing, which is where a topic
//! that appears later comes from. A partitioned topic is many topics behind one name, created
//! through the admin API and never by a producer, and the partition a message lands in is the
//! producer's choice - so the partition key either places the message or quietly does nothing.
//!
//! Start the stand with `just brokers-up`, then:
//! `PULSAR_TEST_URL=pulsar://127.0.0.1:6650 cargo test --all-features -- --test-threads=1`.

mod live;

use std::collections::{BTreeMap, BTreeSet};
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{ConnectedBroker, IncomingMessage, Outgoing, Serialized, Subscriber};
use ruststream_pulsar::{PulsarPublishSteps, PulsarSubscriber, PulsarSubscription};

use crate::live::{RECV_TIMEOUT, admin, connect, test_url, unique};

/// How long a subscription that should be quiet is watched before the claim is taken as proved.
const QUIET: Duration = Duration::from_secs(5);

/// How many partitions the partitioned checks create. Three, so a key landing on one of them is
/// a choice rather than the only option left.
const PARTITIONS: u32 = 3;

/// The payload these checks carry: what they assert on is where a message went, so the bytes
/// name themselves as the wire form and no codec runs on them.
#[derive(Outgoing, Serialized)]
struct Record(Vec<u8>);

/// Takes `count` deliveries, acknowledging each, and returns the topic each arrived on beside
/// its payload.
async fn collect(subscriber: &mut PulsarSubscriber, count: usize) -> Vec<(String, Vec<u8>)> {
    let mut stream = pin!(subscriber.stream());
    let mut seen = Vec::with_capacity(count);
    for _ in 0..count {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        seen.push((message.topic().to_owned(), message.payload().to_vec()));
        message.ack().await.expect("ack succeeds");
    }
    seen
}

/// The partition of a delivery's topic, which is the suffix the broker appends to the name the
/// service publishes to.
fn partition_of(topic: &str) -> &str {
    topic
        .rsplit_once("-partition-")
        .unwrap_or_else(|| panic!("'{topic}' is not a partition of a partitioned topic"))
        .1
}

/// The partition key places the message: every message under one key is stored in one partition,
/// which is what makes a `KeyShared` subscription able to keep that key in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_sends_every_message_to_one_partition() {
    let Some(url) = test_url() else { return };

    let topic = unique("keyed-partitions");
    admin::create_partitioned_topic(&topic, PARTITIONS).await;
    assert_eq!(
        admin::partition_count(&topic).await,
        u64::from(PARTITIONS),
        "the topic under test must be partitioned, or a key has nowhere to place a message",
    );

    let connected = connect(&url).await;
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    // The payload is the key, so a delivery says which key reached which partition.
    let keys = ["user-a", "user-b", "user-c", "user-d"];
    let rounds = 3;
    let publisher = connected.publisher();
    for _ in 0..rounds {
        for key in keys {
            publisher
                .message(&Record(key.as_bytes().to_vec()))
                .to(topic.as_str())
                .partition_key(key)
                .publish()
                .await
                .expect("publish succeeds");
        }
    }

    // The delivery's own topic is the partition the message was stored in, and the subscription
    // reads every partition of the topic, so this is where each key went.
    let mut partitions: BTreeMap<Vec<u8>, BTreeSet<String>> = BTreeMap::new();
    for (delivered_on, payload) in collect(&mut subscriber, keys.len() * rounds).await {
        partitions
            .entry(payload)
            .or_default()
            .insert(partition_of(&delivered_on).to_owned());
    }
    assert_eq!(partitions.len(), keys.len(), "every key must have arrived");
    for (key, used) in &partitions {
        assert_eq!(
            used.len(),
            1,
            "'{}' was spread over {used:?} instead of staying on one partition",
            String::from_utf8_lossy(key),
        );
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The other edge of the same setting: with no key the producer has nothing to place the message
/// by, so the partitions take the run in turn rather than all of it going to one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unkeyed_run_spreads_over_the_partitions() {
    let Some(url) = test_url() else { return };

    let topic = unique("unkeyed-partitions");
    admin::create_partitioned_topic(&topic, PARTITIONS).await;

    let connected = connect(&url).await;
    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::new(&topic, unique("sub")))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    for id in 0..PARTITIONS * 2 {
        publisher
            .message(&Record(vec![u8::try_from(id).expect("a small run")]))
            .to(topic.as_str())
            .publish()
            .await
            .expect("publish succeeds");
    }

    let used: BTreeSet<String> = collect(&mut subscriber, (PARTITIONS * 2) as usize)
        .await
        .into_iter()
        .map(|(delivered_on, _)| partition_of(&delivered_on).to_owned())
        .collect();
    assert_eq!(
        used.len(),
        PARTITIONS as usize,
        "an unkeyed run must reach every partition, got {used:?}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A pattern subscription reads the topics it matches and leaves the rest alone.
///
/// The pattern resolves against the server's namespace listing, so this is the only place the
/// selection can be checked: the topics exist on the broker, the match is the client's, and a
/// topic outside it belongs to another subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_subscription_reads_the_topics_it_matches() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    // The pattern is anchored on this run's own names, so a topic left over from an earlier run
    // is not a topic this one reads.
    let run = std::process::id();
    let matching = [
        format!("it-pattern-eu-{run}"),
        format!("it-pattern-us-{run}"),
    ];
    let other = format!("it-plain-{run}");
    let publisher = connected.publisher();
    for topic in matching.iter().chain(std::iter::once(&other)) {
        publisher
            .message(&Record(b"seed".to_vec()))
            .to(topic.as_str())
            .publish()
            .await
            .expect("the topic is created by publishing to it");
    }

    let mut subscriber = connected
        .subscribe_descriptor(PulsarSubscription::pattern(
            format!("it-pattern-..-{run}"),
            unique("pattern-sub"),
        ))
        .await
        .expect("subscription opens");

    for topic in matching.iter().chain(std::iter::once(&other)) {
        publisher
            .message(&Record(topic.as_bytes().to_vec()))
            .to(topic.as_str())
            .publish()
            .await
            .expect("publish succeeds");
    }

    let read: BTreeSet<Vec<u8>> = collect(&mut subscriber, matching.len())
        .await
        .into_iter()
        .map(|(_, payload)| payload)
        .collect();
    assert_eq!(
        read,
        matching
            .iter()
            .map(|topic| topic.as_bytes().to_vec())
            .collect::<BTreeSet<_>>(),
    );

    let mut stream = pin!(subscriber.stream());
    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "the pattern read a topic it does not match",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
