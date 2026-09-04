//! Apache Pulsar broker implementation for `RustStream`.
//!
//! Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
//! transport over the [`pulsar`](https://docs.rs/pulsar) client maintained by `StreamNative`.
//!
//! - Four subscription types as an enum with per-variant meaning (exclusive, shared, failover,
//!   key-shared) - combinations that do not exist are unrepresentable.
//! - The consumer-side dead-letter policy carries its delivery-attempt limit, and the ack
//!   timeout redelivers automatically; the Pulsar server enforces both, this crate does not
//!   emulate them.
//! - [`PulsarTopic`] validates the four meanings a topic name carries (persistence, tenant,
//!   namespace, topic) on construction instead of at first use.
//! - Multi-topic and pattern subscriptions are descriptor variants.
//! - Topics are a retained log, so a handler repositions its own subscription: [`PulsarContext`]
//!   carries the delivery's [`Position`] and the subscription's [`SeekHandle`], read by key.
//! - Page handlers work here. The client hands over one delivery at a time, so a page is
//!   assembled on the client from the size the mount site names; only the deadline that closes a
//!   partial page is this crate's own, on [`PulsarSubscription::page_wait`].
//! - Key sharing maps onto the partition key; message properties carry headers directly, with no
//!   extra envelope format. [`PulsarPublishExt`] names the key as a publish argument, ahead of
//!   the framework's publish builder.
//!
//! Transactions and the schema registry are out of scope: the client does not implement them,
//! and the capability traits they would back are optional.

#![forbid(unsafe_code)]

mod broker;
mod context;
mod error;
mod message;
pub mod prelude;
mod publisher;
mod subscriber;
mod subscription;
#[cfg(feature = "testing")]
pub mod testing;
mod topic;

pub use broker::{ConnectedPulsarBroker, PulsarBroker};
pub use context::{Position, PulsarBatchContext, PulsarContext, SeekHandle};
pub use error::PulsarError;
pub use message::{PARTITION_KEY_HEADER, PulsarMessage, PulsarPosition};
pub use publisher::{PartitionKeyed, PulsarPublish, PulsarPublishExt, PulsarPublisher};
pub use subscriber::{PulsarSeeker, PulsarSubscriber};
pub use subscription::{DeadLetter, PulsarSubscription, SubscriptionType};
pub use topic::PulsarTopic;
