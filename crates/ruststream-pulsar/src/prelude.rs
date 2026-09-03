//! The imports a service on Pulsar writes every time, in one glob.
//!
//! `use ruststream_pulsar::prelude::*;` brings in the framework's own prelude, this crate's
//! broker, descriptors, start position and seeker, the delivery and page contexts with the
//! [`Position`] and [`SeekHandle`] keys that read them, its publish policy under the name
//! [`Publish`] and its publish arguments, and the framework capability traits [`Positioned`]
//! and [`Seeker`].
//!
//! A file that mounts two brokers at once reaches for the prefixed names at each crate root
//! ([`PulsarPublish`](crate::PulsarPublish)) to tell their policies apart.
//!
//! [`Publish`] is a publish policy, not the framework's `runtime::Publish` builder, which a
//! service never names.
//!
//! # Examples
//!
//! ```
//! use ruststream_pulsar::prelude::*;
//!
//! let broker = PulsarBroker::new("pulsar://localhost:6650");
//! let orders = PulsarSubscription::new("orders", "workers")
//!     .subscription_type(SubscriptionType::Shared)
//!     .dead_letter(DeadLetter::new("orders-dlq").max_deliveries(5));
//!
//! // The policy is a unit struct: `Publish` is both the type and the value a mount site passes
//! // to `.publisher(..)` or `.out(..)`.
//! let policy: Publish = Publish;
//! # let _ = (broker, orders, policy);
//! ```

pub use ruststream::prelude::*;

pub use ruststream::{Positioned, Seeker};

pub use crate::PulsarPublish as Publish;

pub use crate::{
    DeadLetter, Position, PulsarBatchContext, PulsarBroker, PulsarContext, PulsarPosition,
    PulsarPublishExt, PulsarSeeker, PulsarSubscription, PulsarTopic, SeekHandle, SubscriptionType,
};

// `Partitioned` stays out: the core surfaces `partition_key` through `IncomingMessage`'s
// defaulted method, so re-exporting the trait makes the natural call ambiguous (E0034).
