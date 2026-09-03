//! The imports a service on Pulsar writes every time, in one glob.
//!
//! `use ruststream_pulsar::prelude::*;` brings in the framework's own prelude, this crate's
//! broker, descriptors, start position and seeker, the delivery and page contexts with the
//! [`Position`] and [`SeekHandle`] keys that read them, its publish policy
//! [`PulsarPublish`](crate::PulsarPublish) and its publish arguments, and the framework
//! capability traits [`Positioned`] and [`Seeker`].
//!
//! The policy keeps its prefixed name here. The bare `Publish` belongs to the framework - it is
//! the slot capability trait a handler bounds an out slot with - and an alias would shadow it
//! for every file that writes this glob.
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
//! // The policy is a unit struct: `PulsarPublish` is both the type and the value a mount site
//! // passes to `.publisher(..)` or `.out(..)`.
//! let policy: PulsarPublish = PulsarPublish;
//! # let _ = (broker, orders, policy);
//! ```

pub use ruststream::prelude::*;

pub use ruststream::{Positioned, Seeker};

pub use crate::{
    DeadLetter, Position, PulsarBatchContext, PulsarBroker, PulsarContext, PulsarPosition,
    PulsarPublish, PulsarPublishExt, PulsarSeeker, PulsarSubscription, PulsarTopic, SeekHandle,
    SubscriptionType,
};

// `Partitioned` stays out: the core surfaces `partition_key` through `IncomingMessage`'s
// defaulted method, so re-exporting the trait makes the natural call ambiguous (E0034).
