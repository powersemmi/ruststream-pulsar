//! The imports a routes file on Pulsar writes every time, in one glob.
//!
//! `use ruststream_pulsar::prelude::*;` brings in the framework's own prelude, this crate's
//! broker, descriptors, start position and seeker, the delivery and batch contexts with the
//! [`Position`] and [`SeekHandle`] keys that read them, its publish policy under the name
//! [`Publish`] and its publish arguments, and the framework capability traits [`Positioned`]
//! and [`Seeker`].
//!
//! Two vocabularies, kept apart by which prelude a file writes. A handler body names framework
//! things only - it imports `ruststream::prelude::*` and bounds an injected slot with the
//! broker capability trait it needs (`Out<impl Publisher>` and friends). A routes file names
//! the broker's mount-site vocabulary - it imports this glob, where each publishing mode this
//! broker supports appears under its concept name with the prefix stripped, so
//! `.publisher(Publish)` reads the same whichever broker a service runs on and moving between
//! brokers is an import change. Here that is one name, [`Publish`]; the absence of
//! `TransactionalPublish` is the statement that Pulsar's client has no transactions. The
//! prefixed [`PulsarPublish`](crate::PulsarPublish) stays at the crate root for a file that
//! mounts two brokers at once and must tell their policies apart.
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
