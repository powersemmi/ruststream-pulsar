//! The imports a routes file on Pulsar writes every time, in one glob.
//!
//! `use ruststream_pulsar::prelude::*;` brings in the framework's own prelude, this crate's
//! broker, descriptors, start position and seeker, the delivery and batch contexts with the
//! [`Position`] and [`SeekHandle`] keys that read them, its publish policy under the name
//! [`Publish`] with its per-message settings and the step that adjusts them, and the framework
//! capability traits [`Positioned`] and [`Seeker`].
//!
//! Two vocabularies, kept apart by which prelude a file writes. A handler body names framework
//! things only - it imports `ruststream::prelude::*` and bounds an injected slot with the
//! broker capability trait it needs (`Out<impl Publisher>` and friends). The one exception is a
//! body that names the [`partition_key`](crate::PulsarPublishSteps::partition_key) step: it
//! imports this glob too and bounds its slot
//! `Out<impl Publisher<Options = PulsarPublishOptions>, Marker>`. A routes file names
//! the broker's mount-site vocabulary - it imports this glob, where each publishing mode this
//! broker supports appears under its concept name with the prefix stripped, so
//! `out_reply(Publish)` reads the same whichever broker a service runs on and moving between
//! brokers is an import change. Here that is one name, [`Publish`]; the absence of
//! `TransactionalPublish` is the statement that Pulsar's client has no transactions. The
//! prefixed [`PulsarPublish`](crate::PulsarPublish) stays at the crate root for a file that
//! mounts two brokers at once and must tell their policies apart.
//!
//! # Examples
//!
//! ```
//! # mod demo {
//! use ruststream_pulsar::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Outgoing, Serialize)]
//! #[outgoing(name = "receipts")]
//! struct Receipt {
//!     id: u64,
//! }
//!
//! #[subscriber(
//!     PulsarSubscription::new("orders", "workers").subscription_type(SubscriptionType::Shared),
//!     publish
//! )]
//! async fn confirm(order: &Order) -> Receipt {
//!     Receipt { id: order.id }
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
//!         PulsarBroker::new("pulsar://localhost:6650"),
//!         |b| {
//!             b.include(confirm)
//!                 .out_reply(Publish)
//!                 .max_attempts(nonzero!(5))
//!                 .dead_letter("orders-dlq");
//!         },
//!     )
//! }
//! # }
//! # fn main() {}
//! ```

pub use ruststream::prelude::*;

pub use ruststream::{Positioned, Seeker};

pub use crate::PulsarPublish as Publish;

pub use crate::{
    OperationRetries, Position, PulsarBatchContext, PulsarBroker, PulsarContext, PulsarPosition,
    PulsarPublishOptions, PulsarPublishSteps, PulsarSeeker, PulsarSubscription, PulsarTopic,
    SeekHandle, SubscriptionType,
};

// `Partitioned` stays out: the core surfaces `partition_key` through `IncomingMessage`'s
// defaulted method, so re-exporting the trait makes the natural call ambiguous (E0034).
