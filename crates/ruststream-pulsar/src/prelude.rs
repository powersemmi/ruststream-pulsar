//! The imports a service on Pulsar writes every time, in one glob.
//!
//! `use ruststream_pulsar::prelude::*;` carries the core's own prelude plus this crate's broker,
//! subscription descriptor and its options, positions, per-delivery context keys, the publish
//! policy under its concept name [`Publish`], the [`PulsarPublishSteps`] step and the capability
//! traits [`Positioned`] and [`Seeker`]. The
//! [Prelude](https://powersemmi.github.io/ruststream-pulsar/latest/components/broker/prelude/)
//! page shows it in a service.
//!
//! # Two vocabularies, two files
//!
//! A handler body imports `ruststream::prelude::*` and bounds an injected slot with the
//! **capability trait** it needs, `Out<impl Publisher>`, so it names no broker type at all. A
//! mount site imports this prelude, where the policy reads [`Publish`] as it does on every
//! broker. One handler body imports this prelude too: the one that names the
//! [`partition_key`](PulsarPublishSteps::partition_key) step, which needs the trait in scope and
//! the slot bounded as `Out<impl Publisher<Options = PulsarPublishOptions>, Marker>`.
//!
//! Globbing two broker preludes conflicts on `Publish` where it is used (E0659); the prefixed
//! [`PulsarPublish`](crate::PulsarPublish) at the crate root is the disambiguation.
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
//!     reply
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
