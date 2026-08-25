//! The imports a service on Pulsar writes every time, in one glob.
//!
//! `use ruststream_pulsar::prelude::*;` brings in the framework's own prelude, this crate's
//! user-facing surface (the broker, the topic and subscription descriptors, the start position
//! and its seeker, the publish policy, and the publish arguments this crate adds to the
//! builder), and the framework capability traits a service on this broker writes for itself.
//!
//! The framework's prelude stops short of brokers on purpose, because which broker a service
//! runs on is the one thing every service states for itself. Importing *this* prelude is that
//! statement: the broker is named by the crate path, so the core glob rides along and one import
//! serves a service file.
//!
//! # A capability manifest
//!
//! The capability traits re-exported below are the ones this broker supports *and* a service
//! writes for itself: either in a bound, or to call a method on a value the runtime hands it.
//! Here they are both of the second kind - a delivery reports its own log position
//! ([`Positioned`]), and the handle a `Seek` parameter binds repositions a live subscription
//! ([`Seeker`], whose `seek` is a trait method and so needs the trait in scope).
//!
//! What the glob leaves out is as deliberate. Pulsar has no transactions, no consumer-side batch
//! receive and no reply inbox on this client, so `TransactionalPublisher`, `OwnedTransactions`,
//! `RequestReply` and `BatchSubscriber` are absent and a service reaching for one gets a missing
//! bound rather than a surprise at runtime. `Seekable` is absent even though `PulsarSubscriber`
//! implements it: it is how the runtime mints the seeker, and a service names the seeker's type,
//! never that trait. `Partitioned` is absent for a sharper reason - a delivery's key is already
//! reachable as `message.partition_key()` through [`IncomingMessage`], which the framework's
//! prelude carries, so adding the trait would only make that call ambiguous.
//!
//! Because these re-exports name the framework's own items rather than copies of them, globbing
//! two broker preludes into one file is safe: both resolve to the same `ruststream` traits, and
//! the compiler verifies that rather than taking anyone's word for it.
//!
//! # Policies under their concept names
//!
//! The same principle runs on the policy layer. Every publishing mode this broker supports is
//! re-exported here under its concept name with the broker prefix stripped -
//! [`PulsarPublish`](crate::PulsarPublish) as [`Publish`] - so a mount site reads
//! `.publisher(Publish)` whichever broker it runs on, and moving a service between brokers is an
//! import change rather than a rewrite. The absence of a concept name is the statement that this
//! broker has no such mode: there is no `Transaction` here because the client implements none.
//!
//! The prefixed originals stay at the crate root, which is what a file mounting two brokers at
//! once reaches for when both would answer to the same concept name.
//!
//! [`Publish`] is a **policy** - the declaration the runtime pairs with the connected broker -
//! and not the framework's `runtime::Publish`, the builder that a publish call assembles itself.
//! The two never meet in a service: the builder is reached through methods, never named, and it
//! is not in the framework's prelude.
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
//! // The policy is a unit struct, so the concept name is both the type and the value a mount
//! // site passes to `.publisher(..)` or `.out(..)`.
//! let policy: Publish = Publish;
//! # let _ = (broker, orders, policy);
//! ```

// The framework's prelude first: a service file needs both, and the broker-specificity this
// glob adds lives in the crate path, not in a second import line.
pub use ruststream::prelude::*;

// The capability half of the manifest: the framework traits a service writes itself, either in a
// bound or to call a method on a value the runtime hands it. Both here are the second kind, and
// both name a method with no second home, so the glob cannot make a call ambiguous.
pub use ruststream::{Positioned, Seeker};

// The policy vocabulary, under concept names rather than prefixed ones: see the module docs.
// The prefixed originals stay at the crate root for a file that mounts two brokers at once.
pub use crate::PulsarPublish as Publish;

pub use crate::{
    DeadLetter, PulsarBroker, PulsarPosition, PulsarPublishExt, PulsarSeeker, PulsarSubscription,
    PulsarTopic, SubscriptionType,
};

// `Partitioned` is implemented here, but the core surfaces `partition_key` through
// `IncomingMessage`'s defaulted method - re-exporting the trait would make the natural call
// ambiguous (E0034).
//
// `Seekable` is implemented (by `PulsarSubscriber`) but absent: it is subscriber-side plumbing
// the runtime uses to mint the seeker, and a service names `PulsarSeeker` instead.
//
// `Subscribe`, `DefaultPublish` and `DescribeServer` are implemented and absent for the same
// kind of reason: they are the contract between this crate and the framework, not vocabulary a
// service writes.
//
// The `testing` surface is deliberately absent: it is feature-gated broker-author tooling, not
// user API, and a test names `ruststream_pulsar::testing::*` where it sets a harness up.
//
// `PARTITION_KEY_HEADER` and the types the runtime hands a handler rather than the ones a
// service writes (`PulsarMessage`, `PulsarSubscriber`, `PulsarPublisher`,
// `ConnectedPulsarBroker`, `PartitionKeyed`) are absent for the same reason the framework's
// prelude leaves out its outgoing message type: code that reaches for them is working a layer
// below a service, and the explicit import says so. `PulsarSeeker` is the exception, because a
// handler names it in its own signature: `Seek(seeker): Seek<PulsarSeeker>`.
//
// `PulsarError` is absent because a service names errors where it handles them, not everywhere.
