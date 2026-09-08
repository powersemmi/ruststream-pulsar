//! In-process test support, behind the `testing` feature.
//!
//! [`PulsarTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness.
//!
//! It routes by exact address match over a retained per-address log, which makes a subscription
//! repositionable in process: a service that opens with `start_at(..)`, or repositions itself
//! through the [`SeekHandle`](crate::SeekHandle) key, mounts here unchanged and is tested with
//! the harness rather than against a server. What the stand-in does not simulate is Pulsar's
//! product behaviour (subscription types, dead-letter policies, credit, redelivery timing);
//! that is verified end to end against a real broker.

mod broker;
mod router;
pub(crate) mod seek;
mod subscriber;

pub use broker::{
    ConnectedPulsarTestBroker, PulsarTestBroker, PulsarTestPublish, PulsarTestPublisher,
};
pub use subscriber::{PulsarTestMessage, PulsarTestSubscriber};
