//! In-process test support, behind the `testing` feature.
//!
//! [`PulsarTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness.
//!
//! It routes by topic name over a retained per-address log, which makes a subscription
//! repositionable in process: a service that opens with `start_at(..)`, or repositions itself
//! through the [`SeekHandle`](crate::SeekHandle) key, mounts here unchanged and is tested with
//! the harness rather than against a server.
//!
//! The crate's routes-file vocabulary is the same here as in production.
//! [`PulsarPublish`](crate::PulsarPublish) pairs against this broker into
//! [`PulsarTestPublisher`], and is its default publish policy, so `.out(Reply, Publish)` and the
//! unadorned `include` both mount unchanged; there is no test-only policy to name.
//!
//! [`PulsarSubscription`](crate::PulsarSubscription) is a source for this broker as well as for
//! the real one, so a service is tested through the declaration it ships rather than a
//! test-only rewrite of it. Its addressing is honoured in full: a single topic, the list of
//! [`topics`](crate::PulsarSubscription::topics), and the regular expression of
//! [`pattern`](crate::PulsarSubscription::pattern), which is matched against every topic
//! published to - so a topic that first appears after the subscription opened reaches it, as it
//! does on a server.
//!
//! The rest of a descriptor describes work the Pulsar server does, and the stand-in ignores it:
//! [`subscription_type`](crate::PulsarSubscription::subscription_type),
//! [`dead_letter`](crate::PulsarSubscription::dead_letter) and
//! [`ack_timeout`](crate::PulsarSubscription::ack_timeout) carry no behaviour here, and nor do
//! credit or redelivery timing. Two consequences are worth naming, because a test could
//! otherwise assert what a real broker will not do:
//!
//! * every subscription covering a topic receives every message published to it, so two
//!   handlers sharing one [`SubscriptionType::Shared`](crate::SubscriptionType::Shared)
//!   subscription name each see the whole stream in process, where a server would split it
//!   between them;
//! * a delivery nacked past [`max_deliveries`](crate::DeadLetter::max_deliveries) keeps being
//!   redelivered instead of moving to the dead-letter topic.
//!
//! Both are Pulsar product behaviour, verified end to end against a real broker.
//!
//! Topic names route literally: the stand-in has no namespace to resolve them against, so
//! `orders` and `persistent://public/default/orders` are two addresses here and one topic on a
//! server. A test names its topics the way its handlers do and never notices; one that mixes
//! the two spellings sees no traffic rather than a wrong result. A pattern is matched against
//! the same literal name, while a server matches it against the fully qualified one, so an
//! unanchored `orders-.*` selects the same topics either way but a `^`-anchored pattern over a
//! bare name matches here and nowhere else.

mod broker;
mod router;
pub(crate) mod seek;
mod subscriber;

pub use broker::{ConnectedPulsarTestBroker, PulsarTestBroker, PulsarTestPublisher};
pub use subscriber::{PulsarTestMessage, PulsarTestSubscriber};
