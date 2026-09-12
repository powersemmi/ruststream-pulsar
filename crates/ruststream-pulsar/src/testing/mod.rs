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
//! The ladder is the real one, terminal state included: after `shutdown`, a publisher taken
//! before it and any clone of the connected form report
//! [`PulsarError::NotConnected`](crate::PulsarError::NotConnected) instead of routing into a
//! dead transport. Every framework suite this crate's capabilities justify - the routing suite,
//! `harness::lifecycle`, `capabilities::seeking` and `capabilities::batches` - runs against this
//! broker as well as against a server, so what it claims to follow is checked rather than
//! assumed.
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
//! So is the sharing rule. A message reaches every subscription over its topic, and within one
//! subscription the [`subscription_type`](crate::PulsarSubscription::subscription_type) picks the
//! consumer that takes it: [`Exclusive`](crate::SubscriptionType::Exclusive) holds the
//! subscription for one consumer and refuses a second attach,
//! [`Failover`](crate::SubscriptionType::Failover) delivers to the active consumer and promotes a
//! standby when it leaves, [`Shared`](crate::SubscriptionType::Shared) rotates, and
//! [`KeyShared`](crate::SubscriptionType::KeyShared) splits by partition key. Two handlers on
//! one shared subscription therefore split a run between them in process, as they do in
//! production, and a `nack(requeue = true)` goes back to the subscription, so a retry can land on
//! a sibling.
//!
//! Where that stops short of a server, and why:
//!
//! * `KeyShared` assigns by the key's hash modulo the consumer count, not by Pulsar's hash
//!   RANGES. One key stays on one consumer, which is what a test rests on, but which consumer
//!   that is differs from a server's, and so does what a consumer joining or leaving reshuffles.
//! * A seek moves the consumer that asked for it. On a server the cursor belongs to the
//!   subscription, so a seek from one consumer of a shared subscription moves its siblings too.
//! * A delivery nacked past [`max_deliveries`](crate::DeadLetter::max_deliveries) keeps coming
//!   back instead of moving to the dead-letter topic:
//!   [`dead_letter`](crate::PulsarSubscription::dead_letter) needs the server's per-message
//!   delivery count, which this transport does not keep.
//! * [`ack_timeout`](crate::PulsarSubscription::ack_timeout), credit and redelivery timing carry
//!   no behaviour here either; they are the server's clock, not the transport's.
//!
//! The last two are product behaviour the live suite covers against a real broker; the first two
//! are where this model is coarser than the server's, and a test that leans on either is leaning
//! on the wrong broker.
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
