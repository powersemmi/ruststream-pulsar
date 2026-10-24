//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod asyncapi;
mod conformance_pulsar;
mod delayed_retry;
mod delivery_topic;
mod descriptor_sources;
mod harness_manual;
mod integration_pulsar;
mod live_subscription_types;
mod live_topics;
mod prelude_names;
mod publish_options;
mod retry_declaration;
mod seek_context;
mod subscription_types;
