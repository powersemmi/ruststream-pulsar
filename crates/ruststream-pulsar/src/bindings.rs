//! The `AsyncAPI` binding objects this crate writes, shared by both of its sides.
//!
//! A channel is described the same way whether a subscription reads it or a publish reaches it,
//! so the body and the version live here rather than once per side.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

use crate::topic::PulsarTopic;

/// The version of the `pulsar` binding object this crate writes, as the specification numbers it.
const BINDING_VERSION: &str = "0.1.0";

/// One standard binding, or none when the body will not serialize.
///
/// A binding that fails to build is a binding the document goes without: a broker never holds up
/// a service over a description of itself.
pub(crate) fn one<T: Serialize>(protocol: &'static str, body: &T) -> Bindings {
    Binding::new(protocol, BINDING_VERSION, body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// One extension binding, for what the specification has no field for.
pub(crate) fn extension<T: Serialize>(name: &'static str, body: &T) -> Bindings {
    Binding::extension(name, body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// The `pulsar` channel binding of the `AsyncAPI` specification, as far as a client knows it.
///
/// `compaction`, `geo-replication`, `retention`, `ttl` and `deduplication` are namespace and
/// topic policies an operator sets outside the client, so this crate has no value to report for
/// them and reports none rather than a guess.
#[derive(Serialize)]
struct PulsarChannel<'a> {
    namespace: &'a str,
    persistence: &'a str,
}

/// What a topic name says about its channel: the namespace it lives in and whether it is stored.
///
/// The name is the only place either value appears, so a side that holds a name holds the whole
/// binding - the subscription descriptor's topics on one side, the destination the mount site
/// resolved on the other.
pub(crate) fn topic_channel(topic: &PulsarTopic) -> Bindings {
    let body = PulsarChannel {
        namespace: topic.namespace(),
        persistence: topic.persistence(),
    };
    one("pulsar", &body)
}
