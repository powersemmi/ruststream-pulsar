//! Which topics a subscription reads, in the server's own terms, and the routing answer the test
//! harness asks a connected broker for.
//!
//! A topic is known by its fully qualified name here, as it is on a server: `orders` and
//! `persistent://public/default/orders` are one topic. A pattern subscription resolves against
//! the lookup namespace the client lists, `public/default`, and matches its regular expression
//! against the fully qualified names in it, which is what the Pulsar client does.

use std::sync::{Mutex, MutexGuard};

use regex::Regex;

use crate::error::PulsarError;
use crate::subscription::{PulsarSubscription, Topics};
use crate::topic::PulsarTopic;

/// The namespace a pattern subscription lists its topics in.
///
/// The client asks the server for the topics of one namespace and matches the pattern against
/// those; this crate names no other, so the client's own default applies.
const LOOKUP_TENANT: &str = "public";
const LOOKUP_NAMESPACE: &str = "default";

/// Which topics one subscription reads.
#[derive(Debug, Clone)]
pub(crate) enum Route {
    /// Exact topics, fully qualified: one for a single-topic subscription, several for a list.
    Topics(Vec<String>),
    /// A regular expression over the fully qualified names of the lookup namespace.
    Pattern(Regex),
}

impl Route {
    /// The route a descriptor reads, with every topic name qualified the way the server
    /// qualifies it.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Invalid`] for a topic name or a pattern the descriptor's own
    /// validation refuses.
    pub(crate) fn of(descriptor: &PulsarSubscription) -> Result<Self, PulsarError> {
        match &descriptor.topics {
            Topics::List(topics) => topics
                .iter()
                .map(|topic| PulsarTopic::parse(topic).map(|topic| topic.as_str().to_owned()))
                .collect::<Result<_, _>>()
                .map(Self::Topics),
            Topics::Pattern(pattern) => Regex::new(pattern).map(Self::Pattern).map_err(|err| {
                PulsarError::Invalid(format!("invalid topic pattern '{pattern}': {err}"))
            }),
        }
    }

    /// Whether this route can read `topic` at all. A pattern reads a topic only once the client
    /// has listed it, which the router decides from the topic's age; this is the half that does
    /// not depend on time.
    pub(crate) fn selects(&self, topic: &PulsarTopic) -> bool {
        match self {
            Self::Topics(topics) => topics.iter().any(|name| name == topic.as_str()),
            Self::Pattern(pattern) => {
                topic.tenant() == LOOKUP_TENANT
                    && topic.namespace() == LOOKUP_NAMESPACE
                    && pattern.is_match(topic.as_str())
            }
        }
    }

    /// Whether this is a pattern, which resolves its topics by listing them rather than by name.
    pub(crate) const fn is_pattern(&self) -> bool {
        matches!(self, Self::Pattern(_))
    }

    /// The exact topics of a list route; a pattern names none.
    pub(crate) fn topics(&self) -> &[String] {
        match self {
            Self::Topics(topics) => topics,
            Self::Pattern(_) => &[],
        }
    }

    /// Whether two routes can read the same topic, which is what makes two consumers of one
    /// subscription name members of one subscription rather than of two that share a name.
    ///
    /// Two patterns are compared by their source text: the case that matters, one descriptor
    /// mounted twice, is exact, and deciding whether two regular expressions intersect is not.
    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Topics(mine), Self::Topics(theirs)) => {
                mine.iter().any(|topic| theirs.contains(topic))
            }
            (Self::Topics(topics), pattern @ Self::Pattern(_))
            | (pattern @ Self::Pattern(_), Self::Topics(topics)) => topics
                .iter()
                .any(|topic| PulsarTopic::parse(topic).is_ok_and(|topic| pattern.selects(&topic))),
            (Self::Pattern(mine), Self::Pattern(theirs)) => mine.as_str() == theirs.as_str(),
        }
    }
}

/// One subscription the connected broker opened: the name the framework reports it under, the
/// topics it reads, and the durable subscription it joined.
#[derive(Debug)]
struct Opened {
    name: String,
    route: Route,
    subscription: String,
}

/// Every subscription a connected broker opened, in the order it opened them, on either
/// transport: the routing answer is asked of a broker connected to a server too.
#[derive(Debug, Default)]
pub(crate) struct Subscriptions {
    opened: Mutex<Vec<Opened>>,
}

impl Subscriptions {
    /// Records a subscription the broker has just opened: the name the framework reports it
    /// under, the durable subscription it joined, and the topics it reads.
    pub(crate) fn record(&self, name: String, subscription: String, route: Route) {
        self.lock().push(Opened {
            name,
            route,
            subscription,
        });
    }

    /// The positions in `names` of the subscriptions a message published to `destination`
    /// reaches, one per durable subscription.
    ///
    /// Pulsar hands a message to every subscription over its topic, once each, and within one
    /// subscription to one of its consumers. So the answer names each durable subscription the
    /// topic reaches once, by the first of its consumers in `names`: the harness counts what a
    /// subscription handled by the name it reports, and competing consumers mounted on one
    /// descriptor report one name, whichever of them the server picked. A name this broker did
    /// not open reaches the topic it spells.
    pub(crate) fn routes(&self, destination: &str, names: &[&str]) -> Vec<usize> {
        let Ok(topic) = PulsarTopic::parse(destination) else {
            // The server refuses the publish, so it reaches nobody.
            return Vec::new();
        };
        let opened = self.lock();
        let mut reached: Vec<&str> = Vec::new();
        let mut positions = Vec::new();
        for (position, name) in names.iter().enumerate() {
            // The k-th subscription reported under a name is the k-th one opened under it.
            let occurrence = names[..position]
                .iter()
                .filter(|seen| *seen == name)
                .count();
            let record = opened
                .iter()
                .filter(|record| record.name == *name)
                .nth(occurrence);
            let (selects, subscription) = record.map_or_else(
                || {
                    (
                        PulsarTopic::parse(name).is_ok_and(|named| named == topic),
                        *name,
                    )
                },
                |record| (record.route.selects(&topic), record.subscription.as_str()),
            );
            if selects && !reached.contains(&subscription) {
                reached.push(subscription);
                positions.push(position);
            }
        }
        drop(opened);
        positions
    }

    /// Nothing here panics while the list is held, so the lock cannot have been poisoned.
    fn lock(&self) -> MutexGuard<'_, Vec<Opened>> {
        self.opened
            .lock()
            .expect("the record is read and written without panicking")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(subscriptions: &Subscriptions, descriptor: &PulsarSubscription) {
        subscriptions.record(
            descriptor.source_name().to_owned(),
            descriptor.subscription().to_owned(),
            Route::of(descriptor).expect("a valid descriptor"),
        );
    }

    /// Two subscriptions over one topic each get the message; two consumers of one subscription
    /// share it, so only the first of them is owed it.
    #[test]
    fn a_message_is_owed_once_per_subscription() {
        let subscriptions = Subscriptions::default();
        record(
            &subscriptions,
            &PulsarSubscription::new("orders", "workers"),
        );
        record(
            &subscriptions,
            &PulsarSubscription::new("orders", "workers"),
        );
        record(&subscriptions, &PulsarSubscription::new("orders", "audit"));

        assert_eq!(
            subscriptions.routes("orders", &["orders", "orders", "orders"]),
            [0, 2]
        );
    }

    /// The server qualifies a bare name, so both spellings are one topic.
    #[test]
    fn both_spellings_of_a_topic_are_one_topic() {
        let subscriptions = Subscriptions::default();
        record(
            &subscriptions,
            &PulsarSubscription::new("orders", "workers"),
        );

        assert_eq!(
            subscriptions.routes("persistent://public/default/orders", &["orders"]),
            [0]
        );
        assert!(
            subscriptions
                .routes("non-persistent://public/default/orders", &["orders"])
                .is_empty()
        );
    }

    /// A pattern is matched against the fully qualified name, in the lookup namespace only.
    #[test]
    fn a_pattern_reads_the_lookup_namespace_by_full_name() {
        let subscriptions = Subscriptions::default();
        record(
            &subscriptions,
            &PulsarSubscription::pattern("audit-.*", "audit"),
        );
        record(
            &subscriptions,
            &PulsarSubscription::pattern("^audit-.*", "anchored"),
        );

        assert_eq!(
            subscriptions.routes("audit-eu", &["audit", "anchored"]),
            [0]
        );
        assert!(
            subscriptions
                .routes("acme/eu/audit-eu", &["audit", "anchored"])
                .is_empty()
        );
    }
}
