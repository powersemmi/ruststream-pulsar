//! A pattern subscription's regular expression and the namespace the client lists for it.
//!
//! The client asks the server for the topics of one namespace and matches the regular expression
//! against their fully qualified names. That namespace comes from the pattern itself, as in the
//! Java client: `persistent://acme/orders/.*` and `acme/orders/.*` list `acme/orders`, and a
//! pattern with no `tenant/namespace/` part lists the client's default, `public/default`.

use regex::Regex;

use crate::error::PulsarError;
#[cfg(feature = "testing")]
use crate::topic::PulsarTopic;
use crate::topic::valid_part;

/// The namespace a pattern with no `tenant/namespace/` part lists, the client's own default.
const DEFAULT_TENANT: &str = "public";
const DEFAULT_NAMESPACE: &str = "default";

/// A parsed pattern: the regular expression and the one namespace it reads.
#[derive(Debug, Clone)]
pub(crate) struct TopicPattern {
    regex: Regex,
    tenant: String,
    namespace: String,
}

impl TopicPattern {
    /// Parses `pattern` once, at subscribe.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Invalid`], naming the pattern, when it is no regular expression or
    /// when its prefix names no single namespace: a tenant or a namespace that is itself a
    /// regular expression, or a lone `/` with no `tenant/namespace/` shape around it.
    pub(crate) fn parse(pattern: &str) -> Result<Self, PulsarError> {
        let regex = Regex::new(pattern).map_err(|err| {
            PulsarError::Invalid(format!("invalid topic pattern '{pattern}': {err}"))
        })?;
        // The scheme is left to the regular expression: the client lists persistent and
        // non-persistent topics alike, so it selects between them by full name.
        let rest = pattern.split_once("://").map_or(pattern, |(_, rest)| rest);
        let (tenant, namespace) = match rest.splitn(3, '/').collect::<Vec<_>>().as_slice() {
            [_] if !pattern.contains("://") => (DEFAULT_TENANT, DEFAULT_NAMESPACE),
            [tenant, namespace, _] if valid_part(tenant) && valid_part(namespace) => {
                (*tenant, *namespace)
            }
            _ => {
                return Err(PulsarError::Invalid(format!(
                    "topic pattern '{pattern}' must name one namespace: write it as \
                     'persistent://tenant/namespace/<regex>' or 'tenant/namespace/<regex>' with a \
                     literal tenant and namespace, or as a bare '<regex>' over public/default"
                )));
            }
        };
        Ok(Self {
            tenant: tenant.to_owned(),
            namespace: namespace.to_owned(),
            regex,
        })
    }

    /// The regular expression matched against fully qualified topic names.
    pub(crate) const fn regex(&self) -> &Regex {
        &self.regex
    }

    /// The `tenant/namespace` the client lists.
    pub(crate) fn lookup_namespace(&self) -> String {
        format!("{}/{}", self.tenant, self.namespace)
    }

    /// Whether the client's listing of this pattern's namespace selects `topic`.
    #[cfg(feature = "testing")]
    pub(crate) fn selects(&self, topic: &PulsarTopic) -> bool {
        topic.tenant() == self.tenant
            && topic.namespace() == self.namespace
            && self.regex.is_match(topic.as_str())
    }

    /// The pattern as the service wrote it.
    #[cfg(feature = "testing")]
    pub(crate) fn as_str(&self) -> &str {
        self.regex.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace_of(pattern: &str) -> String {
        TopicPattern::parse(pattern)
            .expect("the pattern parses")
            .lookup_namespace()
    }

    #[test]
    fn a_qualified_prefix_names_the_namespace() {
        assert_eq!(namespace_of("persistent://acme/orders/.*"), "acme/orders");
        assert_eq!(
            namespace_of("non-persistent://acme/orders/created-.*"),
            "acme/orders"
        );
        assert_eq!(
            namespace_of("(non-)?persistent://acme/orders/.*"),
            "acme/orders"
        );
        assert_eq!(namespace_of("acme/orders/.*"), "acme/orders");
    }

    #[test]
    fn a_bare_pattern_reads_the_default_namespace() {
        assert_eq!(namespace_of("orders-.*"), "public/default");
    }

    #[test]
    fn a_prefix_naming_no_single_namespace_is_refused_with_the_pattern() {
        for pattern in [
            "persistent://acme/(orders|billing)/.*",
            "persistent://.*/orders/.*",
            "persistent://acme/orders",
            "orders/.*",
        ] {
            let Err(PulsarError::Invalid(message)) = TopicPattern::parse(pattern) else {
                panic!("'{pattern}' must be refused");
            };
            assert!(message.contains(pattern), "{message}");
        }
    }

    #[cfg(feature = "testing")]
    #[test]
    fn a_pattern_selects_only_its_own_namespace() {
        let pattern = TopicPattern::parse("persistent://acme/orders/.*").expect("parses");
        let topic = |name| PulsarTopic::parse(name).expect("a topic name");
        assert!(pattern.selects(&topic("persistent://acme/orders/created")));
        assert!(!pattern.selects(&topic("persistent://public/default/created")));
    }
}
