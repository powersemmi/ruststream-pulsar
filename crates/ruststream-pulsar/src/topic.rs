//! [`PulsarTopic`]: validated first-class topic addressing.
//!
//! A Pulsar topic name carries four independent meanings (persistence, tenant, namespace,
//! topic); the client treats it as a plain string and defers errors to the broker. The
//! newtype validates on construction instead, so a malformed name fails before any I/O.

use crate::error::PulsarError;

/// A validated Pulsar topic name.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::PulsarTopic;
///
/// let topic = PulsarTopic::persistent("acme", "orders", "created");
/// assert_eq!(topic.as_str(), "persistent://acme/orders/created");
///
/// let parsed = PulsarTopic::parse("persistent://acme/orders/created")?;
/// assert_eq!(parsed, topic);
/// # Ok::<(), ruststream_pulsar::PulsarError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[must_use]
pub struct PulsarTopic {
    full: String,
}

fn valid_part(part: &str) -> bool {
    !part.is_empty()
        && part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

impl PulsarTopic {
    fn of(scheme: &str, tenant: &str, namespace: &str, topic: &str) -> Result<Self, PulsarError> {
        for (label, part) in [
            ("tenant", tenant),
            ("namespace", namespace),
            ("topic", topic),
        ] {
            if !valid_part(part) {
                return Err(PulsarError::Invalid(format!(
                    "{label} '{part}' must be non-empty and contain only alphanumerics, '-', '_', '.'"
                )));
            }
        }
        Ok(Self {
            full: format!("{scheme}://{tenant}/{namespace}/{topic}"),
        })
    }

    /// A persistent topic: `persistent://tenant/namespace/topic`.
    ///
    /// # Panics
    ///
    /// Panics when a component is empty or carries characters Pulsar rejects; use
    /// [`parse`](Self::parse) for fallible construction from untrusted input.
    pub fn persistent(tenant: &str, namespace: &str, topic: &str) -> Self {
        Self::of("persistent", tenant, namespace, topic).expect("invalid pulsar topic component")
    }

    /// A non-persistent topic: `non-persistent://tenant/namespace/topic`.
    ///
    /// # Panics
    ///
    /// Panics when a component is empty or carries characters Pulsar rejects; use
    /// [`parse`](Self::parse) for fallible construction from untrusted input.
    pub fn non_persistent(tenant: &str, namespace: &str, topic: &str) -> Self {
        Self::of("non-persistent", tenant, namespace, topic)
            .expect("invalid pulsar topic component")
    }

    /// Parses a fully qualified name (`persistent://t/ns/topic`), a `tenant/namespace/topic`
    /// triple (defaulting to persistent), or a bare topic name (defaulting to
    /// `persistent://public/default/`).
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Invalid`] when the shape or a component is invalid.
    pub fn parse(name: &str) -> Result<Self, PulsarError> {
        let (scheme, rest) = name.strip_prefix("non-persistent://").map_or_else(
            || {
                name.strip_prefix("persistent://")
                    .map_or(("persistent", name), |rest| ("persistent", rest))
            },
            |rest| ("non-persistent", rest),
        );
        let parts: Vec<&str> = rest.split('/').collect();
        match parts.as_slice() {
            [topic] => Self::of(scheme, "public", "default", topic),
            [tenant, namespace, topic] => Self::of(scheme, tenant, namespace, topic),
            _ => Err(PulsarError::Invalid(format!(
                "topic '{name}' must be 'topic', 'tenant/namespace/topic', or fully qualified"
            ))),
        }
    }

    /// The fully qualified name sent to the broker.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.full
    }

    /// The persistence of the topic: `"persistent"` or `"non-persistent"`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_pulsar::PulsarTopic;
    ///
    /// let ticks = PulsarTopic::non_persistent("acme", "telemetry", "ticks");
    /// assert_eq!(ticks.persistence(), "non-persistent");
    /// ```
    #[must_use]
    pub fn persistence(&self) -> &str {
        self.parts().0
    }

    /// The tenant the topic belongs to; `public` for a bare name.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_pulsar::PulsarTopic;
    ///
    /// assert_eq!(PulsarTopic::parse("orders")?.tenant(), "public");
    /// # Ok::<(), ruststream_pulsar::PulsarError>(())
    /// ```
    #[must_use]
    pub fn tenant(&self) -> &str {
        self.parts().1
    }

    /// The namespace the topic lives in; `default` for a bare name.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_pulsar::PulsarTopic;
    ///
    /// assert_eq!(PulsarTopic::parse("acme/orders/created")?.namespace(), "orders");
    /// # Ok::<(), ruststream_pulsar::PulsarError>(())
    /// ```
    #[must_use]
    pub fn namespace(&self) -> &str {
        self.parts().2
    }

    /// The topic's own name, without tenant, namespace or scheme.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_pulsar::PulsarTopic;
    ///
    /// assert_eq!(PulsarTopic::parse("acme/orders/created")?.name(), "created");
    /// # Ok::<(), ruststream_pulsar::PulsarError>(())
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        self.parts().3
    }

    /// Splits the qualified name back into its four meanings.
    ///
    /// Every value this type holds was built by [`Self::of`], which writes all four and rejects
    /// an empty one, so the split cannot come up short.
    fn parts(&self) -> (&str, &str, &str, &str) {
        let (scheme, rest) = self
            .full
            .split_once("://")
            .expect("a validated topic carries its scheme");
        let mut parts = rest.split('/');
        let mut next = || {
            parts
                .next()
                .expect("a validated topic carries tenant, namespace and name")
        };
        let tenant = next();
        let namespace = next();
        (scheme, tenant, namespace, next())
    }
}

impl std::fmt::Display for PulsarTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.full)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_qualify_fully() {
        assert_eq!(
            PulsarTopic::persistent("acme", "orders", "created").as_str(),
            "persistent://acme/orders/created"
        );
        assert_eq!(
            PulsarTopic::non_persistent("acme", "telemetry", "ticks").as_str(),
            "non-persistent://acme/telemetry/ticks"
        );
    }

    #[test]
    fn parse_defaults_bare_names_to_public_default() {
        assert_eq!(
            PulsarTopic::parse("orders").expect("parses").as_str(),
            "persistent://public/default/orders"
        );
    }

    /// The four meanings a name carries come back out of it, which is what the generated
    /// document reports about a channel.
    #[test]
    fn a_validated_topic_reports_its_four_parts() {
        let qualified =
            PulsarTopic::parse("non-persistent://acme/telemetry/ticks").expect("parses");
        assert_eq!(qualified.persistence(), "non-persistent");
        assert_eq!(qualified.tenant(), "acme");
        assert_eq!(qualified.namespace(), "telemetry");
        assert_eq!(qualified.name(), "ticks");

        let bare = PulsarTopic::parse("orders").expect("parses");
        assert_eq!(bare.persistence(), "persistent");
        assert_eq!(bare.tenant(), "public");
        assert_eq!(bare.namespace(), "default");
        assert_eq!(bare.name(), "orders");
    }

    #[test]
    fn parse_rejects_malformed_shapes() {
        assert!(PulsarTopic::parse("a/b").is_err());
        assert!(PulsarTopic::parse("persistent://a//c").is_err());
        assert!(PulsarTopic::parse("bad name").is_err());
    }
}
