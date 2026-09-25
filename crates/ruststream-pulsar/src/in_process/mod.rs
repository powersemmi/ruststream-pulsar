//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber, its publisher and its delivery type each carry this
//! transport as a variant of their own, so a service's descriptors, publish policies and handlers
//! run against it unchanged. It has no configuration of its own: the default subscription, the
//! retry declarations and every descriptor setting come from the production broker. It never
//! succeeds where a server fails: a destination that is no topic name, a descriptor the client
//! refuses, a second consumer of an exclusive subscription and a handle outliving its connection are refused here with the error the live broker
//! returns.
//!
//! What it models: fully qualified topic names; single-topic, multi-topic and pattern
//! subscriptions, a pattern reading the `public/default` namespace by full name and a topic
//! created after it opened from the client's next listing, thirty seconds on; the four
//! subscription types, with one delivery per subscription and the type choosing the consumer;
//! negative acknowledgement back to the subscription; the client's dead-letter policy; delayed
//! retries on the runtime's clock; the retained log a seek or `start_at` repositions over,
//! subscription-wide; and a publish framed the way the client frames it. What belongs to the
//! server and is left to the live mode: `KeyShared` hash ranges (a key stays on one consumer
//! here, but which one differs), which consumer of a shared subscription a server picks, the
//! acknowledgement timeout's own redelivery, partitioned topics, and a backlog kept while a
//! subscription has no consumer.

mod bus;
mod deliveries;
mod route;
mod router;
mod seek;

use std::sync::Arc;

use url::Url;

use crate::error::PulsarError;
use crate::in_process::router::Membership;
use crate::subscriber::PulsarSubscriber;
use crate::subscription::PulsarSubscription;

pub(crate) use bus::Bus;
pub(crate) use deliveries::{Queued, Settlement};
pub(crate) use route::{Route, Subscriptions};
pub(crate) use router::Delivery;
pub(crate) use seek::LogSeeker;

/// Refuses a service URL the client cannot connect with, as `connect` does before it dials.
///
/// # Errors
///
/// Returns [`PulsarError::Connect`] for a URL that does not parse, names no host, or uses a
/// scheme other than `pulsar` and `pulsar+ssl`. The client parses one URL, so a comma-separated
/// broker list is refused here as `connect` refuses it.
pub(crate) fn check_url(url: &str) -> Result<(), PulsarError> {
    let parsed = Url::parse(url).map_err(|err| {
        PulsarError::Connect(Box::from(format!(
            "the service URL is not one the client can connect with: {err}"
        )))
    })?;
    if !matches!(parsed.scheme(), "pulsar" | "pulsar+ssl") {
        return Err(PulsarError::Connect(Box::from(format!(
            "the service URL's scheme {:?} is not one the client dials: pulsar or pulsar+ssl",
            parsed.scheme()
        ))));
    }
    if parsed.host_str().is_none() {
        return Err(PulsarError::Connect(Box::from(
            "the service URL names no host for the client to connect to",
        )));
    }
    Ok(())
}

/// Opens a subscription for `descriptor` reading `route` on the in-process transport.
///
/// # Errors
///
/// Returns [`PulsarError::Invalid`] for a retry declaration the client cannot apply, and
/// [`PulsarError::Subscribe`] for a second consumer of an exclusive subscription.
pub(crate) fn subscribe(
    bus: &Arc<Bus>,
    descriptor: PulsarSubscription,
    route: Route,
) -> Result<PulsarSubscriber, PulsarError> {
    let display = descriptor.display_topic();
    let dead_letter = descriptor.dead_letter_policy()?;
    let membership = Membership::new(descriptor.subscription, descriptor.sub_type);
    let id = bus
        .router()
        .subscribe(route, membership, dead_letter)
        .map_err(|held| PulsarError::Subscribe {
            topic: display.clone(),
            source: Box::new(held),
        })?;
    Ok(PulsarSubscriber::in_process(
        display,
        Queued::new(Arc::clone(bus), id, descriptor.ack_timeout),
        descriptor.batch_wait,
    ))
}

#[cfg(test)]
mod url_tests {
    use super::check_url;

    #[test]
    fn a_url_the_client_does_not_dial_is_refused() {
        assert!(check_url("pulsar://broker:6650").is_ok());
        assert!(check_url("pulsar+ssl://broker:6651").is_ok());
        for refused in [
            "http://broker:6650",
            "pulsar://one:6650,two:6650",
            "broker:6650",
        ] {
            assert!(check_url(refused).is_err(), "{refused}");
        }
    }
}
