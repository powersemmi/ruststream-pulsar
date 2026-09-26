//! Repositioning an in-process subscription over the transport's retained log.
//!
//! The reposition completes inside the call, because the queues it swaps live in the router under
//! the same lock as the log. That is what the harness needs: by the time the seek resolves, the
//! replay is already counted in flight, so a test that waits for quiescence waits for the
//! replayed deliveries too.

use std::sync::Arc;

use ruststream::testing::Coordinator;

use crate::in_process::bus::Bus;
use crate::in_process::router::ConsumerId;
use crate::message::PulsarPosition;

/// Repositions one in-process subscription; the value behind
/// [`PulsarSeeker`](crate::PulsarSeeker) on a broker connected in process.
#[derive(Debug, Clone)]
pub(crate) struct LogSeeker {
    bus: Arc<Bus>,
    id: ConsumerId,
    /// The harness coordinator, so a reposition keeps the in-flight count balanced. `None`
    /// outside a harness run.
    coordinator: Option<Coordinator>,
}

impl LogSeeker {
    pub(crate) const fn new(
        bus: Arc<Bus>,
        id: ConsumerId,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            bus,
            id,
            coordinator,
        }
    }

    /// Repositions the subscription and wakes its consumers, so the next delivery reflects the
    /// new position before this returns. A seek through a handle whose subscription is gone
    /// (dropped, or the broker shut down) does nothing.
    pub(crate) fn seek(&self, to: &PulsarPosition) {
        self.bus
            .router()
            .seek(self.id, to, self.coordinator.as_ref());
    }
}
