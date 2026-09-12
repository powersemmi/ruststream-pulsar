//! Repositioning an in-process subscription over the stand-in's retained log.
//!
//! The stand-in is a log broker, so it backs the same seek surface the real one does: a service
//! that reads [`SeekHandle`](crate::SeekHandle) off its delivery context, or opens its
//! subscription with `start_at(..)`, mounts on [`PulsarTestBroker`](super::PulsarTestBroker)
//! unchanged and is tested with the framework's harness rather than against a server.
//!
//! The reposition completes inside the call, because the queue it swaps lives in the router
//! under the same lock as the log. That is what the harness needs: by the time the seek
//! resolves, the replay is already counted in flight, so a test that waits for quiescence waits
//! for the replayed deliveries too.

use std::sync::Arc;

use ruststream::testing::Coordinator;

use crate::message::PulsarPosition;
use crate::testing::broker::TestState;
use crate::testing::router::ConsumerId;

/// Repositions one in-process subscription; the value behind
/// [`PulsarSeeker`](crate::PulsarSeeker) on the stand-in.
#[derive(Debug, Clone)]
pub(crate) struct LogSeeker {
    state: Arc<TestState>,
    id: ConsumerId,
    /// A clone of the broker's harness coordinator, so a reposition keeps the in-flight count
    /// balanced. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl LogSeeker {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: ConsumerId,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            state,
            id,
            coordinator,
        }
    }

    /// Repositions the subscription and wakes it, so the next delivery reflects the new
    /// position before this returns.
    ///
    /// Infallible: the stand-in models routing over a log, not the connection ladder a real
    /// consumer's seek travels, so there is no wire for it to fail on. A seek through a handle
    /// whose subscription is gone (dropped, or the broker shut down) does nothing.
    pub(crate) fn seek(&self, to: &PulsarPosition) {
        self.state
            .router
            .seek(self.id, to, self.coordinator.as_ref());
    }
}
