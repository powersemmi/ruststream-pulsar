//! The contexts a Pulsar subscription hands its handlers, and the keys that read them.
//!
//! Pulsar topics are a retained log, so the two things a body needs to reposition itself are
//! per-delivery state: where this message sits ([`Position`]) and the handle that moves the
//! subscription ([`SeekHandle`]). Both ride [`PulsarContext`], built once per delivery; a page
//! spans many deliveries, so [`PulsarBatchContext`] carries only the subscription-scoped half.

use ruststream::{BuildBatchContext, BuildContext, ContextField, Field, Positioned};

use crate::message::{PulsarMessage, PulsarPosition};
use crate::subscriber::PulsarSeeker;

/// The per-delivery context of a Pulsar subscription: this message's position in the topic's
/// retained log, and the subscription's own seeker.
///
/// The runtime builds one per dispatched delivery (see [`BuildContext`]), and handlers read its
/// fields by key - [`Position`] and [`SeekHandle`] - through the `Ctx` extractor or
/// `ctx.context(..)`. A body that repositions its own subscription names this type as its `C`
/// axis and needs nothing else.
///
/// Building it costs no allocation: the position is the delivery's message id, and the seeker
/// is a clone of the channel to the subscription's driver task.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::prelude::*;
/// use ruststream_pulsar::{Position, PulsarContext, SeekHandle};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<Job, (), (), PulsarContext> for Replayer {
///     async fn handle(
///         &self,
///         job: &Job,
///         _outs: &(),
///         ctx: &mut Context<'_, PulsarContext>,
///     ) -> Result<(), HandlerOutcome> {
///         let here = ctx.context(Position).clone();
///         if job.id == u64::MAX && ctx.context(SeekHandle).seek(here).await.is_err() {
///             return Err(HandlerOutcome::retry());
///         }
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug)]
pub struct PulsarContext {
    position: PulsarPosition,
    seeker: PulsarSeeker,
}

impl BuildContext<PulsarMessage> for PulsarContext {
    fn build(msg: &PulsarMessage) -> Self {
        Self {
            position: Positioned::position(msg),
            seeker: PulsarSeeker::new(msg.driver().clone()),
        }
    }
}

/// The in-process stand-in retains a log of its own, so a service that repositions itself needs
/// no second shape to be unit-tested: the same context, built off the in-process delivery.
#[cfg(feature = "testing")]
impl BuildContext<crate::testing::PulsarTestMessage> for PulsarContext {
    fn build(msg: &crate::testing::PulsarTestMessage) -> Self {
        Self {
            position: Positioned::position(msg),
            seeker: msg.seeker().clone(),
        }
    }
}

/// The subscription-scoped page context of a Pulsar subscription: its seeker, shared by every
/// delivery of the page.
///
/// The runtime builds one per dispatched page from the page's first delivery (see
/// [`BuildBatchContext`]), and a page body reads it by key - [`SeekHandle`] - through
/// `ctx.context(..)`. A [`Position`] has no place here: a page spans many deliveries, so where
/// to seek rides the elements themselves (a `&[Message<H, T>]` page reads it off each element's
/// header contract). Keeping this a separate type from [`PulsarContext`] is what rejects a page
/// body asking for per-delivery fields at compile time.
///
/// Pulsar's client has no consumer-side batch receive, so a page here is assembled on the client
/// from the size the mount site's `batch(n)` names. Nothing about that reaches the body: the
/// context, the seeker and the settlement are the same either way.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::prelude::*;
/// use ruststream_pulsar::{PulsarBatchContext, SeekHandle};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<[Job], (), (), PulsarBatchContext> for Replayer {
///     async fn handle(
///         &self,
///         page: &[Job],
///         _outs: &(),
///         ctx: &mut Context<'_, PulsarBatchContext>,
///     ) -> Result<(), Vec<HandlerOutcome>> {
///         // A page that saw the rewind marker replays the retained backlog once it is
///         // settled; the next page opens at the beginning of the log.
///         if page.iter().any(|job| job.id == u64::MAX)
///             && ctx
///                 .context(SeekHandle)
///                 .seek(PulsarPosition::earliest())
///                 .await
///                 .is_err()
///         {
///             return Err(page.iter().map(|_| HandlerOutcome::retry()).collect());
///         }
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug)]
pub struct PulsarBatchContext {
    seeker: PulsarSeeker,
}

impl BuildBatchContext<PulsarMessage> for PulsarBatchContext {
    fn build(first: &PulsarMessage) -> Self {
        Self {
            seeker: PulsarSeeker::new(first.driver().clone()),
        }
    }
}

/// The page counterpart on the stand-in, so a page body that repositions is unit-testable too.
#[cfg(feature = "testing")]
impl BuildBatchContext<crate::testing::PulsarTestMessage> for PulsarBatchContext {
    fn build(first: &crate::testing::PulsarTestMessage) -> Self {
        Self {
            seeker: first.seeker().clone(),
        }
    }
}

/// The key reading this delivery's [`PulsarPosition`] out of [`PulsarContext`].
///
/// The value is the delivery's own message id, so it carries the framework's pinned contract:
/// seeking to it redelivers exactly this message.
///
/// A message id is not `Copy`, so `ctx.context(Position)` borrows it and only the `Ctx<Position>`
/// extractor - which must yield an owned value - copies one per delivery.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::Position;
/// use ruststream_pulsar::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// #[subscriber("jobs")]
/// async fn audit(job: &Job, Ctx(at): Ctx<Position>) -> HandlerOutcome {
///     println!("job {} sits at {at:?}", job.id);
///     HandlerOutcome::ack()
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Position;

impl ContextField for Position {
    type Context = PulsarContext;
    type Value = PulsarPosition;

    fn read(self, src: &PulsarContext) -> PulsarPosition {
        src.position.clone()
    }
}

impl Field<PulsarContext> for Position {
    type Value<'a> = &'a PulsarPosition;

    fn get(self, src: &PulsarContext) -> &PulsarPosition {
        &src.position
    }
}

/// The key reading the subscription's [`PulsarSeeker`] out of [`PulsarContext`] (and out of
/// [`PulsarBatchContext`] on the page path): the reposition handle every delivery of the
/// subscription shares.
///
/// One seek covers every topic and every partition of the subscription's consumer.
///
/// # Examples
///
/// ```
/// use ruststream_pulsar::SeekHandle;
/// use ruststream_pulsar::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, poisoned_until: Option<u64> }
///
/// /// Skips forward past a region the producer marked poisoned.
/// #[subscriber("jobs")]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if let Some(resume_at) = job.poisoned_until
///         && seeker.seek(PulsarPosition::timestamp(resume_at)).await.is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SeekHandle;

impl ContextField for SeekHandle {
    type Context = PulsarContext;
    type Value = PulsarSeeker;

    fn read(self, src: &PulsarContext) -> PulsarSeeker {
        src.seeker.clone()
    }
}

impl Field<PulsarContext> for SeekHandle {
    type Value<'a> = &'a PulsarSeeker;

    fn get(self, src: &PulsarContext) -> &PulsarSeeker {
        &src.seeker
    }
}

impl Field<PulsarBatchContext> for SeekHandle {
    type Value<'a> = &'a PulsarSeeker;

    fn get(self, src: &PulsarBatchContext) -> &PulsarSeeker {
        &src.seeker
    }
}
