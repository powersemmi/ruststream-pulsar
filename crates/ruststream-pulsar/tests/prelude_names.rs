//! What the crate prelude must put in scope, and under which name.
//!
//! The glob re-exports `ruststream::prelude::*` and aliases this broker's policies over it, so
//! the two vocabularies meet here and nowhere else: a routes file writing this glob has to get
//! the mount-site name `Publish` for the policy, while the capability trait a handler body
//! bounds an injected slot with has to stay reachable. Both are checked by compiling, so a
//! re-export that took the wrong name fails in this crate's own suite rather than in a user's
//! service file.

use ruststream_pulsar::prelude::*;

/// A slot is bounded by the broker capability trait, and the glob keeps it reachable.
fn _slots_are_bounded_by_the_capability_trait<T: Publisher>() {}

/// The mount-site vocabulary: the policy answers to its concept name, as a type and as a value.
///
/// The value is spelled bare rather than `Publish::default()`: the policy is a unit struct, and
/// the workspace lints reject both the inherent `default()` on one and the `Default::default()`
/// that would dodge it. What is being pinned is the name, which either spelling would pin.
fn _the_policy_answers_to_its_concept_name() {
    let _: Publish = Publish;
}
