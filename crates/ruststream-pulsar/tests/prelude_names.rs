//! What the crate prelude must not take away from the framework's.
//!
//! The glob re-exports `ruststream::prelude::*` first, and an explicit re-export beside it wins
//! over the glob silently. So a name this crate adds under one of the framework's own is not a
//! collision the compiler reports at the prelude - it is a compile error in every service file
//! that later writes the framework's name. These probes fail at the right place instead: here.

use ruststream_pulsar::prelude::*;

/// `Publish` is the framework's slot capability trait, the bound a handler puts on an out slot.
/// This crate's publish policy keeps its prefixed name so it cannot shadow it.
fn _publish_is_the_core_trait<T: Publish>() {}

/// The policy is still one glob away, under its own name.
fn _the_policy_is_reachable() {
    let _: PulsarPublish = PulsarPublish;
}
