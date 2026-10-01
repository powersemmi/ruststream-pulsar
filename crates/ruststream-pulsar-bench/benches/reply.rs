// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Replying: the handler returns a value, the runtime encodes it, and this crate's publisher
//! sends it to the topic the reply type declares and waits for the broker's receipt.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream_pulsar::PulsarSubscription;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(
    PulsarSubscription::new(common::topic(), common::SUBSCRIPTION),
    publish
)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(confirm);
    })
}

// About thirty-eight allocations per reply: the consume path's, the buffer the framework encodes
// the reply into, and the publish. Eight of the publish's are this crate's: six parse and format
// the destination into its full topic name on every publish, and two box the producer lookup and
// the client's send. The rest are the `pulsar` client's. Five runs allocated 288, 38,345 and
// 76,384 blocks over one, a thousand and two thousand deliveries, each the same in all five.
#[library_benchmark(config = common::config_every(38_039, 1_000, 306))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
