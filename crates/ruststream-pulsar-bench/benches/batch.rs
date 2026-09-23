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
//! Consuming in batches of 64: the subscription assembles the batch on the client out of the
//! deliveries the client hands over one at a time, the handler takes a slice, and the runtime
//! settles every delivery in it.
//!
//! The batch wait is raised from its default so that every batch but the last fills: with the
//! default of ten milliseconds, how full a batch gets under valgrind would depend on the machine
//! rather than on the code.

mod common;

use std::hint::black_box;
use std::time::Duration;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream_pulsar::PulsarSubscription;

#[subscriber(
    PulsarSubscription::new(common::topic(), common::SUBSCRIPTION)
        .batch_wait(Duration::from_secs(1))
)]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume.batch(nonzero!(64)));
    })
}

// About eighteen allocations per delivery, this crate's four among them as on the consume path;
// the batch itself allocates once per batch. Five runs allocated 277, 20,110 and 38,296 to
// 38,298 blocks over one, a thousand and two thousand deliveries; the floor takes the highest.
#[library_benchmark(config = common::config_every(18_188, 1_000, 1_922))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
