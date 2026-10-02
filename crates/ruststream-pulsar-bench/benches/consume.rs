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
//! Consuming a small JSON body: the client delivers it, the subscription turns it into the
//! crate's message, the dispatcher decodes it into a struct, the handler reads a field, and the
//! runtime acks it through the subscription's driver.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream_pulsar::PulsarSubscription;

#[subscriber(PulsarSubscription::new(common::topic(), common::SUBSCRIPTION))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume);
    })
}

// About nineteen allocations per delivery. Four are this crate's, all in turning the client's
// message into its own and settling it: the header map, the copy of the payload, the topic name,
// and the channel an ack waits on. The rest are the `pulsar` client's frame decoding and the
// channels between its tasks. Five runs allocated 273, 20,285 and 39,147 blocks over one, a
// thousand and two thousand deliveries, each the same in all five.
#[library_benchmark(config = common::config_every(18_862, 1_000, 1_423))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
