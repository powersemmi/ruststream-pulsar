//! What a publish through the in-process transport costs for the headers it carries.
//!
//! The publisher is handed the header map the publish filled, and hands it on rather than
//! copying it: the only copy left is the transport's own log entry. The cost is read off this
//! thread's allocation counter, because content equality and addresses cannot tell a hand-over
//! from a copy.
#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ruststream::{Broker, BytesMut, HeaderMap, OutgoingMessage, Publisher, Take};
use ruststream_pulsar::testing::PulsarTestBroker;

/// Counts this thread's allocations, so the cost of one publish can be read off directly. A
/// thread-local count rather than a global one: nothing another thread does belongs in this
/// measurement.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// The headers a publish carries here: what a transform stamps on the way out.
fn stamped() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-stamp", b"1".to_vec());
    headers.insert("x-tenant", b"acme".to_vec());
    headers
}

/// What one publish to `topic` costs, with the topic's own log already opened by a warm-up
/// publish before the count, so the two measurements differ in their headers alone.
async fn spent<P: Publisher<Payload = Take>>(
    publisher: &P,
    topic: &str,
    headers: HeaderMap,
) -> usize {
    publisher
        .publish(
            OutgoingMessage::produced(topic, BytesMut::from(&b"{}"[..])),
            None,
        )
        .await
        .map_err(|err| err.to_string())
        .expect("the in-process transport accepts the warm-up");

    let msg = OutgoingMessage::produced(topic, BytesMut::from(&b"{}"[..])).with_headers(headers);
    let before = allocations();
    publisher
        .publish(msg, None)
        .await
        .map_err(|err| err.to_string())
        .expect("the in-process transport accepts the publish");
    allocations() - before
}

/// What one copy of that map costs, measured on a map of its own so the publish's copy is a
/// first copy too.
// The copy is the measurement, so `redundant_clone` is reading the intent backwards here.
#[allow(clippy::redundant_clone)]
fn one_copy() -> usize {
    let headers = stamped();
    let before = allocations();
    let copy = headers.clone();
    let spent = allocations() - before;
    drop(copy);
    spent
}

/// The transport takes the map the publish filled: what two headers cost a publish is one copy,
/// the log entry the transport keeps for itself, and nothing above it.
#[tokio::test]
async fn a_publish_costs_one_copy_of_its_header_map() {
    let connected = PulsarTestBroker::new()
        .connect()
        .await
        .expect("the stand-in connects");
    let publisher = connected.publisher();

    // A topic apiece, so both measured publishes are the second one on a fresh log and the
    // transport's own bookkeeping is the same under either.
    let with_headers = spent(&publisher, "with-headers", stamped()).await;
    let without_headers = spent(&publisher, "without-headers", HeaderMap::new()).await;

    assert_eq!(
        with_headers - without_headers,
        one_copy(),
        "two headers cost the publish the log entry's own copy of the map and nothing more \
         ({with_headers} with, {without_headers} without)",
    );
}
