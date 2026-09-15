//! The server's own view of a topic, for the live suites.
//!
//! A delivery says what this client saw; the admin API says what the broker holds - the type it
//! recorded for a subscription, the backlog left on it, the partitions a topic was created with.
//! The live suites assert on both, so a setting that never left the client cannot pass.
//!
//! It speaks HTTP over a socket rather than through a client crate: these are a handful of reads
//! against the stand on the loopback interface, and the stand is the one this repository's
//! `docker-compose.test.yml` and the CI job start, which publish the admin port beside the
//! service port.

use std::fmt::Write as _;
use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::Value;

/// The HTTP port a Pulsar broker serves its admin API on, beside the service port the suites
/// connect to.
const ADMIN_PORT: u16 = 8080;

/// How long a request may take before the suite fails instead of waiting on a wedged broker.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The namespace a bare topic name lives in, which is where the suites create their topics.
const NAMESPACE: &str = "public/default";

/// The admin address of the stand `PULSAR_TEST_URL` names.
///
/// # Panics
///
/// Panics when the variable is unset: only the live suites reach this, and they have already
/// returned when there is no stand.
fn address() -> String {
    let url = std::env::var("PULSAR_TEST_URL").expect("the live suites check the URL first");
    let rest = url
        .split_once("://")
        .map_or(url.as_str(), |(_, rest)| rest)
        .split(',')
        .next()
        .unwrap_or_default();
    let host = rest.rsplit_once(':').map_or(rest, |(host, _)| host);
    format!("{host}:{ADMIN_PORT}")
}

/// One request, and the body of a successful response.
///
/// # Panics
///
/// Panics when the broker cannot be reached or answers anything but a 2xx, naming the request
/// and what came back: a live suite has no useful fallback for an admin API that is not there.
fn request(method: &str, path: &str, body: Option<&str>) -> String {
    let address = address();
    let socket = address
        .parse()
        .expect("the stand address is a socket address");
    let mut stream = TcpStream::connect_timeout(&socket, REQUEST_TIMEOUT)
        .unwrap_or_else(|err| panic!("the admin API at {address} is not reachable: {err}"));
    stream
        .set_read_timeout(Some(REQUEST_TIMEOUT))
        .expect("a read timeout is set before the request goes out");
    let mut head = format!(
        "{method} /admin/v2/{path} HTTP/1.1\r\nHost: {address}\r\nAccept: application/json\r\n\
         Connection: close\r\n"
    );
    if let Some(body) = body {
        write!(
            head,
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
        .expect("a String never fails to take a write");
    }
    head.push_str("\r\n");
    if let Some(body) = body {
        head.push_str(body);
    }
    stream
        .write_all(head.as_bytes())
        .unwrap_or_else(|err| panic!("{method} {path} could not be sent: {err}"));
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .unwrap_or_else(|err| panic!("{method} {path} was not answered: {err}"));
    let response = String::from_utf8_lossy(&response).into_owned();
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("{method} {path} answered no HTTP response: {response}"));
    let status = head.lines().next().unwrap_or_default();
    assert!(
        status
            .split_whitespace()
            .nth(1)
            .is_some_and(|code| code.starts_with('2')),
        "{method} {path} answered {status}: {body}",
    );
    body.to_owned()
}

/// A `GET` of one admin path, as JSON.
///
/// # Panics
///
/// Panics when the request fails or the body is not JSON.
pub(crate) async fn get(path: String) -> Value {
    let body = tokio::task::spawn_blocking(move || request("GET", &path, None))
        .await
        .expect("the admin request runs to completion");
    serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("the admin API answered no JSON: {err}"))
}

/// Creates `topic` with `partitions` partitions, which is the only way a partitioned topic comes
/// into being: a producer never creates one.
///
/// # Panics
///
/// Panics when the broker refuses the request.
pub(crate) async fn create_partitioned_topic(topic: &str, partitions: u32) {
    let path = format!("persistent/{NAMESPACE}/{topic}/partitions");
    tokio::task::spawn_blocking(move || request("PUT", &path, Some(&partitions.to_string())))
        .await
        .expect("the admin request runs to completion");
}

/// What the broker holds for `subscription` on `topic`: its type, its backlog, its consumers.
///
/// # Panics
///
/// Panics when the topic has no such subscription, which means the descriptor never reached the
/// server.
pub(crate) async fn subscription_stats(topic: &str, subscription: &str) -> Value {
    let stats = get(format!("persistent/{NAMESPACE}/{topic}/stats")).await;
    stats
        .get("subscriptions")
        .and_then(|subscriptions| subscriptions.get(subscription))
        .cloned()
        .unwrap_or_else(|| panic!("the broker holds no subscription '{subscription}' on '{topic}'"))
}

/// The number this topic was created with, or `0` where the topic is not partitioned.
///
/// # Panics
///
/// Panics when the broker refuses the request.
pub(crate) async fn partition_count(topic: &str) -> u64 {
    get(format!("persistent/{NAMESPACE}/{topic}/partitions"))
        .await
        .get("partitions")
        .and_then(Value::as_u64)
        .unwrap_or_default()
}
