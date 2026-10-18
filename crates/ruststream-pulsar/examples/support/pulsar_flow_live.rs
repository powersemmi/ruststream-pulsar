//! The stand the live scenarios of the positions and settlement examples run against.
//!
//! A scenario runs when `PULSAR_TEST_URL` names a broker and returns quietly otherwise;
//! `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure, so a run that must reach the stand
//! cannot pass without it.

// Each example includes this module and calls the part its own scenario needs.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// The HTTP port a Pulsar broker serves its admin API on, beside the service port.
const ADMIN_PORT: u16 = 8080;

/// How long an admin request may take before the scenario fails instead of hanging.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The broker to run the live scenario against, or `None` to skip it.
///
/// # Panics
///
/// Panics when `RUSTSTREAM_REQUIRE_LIVE` is set and no URL is.
pub(crate) fn url() -> Option<String> {
    match std::env::var("PULSAR_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live scenario must run, but \
                 PULSAR_TEST_URL is missing or empty",
            );
            None
        }
    }
}

/// A name unique to this run: a subscription and its backlog outlive the run that made them.
pub(crate) fn unique(name: &str) -> String {
    format!("example-{name}-{}", std::process::id())
}

/// Deletes a topic of `public/default` with its subscriptions, so the scenario starts on an empty
/// log; a topic that does not exist yet is already that.
///
/// # Panics
///
/// Panics when the admin API is not reachable or refuses the request.
pub(crate) fn delete_topic(url: &str, topic: &str) {
    let host = url
        .split_once("://")
        .map_or(url, |(_, rest)| rest)
        .rsplit_once(':')
        .map_or(url, |(host, _)| host);
    let address = format!("{host}:{ADMIN_PORT}");
    let socket = address
        .parse()
        .expect("the stand address is a socket address");
    let mut stream = TcpStream::connect_timeout(&socket, REQUEST_TIMEOUT)
        .unwrap_or_else(|err| panic!("the admin API at {address} is not reachable: {err}"));
    stream
        .set_read_timeout(Some(REQUEST_TIMEOUT))
        .expect("a read timeout is set before the request goes out");
    let request = format!(
        "DELETE /admin/v2/persistent/public/default/{topic}?force=true HTTP/1.1\r\n\
         Host: {address}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .unwrap_or_else(|err| panic!("deleting {topic} could not be sent: {err}"));
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .unwrap_or_else(|err| panic!("deleting {topic} was not answered: {err}"));
    let status = response.lines().next().unwrap_or_default();
    let code = status.split_whitespace().nth(1).unwrap_or_default();
    assert!(
        code.starts_with('2') || code == "404",
        "deleting {topic} answered {status}",
    );
}
