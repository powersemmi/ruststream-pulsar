//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the Apache Pulsar broker.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors so the public API does not leak the client's
/// error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PulsarError {
    /// Establishing the client connection failed.
    #[error("pulsar connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Creating a consumer failed.
    #[error("pulsar subscribe error on '{topic}': {source}")]
    Subscribe {
        /// The topic (or pattern) the subscription targeted.
        topic: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The consumer stream failed or ended permanently.
    #[error("pulsar receive error on '{topic}': {source}")]
    Receive {
        /// The topic (or pattern) of the subscription.
        topic: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Creating a producer or sending a message failed.
    #[error("pulsar publish error to '{topic}': {source}")]
    Publish {
        /// The topic the message targeted.
        topic: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("pulsar broker is not connected")]
    NotConnected,

    /// A topic name or subscription descriptor is invalid.
    #[error("invalid pulsar descriptor: {0}")]
    Invalid(String),
}

/// Boxes a client error into the crate's `Box<dyn StdError>` source form.
pub(crate) fn box_err<E>(err: E) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
{
    Box::new(err)
}
