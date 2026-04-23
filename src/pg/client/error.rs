//! Database-layer error classification helpers.

use thiserror::Error;

/// Result type used by the asynchronous database layer.
pub(crate) type DbResult<T> = Result<T, DbError>;

/// Errors surfaced from the database layer with UI-relevant classification.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub(crate) enum DbError {
    /// The request exceeded the available refresh budget.
    #[error("The database request timed out before pgmon's refresh budget expired.")]
    Timeout,
    /// The request failed due to a retryable transport, pool, or query issue.
    #[error("{0}")]
    Transient(String),
    /// The request failed due to invalid configuration or a non-retryable guard.
    #[error("{0}")]
    Fatal(String),
    /// An optional capability required by the request is unavailable.
    #[error("{0}")]
    CapabilityMissing(&'static str),
}

impl DbError {
    /// Build a transient error with a formatted message.
    pub(crate) fn transient(message: impl Into<String>) -> Self {
        Self::Transient(message.into())
    }

    /// Build a fatal error with a formatted message.
    pub(crate) fn fatal(message: impl Into<String>) -> Self {
        Self::Fatal(message.into())
    }
}
