//! Typed error surface for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-02 — top-level error enum is introduced here so every
//!   subsystem can return `shelfd::Result<T>` instead of
//!   `anyhow::Result<T>` (per agents/4-shelfd-builder.md Pass 2:
//!   "every error must have a path").
//! - SHELF-08 — each variant maps to a low-cardinality
//!   `{component, kind}` label pair on the `shelfd_error_total`
//!   Prometheus counter.
//!
//! The variants here are a scaffold; concrete error kinds will be
//! added incrementally by their owning ticket. The goal of this file
//! is to lock in the type without inviting `.unwrap()` elsewhere.

use thiserror::Error;

/// Top-level `shelfd` error.
///
/// Keep variants coarse-grained enough that the metric label set stays
/// bounded (≤ 32 distinct `{component, kind}` pairs across the binary).
#[derive(Debug, Error)]
pub enum Error {
    /// Config parse/validation failure (SHELF-02).
    #[error("config error: {0}")]
    Config(String),

    /// Network-layer error talking to S3 / Trino workers / peer pods.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Error coming back from the S3 origin client (SHELF-05).
    #[error("origin error: {0}")]
    Origin(String),

    /// Error from the Foyer-backed store (SHELF-03 / SHELF-17 / SHELF-18).
    #[error("store error: {0}")]
    Store(String),

    /// Routing failed — e.g. HRW returned no owner (SHELF-19 / SHELF-20).
    #[error("router error: {0}")]
    Router(String),

    /// Admission policy rejected the insert (SHELF-25). Not user-facing.
    #[error("admission rejected: {0}")]
    Admission(String),

    /// Membership resolver failed (DNS, /stats poll) (SHELF-20).
    #[error("membership error: {0}")]
    Membership(String),

    /// Catch-all for code paths that have not been tightened yet.
    /// Owning ticket must remove these before their PR merges.
    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
}

impl Error {
    /// Low-cardinality component label for `shelfd_error_total`.
    /// SHELF-08 wires this into the metric registry.
    pub fn component(&self) -> &'static str {
        match self {
            Error::Config(_) => "config",
            Error::Io(_) => "io",
            Error::Origin(_) => "origin",
            Error::Store(_) => "store",
            Error::Router(_) => "router",
            Error::Admission(_) => "admission",
            Error::Membership(_) => "membership",
            Error::Internal(_) => "internal",
        }
    }
}
