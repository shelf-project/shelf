//! S3 origin client for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-05 — `aws-sdk-s3` client with one pooled `HyperClient`,
//!   retry on 503, `x-amz-request-id` logging, IRSA-friendly credential
//!   chain. Exposes `get_range(bucket, key, offset, length) -> Bytes`
//!   as the only entry point in v0.1.
//! - Phase 3 (SHELF-3x) — per-prefix connection pools replace the
//!   single global pool.
//!
//! The module is a scaffold: construction paths are present so that
//! the HTTP handler can take an `Origin` via a trait object once
//! SHELF-06 lands.

use bytes::Bytes;
use std::fmt::Debug;

/// Origin for remote byte-range reads. Today: S3. Tomorrow (when we
/// add Spark / DuckDB): possibly an abstraction over multiple
/// object-store backends.
pub trait Origin: Send + Sync + Debug + 'static {
    fn get_range(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> impl std::future::Future<Output = crate::Result<Bytes>> + Send;

    fn head(
        &self,
        bucket: &str,
        key: &str,
    ) -> impl std::future::Future<Output = crate::Result<ObjectHead>> + Send;
}

/// Result of a `HEAD` request (SHELF-07).
#[derive(Debug, Clone)]
pub struct ObjectHead {
    pub content_length: u64,
    /// Raw ETag bytes. Note: the multipart ETag is NOT an MD5; the
    /// SHELF-04 doc-comment explains the invariant. Don't call this
    /// a cryptographic content hash.
    pub etag: Vec<u8>,
}

/// AWS SDK–backed `Origin`. Fields elided until SHELF-05.
#[derive(Debug)]
pub struct S3Origin {
    _private: (),
}

impl S3Origin {
    /// Build the singleton S3 client.
    pub async fn new(_config: &crate::config::OriginConfig) -> crate::Result<Self> {
        todo!(
            "SHELF-05: origin: construct aws-sdk-s3 client with pooled \
             HyperClient + retry-on-503 + IRSA credential chain; see \
             03-plan.md §4 SHELF-05"
        )
    }
}

impl Origin for S3Origin {
    async fn get_range(
        &self,
        _bucket: &str,
        _key: &str,
        _offset: u64,
        _length: u64,
    ) -> crate::Result<Bytes> {
        todo!(
            "SHELF-05: origin: implement GetObject with Range header + \
             request-id logging; see 03-plan.md §4 SHELF-05"
        )
    }

    async fn head(&self, _bucket: &str, _key: &str) -> crate::Result<ObjectHead> {
        todo!("SHELF-07: origin: implement HeadObject + small DRAM LRU cache of HEAD results; see 03-plan.md §4 SHELF-07")
    }
}
