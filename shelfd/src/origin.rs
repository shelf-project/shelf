//! S3 origin client for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-05 — `aws-sdk-s3` client, `get_range(bucket, key, offset,
//!   length)`, request-id logging, per-request timeout, IRSA-friendly
//!   default credential chain, MinIO-compatible path-style when
//!   `endpoint_url` is set.
//! - SHELF-07 — `head(bucket, key)` returns the `Content-Length` +
//!   raw ETag bytes the plugin needs to build a SHELF-04 key.
//! - Phase 3 (SHELF-3x) — per-prefix connection pools.
//!
//! The module is intentionally thin over the AWS SDK: the SDK's
//! default retry classifier already covers 503/Throttling, so we only
//! add a coarse per-request timeout and request-id logging here.

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, Region};
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::Client as S3Client;
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
    /// Raw ETag bytes, quotes included. **Not** a cryptographic hash —
    /// multipart S3 ETags are `md5(parts)-N`. The SHELF-04 key derives
    /// its content-addressed property from SHA-256 over the full
    /// tuple, not from this field.
    pub etag: Vec<u8>,
}

/// AWS SDK–backed `Origin`.
#[derive(Debug)]
pub struct S3Origin {
    client: S3Client,
    bucket: String,
    request_timeout: Duration,
}

impl S3Origin {
    /// Build the singleton S3 client.
    ///
    /// Resolution order for endpoint/region:
    /// 1. If `config.endpoint_url` is `Some`, use it verbatim + force
    ///    `path_style` (MinIO / LocalStack compatibility).
    /// 2. If `config.region` is `Some`, pin that region.
    /// 3. Otherwise fall back to the AWS SDK default provider chain
    ///    (`AWS_REGION`, IRSA, `~/.aws/config`, IMDS).
    pub async fn new(config: &crate::config::OriginConfig) -> crate::Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = config.region.clone() {
            loader = loader.region(Region::new(region));
        }
        let shared = loader.load().await;

        let mut s3_cfg_builder: S3ConfigBuilder = aws_sdk_s3::config::Builder::from(&shared);
        if let Some(endpoint) = config.endpoint_url.as_ref() {
            s3_cfg_builder = s3_cfg_builder.endpoint_url(endpoint).force_path_style(true);

            // MinIO in CI is seeded with static credentials; pick them
            // up via env if no SDK-resolved credentials are present.
            if shared.credentials_provider().is_none() {
                if let (Ok(ak), Ok(sk)) = (
                    std::env::var("AWS_ACCESS_KEY_ID"),
                    std::env::var("AWS_SECRET_ACCESS_KEY"),
                ) {
                    s3_cfg_builder = s3_cfg_builder.credentials_provider(Credentials::new(
                        ak,
                        sk,
                        None,
                        None,
                        "env-static",
                    ));
                }
            }
        }
        let client = S3Client::from_conf(s3_cfg_builder.build());

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            request_timeout: Duration::from_secs(30),
        })
    }

    /// Bucket the plugin targets by default. A future release may let
    /// the plugin override this per-request (needed once we cache
    /// cross-bucket Iceberg catalogs).
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

impl Origin for S3Origin {
    async fn get_range(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> crate::Result<Bytes> {
        if length == 0 {
            return Err(crate::Error::Origin("get_range: length must be > 0".into()));
        }
        // HTTP Range header uses INCLUSIVE end.
        let range = format!("bytes={}-{}", offset, offset + length - 1);
        let fut = async {
            let resp = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(range)
                .send()
                .await
                .map_err(|e| crate::Error::Origin(format!("GetObject {bucket}/{key}: {e}")))?;

            if let Some(rid) = resp.request_id() {
                tracing::debug!(request_id = rid, "s3 request-id");
            }

            let collected = resp
                .body
                .collect()
                .await
                .map_err(|e| crate::Error::Origin(format!("collect body: {e}")))?;
            Ok::<_, crate::Error>(collected.into_bytes())
        };

        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| {
                crate::Error::Origin(format!(
                    "GetObject {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                ))
            })?
    }

    async fn head(&self, bucket: &str, key: &str) -> crate::Result<ObjectHead> {
        let fut = async {
            let resp = self
                .client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| crate::Error::Origin(format!("HeadObject {bucket}/{key}: {e}")))?;
            let content_length = resp.content_length().unwrap_or_default().max(0) as u64;
            let etag = resp
                .e_tag()
                .map(|s| s.as_bytes().to_vec())
                .unwrap_or_default();
            Ok::<_, crate::Error>(ObjectHead {
                content_length,
                etag,
            })
        };
        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| {
                crate::Error::Origin(format!(
                    "HeadObject {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                ))
            })?
    }
}
