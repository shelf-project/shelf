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
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::primitives::DateTimeFormat;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use std::fmt::Debug;
use tracing::{field, Instrument};

/// SHELF-23 — outcome of a conditional GET ([`Origin::get_range_conditional`]).
///
/// `NotModified` is the cheap path: the cached ETag still matches and
/// the caller can serve the local body without a transfer. `Modified`
/// carries the fresh bytes and the new ETag the caller should record;
/// the new ETag is `None` when origin does not return one (rare; some
/// MinIO error paths).
#[derive(Debug, Clone)]
pub enum ConditionalGet {
    NotModified,
    Modified { bytes: Bytes, etag: Option<String> },
}

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

    /// SHELF-23 — conditional `GET` against `if_none_match` (S3 ETag,
    /// quoted form expected — exactly what `HeadMeta::etag` stores).
    ///
    /// On a 304 from origin, returns [`ConditionalGet::NotModified`]
    /// (no body transfer) so the shim can serve from its local cache
    /// without a refetch. On a 200, returns [`ConditionalGet::Modified`]
    /// with the fresh bytes and the new ETag the caller should record
    /// to supersede its (now-stale) entry.
    ///
    /// Default impl falls through to an unconditional [`get_range`],
    /// which is correct (just expensive) for backends that don't model
    /// `If-None-Match`. The S3 implementation overrides this so the
    /// 304 fast-path actually saves a body round-trip.
    ///
    /// [`get_range`]: Origin::get_range
    fn get_range_conditional(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        _if_none_match: &str,
    ) -> impl std::future::Future<Output = crate::Result<ConditionalGet>> + Send {
        async move {
            let bytes = self.get_range(bucket, key, offset, length).await?;
            Ok(ConditionalGet::Modified { bytes, etag: None })
        }
    }

    /// `HEAD` the origin object. `Ok(None)` is the canonical signal
    /// for "object does not exist" (S3 404 / `NoSuchKey`); all other
    /// failures surface as `Err`.
    fn head(
        &self,
        bucket: &str,
        key: &str,
    ) -> impl std::future::Future<Output = crate::Result<Option<ObjectHead>>> + Send;

    /// SHELF-21 — single-shot `PUT`.
    ///
    /// Forwards the buffered body to S3 and returns the response
    /// `ETag` (without surrounding quotes) so the shim can echo it
    /// back to the client as `ETag:` — Trino's S3 filesystem reads
    /// that header to verify a successful write before recording the
    /// Iceberg manifest entry. Multipart uploads (POST `?uploads`,
    /// `?partNumber=`, `?uploadId=`) are out of scope for v1 and
    /// tracked under SHELF-21b.
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<&str>,
    ) -> impl std::future::Future<Output = crate::Result<PutObjectResult>> + Send;

    /// SHELF-21 — single-key `DELETE`.
    ///
    /// Returns `Ok(())` on both 204 NoContent and 404 NotFound — S3
    /// itself models DELETE as idempotent, and Iceberg's
    /// `RemoveOrphanFiles` relies on that semantic. All other errors
    /// surface as `Err`.
    fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> impl std::future::Future<Output = crate::Result<()>> + Send;

    /// SHELF-21b — multipart upload init.
    ///
    /// Returns the upstream `UploadId`. The shim threads it back to
    /// the caller in the `<UploadId>` slot of the
    /// `InitiateMultipartUploadResult` envelope so subsequent
    /// `UploadPart` / `CompleteMultipartUpload` calls work.
    fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> impl std::future::Future<Output = crate::Result<String>> + Send;

    /// SHELF-21b — upload a single part.
    ///
    /// Returns the part `ETag` (including AWS's surrounding quotes
    /// — the caller copies it byte-for-byte into the `ETag:`
    /// response header so the client's eventual
    /// `CompleteMultipartUpload` recomputes the same composite hash).
    ///
    /// SHELF-21c: takes a streaming `ByteStream` instead of a
    /// buffered `Bytes` so the shim can pipe the wire body straight
    /// through without the 256 MiB per-part buffer. `content_length`
    /// is mandatory — SigV4 needs it for the operation hash, and the
    /// HTTP-1.1 wire form requires it. `ByteStream` from a streaming
    /// body is non-replayable, so the SDK won't retry on transient
    /// failure; the shim's caller (Trino) retries at its level.
    fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: aws_sdk_s3::primitives::ByteStream,
        content_length: u64,
    ) -> impl std::future::Future<Output = crate::Result<String>> + Send;

    /// SHELF-21b — finalize an in-progress multipart upload.
    ///
    /// `parts` must be in ascending `part_number` order; AWS rejects
    /// out-of-order lists. The shim's XML parser preserves caller
    /// order verbatim — re-sorting would silently mask client bugs.
    fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> impl std::future::Future<Output = crate::Result<PutObjectResult>> + Send;

    /// SHELF-21b — abort an in-progress multipart upload.
    ///
    /// Idempotent: an abort on an unknown `upload_id` returns
    /// `Ok(())` (matches AWS, where the "no such upload" case is
    /// modelled as a 404 the SDK swallows for cleanup ergonomics).
    fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> impl std::future::Future<Output = crate::Result<()>> + Send;

    /// SHELF-21b — `ListObjectsV2`.
    ///
    /// All filter parameters mirror the AWS SDK signature 1:1; the
    /// shim parses the corresponding query params and forwards them
    /// verbatim. We deliberately do **not** stitch multiple SDK
    /// pages together — Iceberg's directory walk hands the
    /// `next_continuation_token` back to us on the next request, so
    /// pass-through pagination is correct and avoids unbounded
    /// buffering.
    fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: Option<i32>,
    ) -> impl std::future::Future<Output = crate::Result<ListObjectsV2Page>> + Send;

    /// SHELF-21b/c — bulk `DeleteObjects` (POST `/:bucket?delete`).
    ///
    /// SHELF-21c implementation: native `delete_objects()` SDK call
    /// per ≤1000-key chunk (the hard S3 cap). One round-trip per
    /// chunk replaces the SHELF-21b 32-way single-key fan-out;
    /// idempotent semantics are preserved (the SDK normalises 404s
    /// per-key in its `Errors` envelope, which we coerce to
    /// "deleted" in `BulkDeleteOutcome`). Larger requests are
    /// chunked transparently here so callers can pass any
    /// vector size.
    fn delete_objects_bulk(
        &self,
        bucket: &str,
        keys: Vec<String>,
    ) -> impl std::future::Future<Output = crate::Result<Vec<BulkDeleteOutcome>>> + Send;
}

/// SHELF-21 — what the shim needs back from a successful `PutObject`
/// to populate the response headers Trino's S3 filesystem expects.
#[derive(Debug, Clone, Default)]
pub struct PutObjectResult {
    /// `ETag` without surrounding quotes, e.g. `abc123-1` for a
    /// multipart-style ETag (we store as-received from the SDK and
    /// the caller adds quotes when emitting the HTTP header).
    pub etag: Option<String>,
    /// `x-amz-version-id`, when bucket versioning is enabled.
    pub version_id: Option<String>,
}

/// SHELF-21b — one part of a multipart upload, as supplied by the
/// client in the `CompleteMultipartUpload` body.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    pub part_number: i32,
    /// `ETag` of the part as returned by the prior `UploadPart`. The
    /// caller passes it through verbatim — including any quotes — so
    /// AWS-SDK's deserializer can match what S3 expects.
    pub etag: String,
}

/// SHELF-21b — one row of a `ListObjectsV2` response.
#[derive(Debug, Clone)]
pub struct ListedObject {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    /// `Last-Modified` already RFC-3339-formatted (matches the
    /// `ObjectHead::last_modified` shape in this module).
    pub last_modified: Option<String>,
}

/// SHELF-21b — the page-shaped result the shim renders to XML.
#[derive(Debug, Clone, Default)]
pub struct ListObjectsV2Page {
    pub contents: Vec<ListedObject>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub key_count: u32,
}

/// SHELF-21b — outcome of a single key from a bulk `DeleteObjects`
/// fan-out.
#[derive(Debug, Clone)]
pub struct BulkDeleteOutcome {
    pub key: String,
    /// `Some` on failure (`Code`, `Message`); `None` on success.
    pub error: Option<(String, String)>,
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
    /// RFC-3339-formatted `Last-Modified` timestamp as reported by S3,
    /// when present. Used only to decorate the HEAD response headers —
    /// the SHELF-04 key derivation does not depend on it.
    pub last_modified: Option<String>,
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

    /// SHELF-24: expose the configured S3 client so the pin-list
    /// loader can reuse the same credential chain, region, and
    /// `endpoint_url` override (MinIO/LocalStack) without rebuilding
    /// them from scratch.
    pub fn client(&self) -> &S3Client {
        &self.client
    }
}

/// Track B3 — report a completed origin request to Prometheus.
///
/// Separated into a helper so every error path surfaces the same
/// (bucket, op, outcome) cardinality and the histogram observation
/// can include latency even on failure. `bytes` is 0 for `head` and
/// for non-200 outcomes; the caller passes the body length on
/// success.
fn record_origin(
    bucket: &str,
    op: &'static str,
    outcome: &'static str,
    bytes: u64,
    elapsed_s: f64,
) {
    crate::metrics::ORIGIN_REQUEST_BYTES_TOTAL
        .with_label_values(&[bucket, op, outcome])
        .inc_by(bytes);
    crate::metrics::ORIGIN_REQUEST_SECONDS
        .with_label_values(&[bucket, op, outcome])
        .observe(elapsed_s);
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
        let start = std::time::Instant::now();
        // HTTP Range header uses INCLUSIVE end. Use checked
        // arithmetic so callers with `offset` near `u64::MAX` surface
        // a clean origin error instead of panicking in debug / wrapping
        // in release. In practice the HTTP handler and S3 shim cap
        // `offset + length` at `total_size`, but `Origin` is a public
        // trait and other or future call sites should not be able to
        // construct a malformed `Range` header via this path.
        let end = offset
            .checked_add(length)
            .and_then(|sum| sum.checked_sub(1))
            .ok_or_else(|| {
                crate::Error::Origin(format!(
                    "get_range: offset={offset} + length={length} overflows u64"
                ))
            })?;
        let range = format!("bytes={}-{}", offset, end);
        // SHELF-08: name the span so a Tempo trace resolves the
        // `http.get_cache → s3.get_object` parent/child pair cleanly.
        // `aws.request_id` is recorded on completion (Empty at open).
        let span = tracing::info_span!(
            "s3.get_object",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            range = %range,
            aws.request_id = field::Empty,
        );
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
                tracing::Span::current().record("aws.request_id", rid);
                tracing::debug!(request_id = rid, "s3 request-id");
            }

            let collected = resp
                .body
                .collect()
                .await
                .map_err(|e| crate::Error::Origin(format!("collect body: {e}")))?;
            Ok::<_, crate::Error>(collected.into_bytes())
        }
        .instrument(span);

        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(bytes)) => {
                record_origin(bucket, "get_range", "ok", bytes.len() as u64, elapsed);
                Ok(bytes)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "get_range", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "get_range", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "GetObject {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn get_range_conditional(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        if_none_match: &str,
    ) -> crate::Result<ConditionalGet> {
        if length == 0 {
            return Err(crate::Error::Origin(
                "get_range_conditional: length must be > 0".into(),
            ));
        }
        let start = std::time::Instant::now();
        let end = offset
            .checked_add(length)
            .and_then(|sum| sum.checked_sub(1))
            .ok_or_else(|| {
                crate::Error::Origin(format!(
                    "get_range_conditional: offset={offset} + length={length} overflows u64"
                ))
            })?;
        let range = format!("bytes={}-{}", offset, end);
        // SHELF-23 — `s3.get_object_conditional` so the
        // /metrics dashboard can split conditional vs. unconditional
        // GETs. The Tempo span name is parallel to `s3.get_object`.
        let span = tracing::info_span!(
            "s3.get_object_conditional",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            range = %range,
            if_none_match = %if_none_match,
            aws.request_id = field::Empty,
        );
        let fut = async {
            let resp = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(range)
                .if_none_match(if_none_match)
                .send()
                .await;
            match resp {
                Ok(resp) => {
                    if let Some(rid) = resp.request_id() {
                        tracing::Span::current().record("aws.request_id", rid);
                        tracing::debug!(request_id = rid, "s3 request-id");
                    }
                    let new_etag = resp.e_tag().map(|s| s.to_owned());
                    let collected = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| crate::Error::Origin(format!("collect body: {e}")))?;
                    Ok::<_, crate::Error>(ConditionalGet::Modified {
                        bytes: collected.into_bytes(),
                        etag: new_etag,
                    })
                }
                // S3 SDK surfaces 304 as a service error path. Detect
                // it via the HTTP status on the raw response so we
                // don't need to depend on a specific operation-error
                // variant — `PreconditionFailed` is for `If-Match`,
                // and there is no first-class `NotModified` variant
                // in `GetObjectError` today.
                Err(e) => {
                    let status = e.raw_response().map(|r| r.status().as_u16());
                    if status == Some(304) {
                        Ok::<_, crate::Error>(ConditionalGet::NotModified)
                    } else {
                        Err(crate::Error::Origin(format!(
                            "GetObject(if-none-match) {bucket}/{key}: {e}"
                        )))
                    }
                }
            }
        }
        .instrument(span);

        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(out)) => {
                let (outcome, bytes) = match &out {
                    ConditionalGet::NotModified => ("not_modified", 0),
                    ConditionalGet::Modified { bytes, .. } => ("ok", bytes.len() as u64),
                };
                record_origin(bucket, "get_range_conditional", outcome, bytes, elapsed);
                Ok(out)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "get_range_conditional", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "get_range_conditional", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "GetObject(if-none-match) {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<&str>,
    ) -> crate::Result<PutObjectResult> {
        let start = std::time::Instant::now();
        let len = body.len() as u64;
        let span = tracing::info_span!(
            "s3.put_object",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            content_length = len,
            aws.request_id = field::Empty,
        );
        let body_stream = aws_sdk_s3::primitives::ByteStream::from(body);
        let fut = async {
            let mut req = self
                .client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body_stream);
            if let Some(ct) = content_type {
                req = req.content_type(ct);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| crate::Error::Origin(format!("PutObject {bucket}/{key}: {e}")))?;
            if let Some(rid) = resp.request_id() {
                tracing::Span::current().record("aws.request_id", rid);
                tracing::debug!(request_id = rid, "s3 request-id");
            }
            Ok::<_, crate::Error>(PutObjectResult {
                etag: resp.e_tag().map(|s| s.trim_matches('"').to_owned()),
                version_id: resp.version_id().map(|s| s.to_owned()),
            })
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(out)) => {
                record_origin(bucket, "put_object", "ok", len, elapsed);
                Ok(out)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "put_object", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "put_object", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "PutObject {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> crate::Result<()> {
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "s3.delete_object",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            aws.request_id = field::Empty,
        );
        let fut = async {
            let resp = self
                .client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            match resp {
                Ok(r) => {
                    if let Some(rid) = r.request_id() {
                        tracing::Span::current().record("aws.request_id", rid);
                        tracing::debug!(request_id = rid, "s3 request-id");
                    }
                    Ok::<_, crate::Error>(())
                }
                Err(err) => {
                    // S3 DELETE is idempotent: 404 / NoSuchKey is a
                    // successful no-op. `RemoveOrphanFiles` and dbt
                    // post-write cleanups depend on this.
                    if err.raw_response().map(|r| r.status().as_u16()) == Some(404) {
                        return Ok(());
                    }
                    Err(crate::Error::Origin(format!(
                        "DeleteObject {bucket}/{key}: {err}"
                    )))
                }
            }
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(())) => {
                record_origin(bucket, "delete_object", "ok", 0, elapsed);
                Ok(())
            }
            Ok(Err(e)) => {
                record_origin(bucket, "delete_object", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "delete_object", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "DeleteObject {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> crate::Result<String> {
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "s3.create_multipart_upload",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            aws.request_id = field::Empty,
        );
        let fut = async {
            let mut req = self
                .client
                .create_multipart_upload()
                .bucket(bucket)
                .key(key);
            if let Some(ct) = content_type {
                req = req.content_type(ct);
            }
            let resp = req.send().await.map_err(|e| {
                crate::Error::Origin(format!("CreateMultipartUpload {bucket}/{key}: {e}"))
            })?;
            if let Some(rid) = resp.request_id() {
                tracing::Span::current().record("aws.request_id", rid);
            }
            resp.upload_id().map(|s| s.to_owned()).ok_or_else(|| {
                crate::Error::Origin(format!(
                    "CreateMultipartUpload {bucket}/{key}: missing upload_id in response"
                ))
            })
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(id)) => {
                record_origin(bucket, "create_multipart_upload", "ok", 0, elapsed);
                Ok(id)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "create_multipart_upload", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "create_multipart_upload", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "CreateMultipartUpload {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: aws_sdk_s3::primitives::ByteStream,
        content_length: u64,
    ) -> crate::Result<String> {
        let start = std::time::Instant::now();
        let len = content_length;
        let span = tracing::info_span!(
            "s3.upload_part",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            part_number,
            content_length = len,
            streaming = true,
            aws.request_id = field::Empty,
        );
        let fut = async {
            let resp = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .content_length(content_length as i64)
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    crate::Error::Origin(format!(
                        "UploadPart {bucket}/{key} part={part_number}: {e}"
                    ))
                })?;
            if let Some(rid) = resp.request_id() {
                tracing::Span::current().record("aws.request_id", rid);
            }
            resp.e_tag().map(|s| s.to_owned()).ok_or_else(|| {
                crate::Error::Origin(format!(
                    "UploadPart {bucket}/{key} part={part_number}: missing ETag in response"
                ))
            })
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(etag)) => {
                record_origin(bucket, "upload_part", "ok", len, elapsed);
                Ok(etag)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "upload_part", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "upload_part", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "UploadPart {bucket}/{key} part={part_number} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> crate::Result<PutObjectResult> {
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart as SdkPart};

        let start = std::time::Instant::now();
        let n_parts = parts.len();
        let span = tracing::info_span!(
            "s3.complete_multipart_upload",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            parts = n_parts,
            aws.request_id = field::Empty,
        );
        let sdk_parts: Vec<SdkPart> = parts
            .into_iter()
            .map(|p| {
                SdkPart::builder()
                    .part_number(p.part_number)
                    .e_tag(p.etag)
                    .build()
            })
            .collect();
        let multipart = CompletedMultipartUpload::builder()
            .set_parts(Some(sdk_parts))
            .build();
        let fut = async {
            let resp = self
                .client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .multipart_upload(multipart)
                .send()
                .await
                .map_err(|e| {
                    crate::Error::Origin(format!("CompleteMultipartUpload {bucket}/{key}: {e}"))
                })?;
            if let Some(rid) = resp.request_id() {
                tracing::Span::current().record("aws.request_id", rid);
            }
            Ok::<_, crate::Error>(PutObjectResult {
                etag: resp.e_tag().map(|s| s.trim_matches('"').to_owned()),
                version_id: resp.version_id().map(|s| s.to_owned()),
            })
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(out)) => {
                record_origin(bucket, "complete_multipart_upload", "ok", 0, elapsed);
                Ok(out)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "complete_multipart_upload", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "complete_multipart_upload", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "CompleteMultipartUpload {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> crate::Result<()> {
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "s3.abort_multipart_upload",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            aws.request_id = field::Empty,
        );
        let fut = async {
            let resp = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await;
            match resp {
                Ok(r) => {
                    if let Some(rid) = r.request_id() {
                        tracing::Span::current().record("aws.request_id", rid);
                    }
                    Ok::<_, crate::Error>(())
                }
                Err(err) => {
                    // S3 returns NoSuchUpload (404) when the upload
                    // id has already been aborted / completed —
                    // model as success so cleanup loops are safe to
                    // retry.
                    if err.raw_response().map(|r| r.status().as_u16()) == Some(404) {
                        return Ok(());
                    }
                    Err(crate::Error::Origin(format!(
                        "AbortMultipartUpload {bucket}/{key}: {err}"
                    )))
                }
            }
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(())) => {
                record_origin(bucket, "abort_multipart_upload", "ok", 0, elapsed);
                Ok(())
            }
            Ok(Err(e)) => {
                record_origin(bucket, "abort_multipart_upload", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "abort_multipart_upload", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "AbortMultipartUpload {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: Option<i32>,
    ) -> crate::Result<ListObjectsV2Page> {
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "s3.list_objects_v2",
            otel.kind = "client",
            bucket = %bucket,
            prefix = prefix.unwrap_or(""),
            aws.request_id = field::Empty,
        );
        let fut = async {
            let mut req = self.client.list_objects_v2().bucket(bucket);
            if let Some(p) = prefix {
                req = req.prefix(p);
            }
            if let Some(d) = delimiter {
                req = req.delimiter(d);
            }
            if let Some(t) = continuation_token {
                req = req.continuation_token(t);
            }
            if let Some(s) = start_after {
                req = req.start_after(s);
            }
            if let Some(m) = max_keys {
                req = req.max_keys(m);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| crate::Error::Origin(format!("ListObjectsV2 {bucket}: {e}")))?;
            if let Some(rid) = resp.request_id() {
                tracing::Span::current().record("aws.request_id", rid);
            }
            let contents: Vec<ListedObject> = resp
                .contents()
                .iter()
                .map(|o| ListedObject {
                    key: o.key().unwrap_or_default().to_owned(),
                    size: o.size().unwrap_or_default().max(0) as u64,
                    etag: o.e_tag().map(|s| s.to_owned()),
                    last_modified: o
                        .last_modified()
                        .and_then(|dt| dt.fmt(DateTimeFormat::DateTime).ok()),
                })
                .collect();
            let common_prefixes: Vec<String> = resp
                .common_prefixes()
                .iter()
                .filter_map(|p| p.prefix().map(|s| s.to_owned()))
                .collect();
            Ok::<_, crate::Error>(ListObjectsV2Page {
                key_count: resp.key_count().unwrap_or_default().max(0) as u32,
                is_truncated: resp.is_truncated().unwrap_or_default(),
                next_continuation_token: resp.next_continuation_token().map(|s| s.to_owned()),
                contents,
                common_prefixes,
            })
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(page)) => {
                record_origin(bucket, "list_objects_v2", "ok", 0, elapsed);
                Ok(page)
            }
            Ok(Err(e)) => {
                record_origin(bucket, "list_objects_v2", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "list_objects_v2", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "ListObjectsV2 {bucket} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }

    async fn delete_objects_bulk(
        &self,
        bucket: &str,
        keys: Vec<String>,
    ) -> crate::Result<Vec<BulkDeleteOutcome>> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};

        // S3's hard limit is 1000 keys per `DeleteObjects` request.
        // We chunk transparently so callers can pass any vector size
        // and don't have to know about the wire bound.
        const MAX_PER_CALL: usize = 1000;

        let start = std::time::Instant::now();
        let total = keys.len();
        let span = tracing::info_span!(
            "s3.delete_objects",
            otel.kind = "client",
            bucket = %bucket,
            total_keys = total,
            chunks = total.div_ceil(MAX_PER_CALL),
            aws.request_id = field::Empty,
        );

        let fut = async {
            let mut outcomes: Vec<BulkDeleteOutcome> = Vec::with_capacity(total);
            for chunk in keys.chunks(MAX_PER_CALL) {
                let identifiers: Vec<ObjectIdentifier> = chunk
                    .iter()
                    .map(|k| {
                        ObjectIdentifier::builder()
                            .key(k.clone())
                            .build()
                            .expect("ObjectIdentifier::build only fails when key is unset")
                    })
                    .collect();
                // `quiet(false)` so AWS returns the per-key Deleted
                // envelope; we expose it back to the caller via
                // `BulkDeleteOutcome { error: None }`.
                let delete = Delete::builder()
                    .set_objects(Some(identifiers))
                    .quiet(false)
                    .build()
                    .map_err(|e| crate::Error::Origin(format!("Delete::build {bucket}: {e}")))?;
                let resp = self
                    .client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| {
                        let detail = e
                            .as_service_error()
                            .map(|svc| {
                                format!(
                                    "code={:?} message={:?}",
                                    svc.meta().code(),
                                    svc.meta().message()
                                )
                            })
                            .unwrap_or_else(|| format!("{e:?}"));
                        crate::Error::Origin(format!("DeleteObjects {bucket}: {e} ({detail})"))
                    })?;
                if let Some(rid) = resp.request_id() {
                    tracing::Span::current().record("aws.request_id", rid);
                }

                // Map AWS's two-list response back to our flat
                // BulkDeleteOutcome list, preserving the caller's
                // input order so callers can zip outcomes back to
                // their original keys without index gymnastics.
                let mut by_key: std::collections::HashMap<String, Option<(String, String)>> =
                    std::collections::HashMap::with_capacity(chunk.len());
                for d in resp.deleted() {
                    if let Some(k) = d.key() {
                        by_key.insert(k.to_owned(), None);
                    }
                }
                for e in resp.errors() {
                    if let Some(k) = e.key() {
                        // S3 reports `NoSuchKey` for already-deleted
                        // objects when versioning is off; treat as
                        // success per the same idempotency contract
                        // single-key DELETE follows. Any other code
                        // bubbles up as a per-key error.
                        let code = e.code().unwrap_or("InternalError");
                        if code == "NoSuchKey" {
                            by_key.insert(k.to_owned(), None);
                        } else {
                            by_key.insert(
                                k.to_owned(),
                                Some((code.to_owned(), e.message().unwrap_or("").to_owned())),
                            );
                        }
                    }
                }

                for k in chunk {
                    // Defensive default: if AWS omitted a key from
                    // both `deleted` and `errors` (shouldn't happen,
                    // but the SDK doesn't enforce it), assume success
                    // — single-key DELETE is idempotent so this is
                    // the safer side of the asymmetry.
                    let error = by_key.remove(k).flatten();
                    outcomes.push(BulkDeleteOutcome {
                        key: k.clone(),
                        error,
                    });
                }
            }
            Ok::<_, crate::Error>(outcomes)
        }
        .instrument(span);

        // No outer timeout — `delete_objects` traffic shapes itself
        // by chunk count, and the SDK's per-request retry/timeout
        // policy already bounds wall-clock per chunk.
        let res = fut.await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(outcomes) => {
                let outcome_label = if outcomes.iter().all(|o| o.error.is_none()) {
                    "ok"
                } else {
                    "partial"
                };
                record_origin(bucket, "delete_objects", outcome_label, 0, elapsed);
                Ok(outcomes)
            }
            Err(e) => {
                record_origin(bucket, "delete_objects", "error", 0, elapsed);
                Err(e)
            }
        }
    }

    async fn head(&self, bucket: &str, key: &str) -> crate::Result<Option<ObjectHead>> {
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "s3.head_object",
            otel.kind = "client",
            bucket = %bucket,
            key = %key,
            aws.request_id = field::Empty,
        );
        let fut = async {
            let resp = match self
                .client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    // Classify 404 / NoSuchKey into `Ok(None)` without
                    // touching the SDK's internal response type, via
                    // the typed discriminator and the raw HTTP status
                    // as independent fallbacks.
                    if let SdkError::ServiceError(svc) = &err {
                        if matches!(svc.err(), HeadObjectError::NotFound(_)) {
                            return Ok(None);
                        }
                    }
                    if err.raw_response().map(|r| r.status().as_u16()) == Some(404) {
                        return Ok(None);
                    }
                    // Track D4 — treat **persistent** 403s as
                    // `Ok(None)` so the caller can negative-cache and
                    // stop hammering S3 about an object IAM forbids.
                    // Transient 403s — IRSA token rotation glitch,
                    // expired session, clock skew — must surface as
                    // `Err` so we don't poison the negative LRU with
                    // a 30s "doesn't exist" verdict for an object
                    // that's about to become readable on the next
                    // request. S3 differentiates these via the
                    // service error code; we discriminate here.
                    if err.raw_response().map(|r| r.status().as_u16()) == Some(403) {
                        let code = err.code().unwrap_or("");
                        if is_persistent_forbidden_code(code) {
                            return Ok(None);
                        }
                        // Fall through to `Err` for transient codes
                        // (or any 403 we don't recognise — fail-safe
                        // by retrying rather than negative-caching).
                    }
                    return Err(crate::Error::Origin(format!(
                        "HeadObject {bucket}/{key}: {err}"
                    )));
                }
            };

            if let Some(rid) = resp.request_id() {
                tracing::Span::current().record("aws.request_id", rid);
                tracing::debug!(request_id = rid, "s3 request-id");
            }

            let content_length = resp.content_length().unwrap_or_default().max(0) as u64;
            let etag = resp
                .e_tag()
                .map(|s| s.as_bytes().to_vec())
                .unwrap_or_default();
            let last_modified = resp
                .last_modified()
                .and_then(|dt| dt.fmt(DateTimeFormat::DateTime).ok());
            Ok::<_, crate::Error>(Some(ObjectHead {
                content_length,
                etag,
                last_modified,
            }))
        }
        .instrument(span);
        let res = tokio::time::timeout(self.request_timeout, fut).await;
        let elapsed = start.elapsed().as_secs_f64();
        match res {
            Ok(Ok(None)) => {
                record_origin(bucket, "head", "not_found", 0, elapsed);
                Ok(None)
            }
            Ok(Ok(Some(head))) => {
                record_origin(bucket, "head", "ok", head.content_length, elapsed);
                Ok(Some(head))
            }
            Ok(Err(e)) => {
                record_origin(bucket, "head", "error", 0, elapsed);
                Err(e)
            }
            Err(_) => {
                record_origin(bucket, "head", "timeout", 0, elapsed);
                Err(crate::Error::Origin(format!(
                    "HeadObject {bucket}/{key} timed out after {:?}",
                    self.request_timeout
                )))
            }
        }
    }
}

/// Classify an S3 HTTP-403 service-error `code` as a *persistent*
/// access denial (safe to cache as `Ok(None)`) versus a *transient*
/// auth blip (must surface as `Err` so we don't poison the negative
/// LRU). Anything we don't recognise is treated as transient: a 30 s
/// false-positive on a real `AccessDenied` is cheaper than a 30 s
/// outage on a recoverable `ExpiredToken`.
fn is_persistent_forbidden_code(code: &str) -> bool {
    matches!(code, "AccessDenied" | "Forbidden" | "AllAccessDisabled")
}

#[cfg(test)]
mod classify_tests {
    use super::is_persistent_forbidden_code;

    #[test]
    fn persistent_codes_are_cacheable() {
        assert!(is_persistent_forbidden_code("AccessDenied"));
        assert!(is_persistent_forbidden_code("Forbidden"));
        assert!(is_persistent_forbidden_code("AllAccessDisabled"));
    }

    #[test]
    fn transient_codes_must_surface_as_err() {
        for code in [
            "ExpiredToken",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "RequestTimeTooSkewed",
            "InvalidToken",
            "TokenRefreshRequired",
            "",
            "SomeNewCodeWeHaventSeen",
        ] {
            assert!(
                !is_persistent_forbidden_code(code),
                "{code} must NOT be treated as persistent"
            );
        }
    }
}
