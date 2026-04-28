//! S3-compatibility shim (SHELF-22 reads, SHELF-21 writes).
//!
//! This module serves a minimal subset of the S3 REST protocol on its
//! own listener (default `0.0.0.0:9092`) so generic clients — boto3,
//! DuckDB, Polars, `aws s3 cp` — and Trino's native S3 filesystem can
//! talk to Shelf without any AWS credentials and without the Trino
//! plugin.
//!
//! In scope (read path — SHELF-22):
//!
//! - `HEAD /:bucket/*key`  -> `HeadObject`
//! - `GET  /:bucket/*key`  -> `GetObject` (optionally `Range:
//!   bytes=<start>-<end>`)
//!
//! In scope (write path — SHELF-21):
//!
//! - `PUT    /:bucket/*key`  -> single-shot `PutObject` (≤ Trino's
//!   `s3.streaming.part-size` of 16 MiB by default; larger bodies
//!   stream as multipart, which is SHELF-21b territory).
//! - `DELETE /:bucket/*key`  -> idempotent `DeleteObject`.
//!
//! Both write verbs proxy through the existing
//! [`crate::origin::Origin`] client, then invalidate the affected
//! HEAD-LRU entry on success. The Foyer caches don't need explicit
//! eviction: SHELF-04 keys are content-addressed via ETag, so a
//! subsequent GET re-HEADs origin, observes the new ETag, and
//! derives a fresh content-addressed Foyer key — old entries
//! become unreachable orphans and age out via S3FIFO/LRU naturally.
//!
//! Explicitly **out of scope** for the v1 shim (see
//! `docs/design-notes/SHELF-21-shim-write-passthrough.md` for the
//! follow-up plan): SigV4 authentication, presigned URLs, multipart
//! uploads (POST `?uploads`, `?partNumber=`, `?uploadId=`),
//! `ListObjectsV2`, `DeleteObjects` bulk, virtual-hosted-style
//! addressing.
//!
//! Both verbs flow through [`crate::store::FoyerStore::get_or_fetch`]
//! so a shim read warms the same pool a native `/cache/...` read
//! would, and the HEAD-LRU ([`crate::head_lru::HeadLru`])
//! short-circuits repeated `HeadObject`s.
//!
//! Error responses use the S3 XML envelope (`<Error><Code>...`,
//! `<Message>...`) so clients that expect S3 parity (boto3's
//! `ClientError`, DuckDB's HTTPFS error surface) decode them as
//! normal. See [`s3_error_xml`].

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;

use crate::aws_chunked::{decode_aws_chunked, AwsChunkedError};
use crate::head_lru::HeadMeta;
use crate::http::ServerState;
use crate::origin::Origin;
use crate::store::{key_from_tuple, Pool, ReadOutcome};

mod xml;
use xml::{
    parse_complete_multipart_upload, parse_delete_objects, parse_delete_quiet,
    render_complete_multipart_upload, render_delete_result, render_initiate_multipart_upload,
    render_list_bucket_v2, ListBucketRequestEcho,
};

/// SHELF-21 — hard cap on a single-shot PUT body the shim is willing
/// to buffer in memory before forwarding to S3. Trino's native S3
/// filesystem sends single-shot PUTs only when the buffered chunk
/// is below `s3.streaming.part-size` (default 16 MiB); larger
/// writes are multipart. We still hold a generous ceiling so a
/// misconfigured client does not OOM the shim — and so the failure
/// surface (501 NotImplemented) tells the caller exactly why.
const SHIM_MAX_PUT_BYTES: usize = 256 * 1024 * 1024;

/// Build the shim router. Pure function, no I/O.
///
/// Keep the route shape path-style (`/:bucket/*key`) so swapping a
/// client's `endpoint_url` from real S3 to `http://shelfd:9092` is a
/// one-line change.
pub fn router(state: Arc<ServerState>) -> axum::Router {
    axum::Router::new()
        .route(
            "/:bucket/*key",
            get(handle_get_object)
                .head(handle_head_object)
                .put(dispatch_put)
                .delete(dispatch_delete)
                .post(dispatch_post_object),
        )
        .route(
            "/:bucket",
            get(handle_list_objects_v2).post(handle_bucket_post),
        )
        .with_state(state)
}

/// SHELF-21b — `PUT /:bucket/*key` dispatcher.
///
/// Trino's S3 client multiplexes single-shot PUTs and multipart
/// `UploadPart` calls onto the same path, distinguishing only by
/// the `partNumber` + `uploadId` query string. AWS itself does the
/// same on the wire, so honour the contract here.
async fn dispatch_put(
    state: State<Arc<ServerState>>,
    path: Path<(String, String)>,
    Query(qs): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match (qs.get("partNumber"), qs.get("uploadId")) {
        (Some(pn), Some(uid)) => {
            handle_upload_part(state, path, pn.clone(), uid.clone(), headers, body).await
        }
        // `partNumber` without `uploadId` is malformed per AWS; we
        // surface the same 400 InvalidArgument an SDK would.
        (Some(_), None) => error_response(
            StatusCode::BAD_REQUEST,
            s3_error_xml(
                "InvalidArgument",
                "partNumber requires a matching uploadId query parameter",
            ),
            None,
        ),
        _ => handle_put_object(state, path, headers, body).await,
    }
}

/// SHELF-21b — `DELETE /:bucket/*key` dispatcher.
async fn dispatch_delete(
    state: State<Arc<ServerState>>,
    path: Path<(String, String)>,
    Query(qs): Query<HashMap<String, String>>,
) -> Response {
    if let Some(uid) = qs.get("uploadId") {
        return handle_abort_multipart(state, path, uid.clone()).await;
    }
    handle_delete_object(state, path).await
}

/// SHELF-21b — `POST /:bucket/*key` dispatcher.
///
/// Two AWS verbs share this path:
/// * `?uploads` (no value) → InitiateMultipartUpload.
/// * `?uploadId=…` (body = `<CompleteMultipartUpload>` XML) →
///   CompleteMultipartUpload.
async fn dispatch_post_object(
    state: State<Arc<ServerState>>,
    path: Path<(String, String)>,
    Query(qs): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if qs.contains_key("uploads") {
        return handle_initiate_multipart(state, path, headers).await;
    }
    if let Some(uid) = qs.get("uploadId") {
        return handle_complete_multipart(state, path, uid.clone(), body).await;
    }
    error_response(
        StatusCode::BAD_REQUEST,
        s3_error_xml(
            "InvalidArgument",
            "POST /<bucket>/<key> requires either ?uploads or ?uploadId=...",
        ),
        None,
    )
}

/// Decide which Foyer pool a key belongs to.
///
/// Mirrors `ShelfFileSystem.poolFor` in `clients/trino` (Java half):
/// `.json`, `.avro`, and anything ending in `metadata.json` land in
/// metadata; everything else lands in row-group. The Java-side test
/// (`ShelfFileSystemTest.poolForUsesMetadataPoolForJsonAndAvro`)
/// pins the same invariant so a shim read and a native plugin read
/// of the same object share a cache entry.
pub(crate) fn pool_for(key: &str) -> Pool {
    let k = key.to_ascii_lowercase();
    if k.ends_with("metadata.json") || k.ends_with(".json") || k.ends_with(".avro") {
        Pool::Metadata
    } else {
        Pool::RowGroup
    }
}

/// SHELF-25 — does this PUT/UploadPart request advertise the AWS
/// SigV4 streaming chunked-transfer envelope?
///
/// AWS sets two headers in lock-step on a streaming-signed body, and
/// real S3 unwraps the envelope when it sees either signal:
///
/// - `Content-Encoding: aws-chunked` (may be a comma-separated list,
///   e.g. `aws-chunked,gzip`; case-insensitive).
/// - `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (or
///   any other `STREAMING-*` variant — `…-TRAILER`, `…-V4A`, etc.).
///
/// We honour either signal: an SDK that emits one but not the other
/// (rare in the wild but defensible per the SigV4 spec) still gets
/// correct decoding instead of corrupted bytes.
fn is_aws_chunked(headers: &HeaderMap) -> bool {
    if let Some(enc) = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        for token in enc.split(',') {
            if token.trim().eq_ignore_ascii_case("aws-chunked") {
                return true;
            }
        }
    }
    if let Some(sha) = headers
        .get(HeaderName::from_static("x-amz-content-sha256"))
        .and_then(|v| v.to_str().ok())
    {
        if sha.starts_with("STREAMING-") || sha.starts_with("streaming-") {
            return true;
        }
    }
    false
}

/// SHELF-25 — render a typed [`AwsChunkedError`] as an S3-shaped
/// 400 `InvalidRequest`. We use the same XML envelope as the rest
/// of the shim so SDK callers route the failure through their
/// normal `ClientError` path; the `<Message>` carries the typed
/// reason verbatim for log-grepability.
fn aws_chunked_decode_error(err: AwsChunkedError) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        s3_error_xml(
            "InvalidRequest",
            &format!("aws-chunked decode failed: {err}"),
        ),
        None,
    )
}

/// Track G-4 — extract a `schema.table` label from an Iceberg-on-S3
/// key. The example cdp layout writes everything under
/// `<bucket>/<schema>/<table>/{data,metadata}/...` so the segment
/// immediately preceding the literal `data/` or `metadata/` is the
/// table name, and the one before that is the schema.
///
/// Returns the sentinel `"other"` for keys that don't fit the
/// pattern (presigned junk, leftover `.alluxio_s3_api_metadata/`
/// uploads, dbt scratch paths). Returning a `'static` slice for
/// the sentinel keeps the hot-path allocation-free in the common
/// degenerate case; matched paths allocate one short `String`.
///
/// Cardinality is bounded by the actual table count in the cluster
/// (≤ ~500 in the prod cdp catalog as of 2026-04-27); see
/// `shelf::metrics::HITS_BY_TABLE_TOTAL` for the budget rationale.
pub(crate) fn table_label(key: &str) -> std::borrow::Cow<'static, str> {
    let segs: Vec<&str> = key.split('/').filter(|s| !s.is_empty()).collect();
    // Need at least `<schema>/<table>/<data|metadata>/<file>`.
    if segs.len() < 4 {
        return std::borrow::Cow::Borrowed("other");
    }
    // Walk segments looking for the boundary marker. We do a forward
    // scan rather than a fixed index because a few legacy tables in
    // the bronze layer write under an extra `<warehouse>/` prefix.
    for i in 1..segs.len() {
        if (segs[i] == "data" || segs[i] == "metadata") && i >= 2 {
            let schema = segs[i - 2];
            let table = segs[i - 1];
            // Defensive: reject obviously non-identifier segments so
            // a hostile key cannot inflate cardinality with random
            // hex/UUID strings.
            if is_identifier(schema) && is_identifier(table) {
                return std::borrow::Cow::Owned(format!("{schema}.{table}"));
            }
            return std::borrow::Cow::Borrowed("other");
        }
    }
    std::borrow::Cow::Borrowed("other")
}

fn is_identifier(s: &str) -> bool {
    if s.is_empty() || s.len() > 96 {
        return false;
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return false;
    }
    // Reject UUID / SHA-style blobs masquerading as a name. Pure
    // hex strings of ≥ 32 chars are overwhelmingly hashes (md5=32,
    // uuid_no_dashes=32, sha1=40, sha256=64) — real schema/table
    // identifiers are virtually never pure hex past 16 chars.
    if s.len() >= 32 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    true
}

/// Render the S3-style XML error envelope. Kept tiny on purpose —
/// S3 carries far more fields (`RequestId`, `HostId`, `Resource`),
/// but boto3 / DuckDB / Polars only need `Code` + `Message` to raise
/// a meaningful exception.
pub(crate) fn s3_error_xml(code: &str, message: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error><Code>{}</Code><Message>{}</Message></Error>",
        xml_escape(code),
        xml_escape(message),
    )
}

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// SHELF-21c — bridge an axum `Body` (which is `Send + !Sync` because
/// `axum_core::body::Body` wraps `UnsyncBoxBody`) into a body that
/// satisfies the `Send + Sync + 'static` bound on
/// `aws_smithy_types::byte_stream::ByteStream::from_body_1_x`.
///
/// The trick is identical to the one axum itself uses on the request
/// side (`Body::from_stream` wraps the stream in `SyncWrapper`):
/// `SyncWrapper<T>` is unconditionally `Sync` because it only ever
/// hands out `&mut T`, never `&T`. `Body::poll_frame` already takes
/// `Pin<&mut Self>`, so the lock-free `&mut`-only access pattern is
/// a perfect fit.
///
/// The wrapper is private — keep the lifetime ergonomics of the
/// shim's hot path inside this module. If a third call site ever
/// needs the same trick, promote to `crate::http_util` rather than
/// duplicating.
struct SyncBody {
    inner: sync_wrapper::SyncWrapper<Body>,
}

impl SyncBody {
    fn new(body: Body) -> Self {
        Self {
            inner: sync_wrapper::SyncWrapper::new(body),
        }
    }
}

impl http_body::Body for SyncBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // `Pin::get_mut` is sound: `SyncBody` does not enforce
        // structural pinning on its field — we re-pin the inner body
        // here. axum's `Body` is `Unpin` (`UnsyncBoxBody` boxes the
        // inner body), so this is the canonical pattern.
        let this = self.get_mut();
        std::pin::Pin::new(this.inner.get_mut()).poll_frame(cx)
    }

    fn size_hint(&self) -> http_body::SizeHint {
        // `SyncWrapper` only exposes `&mut self`. We don't need a
        // shared-ref accessor here; size hints are advisory and we
        // can return the unbounded default. Callers that need a
        // precise size pass `content_length` explicitly to the
        // `upload_part` builder, where the SDK uses it directly.
        http_body::SizeHint::default()
    }
}

/// SHELF-21c — extract a `Content-Length` from a request, with the
/// three states the streaming UploadPart path needs to discriminate:
/// * `Ok(Some(n))` — header present, parses as a non-negative `u64`.
/// * `Ok(None)`    — header absent. Caller decides whether the verb
///   tolerates a chunked body (UploadPart does not; bulk-delete does).
/// * `Err(())`     — header present but malformed. Caller emits 400.
///
/// We *do not* fall back to `Body::size_hint()` here. Kept-alive
/// HTTP/1.1 PUTs without `Content-Length` are normally chunked, and
/// silently switching the upstream SDK to chunked uploads when an
/// SDK-default Trino client always sends `Content-Length` would
/// hide a real client misconfiguration.
fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, ()> {
    let Some(raw) = headers.get(header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let s = raw.to_str().map_err(|_| ())?;
    let n: u64 = s.parse().map_err(|_| ())?;
    Ok(Some(n))
}

/// 16 hex chars derived from `nanos(now) XOR pid`. **Not** a
/// cryptographic nonce — `x-amz-request-id` only needs to be opaque
/// and reasonably unique per request, which this gives us without
/// pulling in `uuid` or `rand`.
fn request_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    format!("{:016x}", nanos ^ pid)
}

/// Parsed shape of a `Range:` header, pre-resolution.
///
/// We deliberately do *not* collapse these into `(offset, length)` at
/// parse time: `RangeFrom` and `Suffix` both need `total_size` from a
/// subsequent `HeadObject`, and the "closed" shape is the only one we
/// can resolve without it. The caller in `handle_get_object` does the
/// resolution after `head_meta` returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeSpec {
    /// `bytes=<start>-<end>` (both inclusive per RFC 9110).
    Closed { start: u64, end: u64 },
    /// `bytes=<start>-` — read from `start` to end of object.
    /// Used by streaming readers that don't yet know the object size.
    From { start: u64 },
    /// `bytes=-<n>` — read the *last* `n` bytes of the object.
    /// This is the Parquet / Avro footer-read shape and is what
    /// Trino's native S3 client uses via `readTail(n)`.
    Suffix { last: u64 },
}

/// Parse `Range: bytes=<spec>` per RFC 9110 §14.1.
///
/// Returns:
/// - `Ok(None)` — no `Range` header at all
/// - `Ok(Some(spec))` — a single-range spec the caller resolves
///   after `HeadObject`
/// - `Err(())` — malformed header (caller returns 416 `InvalidRange`)
fn parse_range_header(headers: &HeaderMap) -> Result<Option<RangeSpec>, ()> {
    let Some(raw) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let s = raw.to_str().map_err(|_| ())?;
    let rest = s.strip_prefix("bytes=").ok_or(())?;
    // Multi-range (`bytes=0-10,20-30`) — S3 itself rejects it for
    // GetObject, so refusing parity is an implementation convenience
    // rather than a deviation.
    if rest.contains(',') {
        return Err(());
    }
    let (start, end) = rest.split_once('-').ok_or(())?;
    match (start.is_empty(), end.is_empty()) {
        // `bytes=-<n>` — suffix read (Parquet/Avro footer shape).
        // RFC 9110: zero-length suffix is malformed.
        (true, false) => {
            let last: u64 = end.parse().map_err(|_| ())?;
            if last == 0 {
                return Err(());
            }
            Ok(Some(RangeSpec::Suffix { last }))
        }
        // `bytes=<start>-` — open-ended read to end-of-object.
        (false, true) => {
            let start: u64 = start.parse().map_err(|_| ())?;
            Ok(Some(RangeSpec::From { start }))
        }
        // `bytes=<start>-<end>` — closed range.
        (false, false) => {
            let start: u64 = start.parse().map_err(|_| ())?;
            let end: u64 = end.parse().map_err(|_| ())?;
            if end < start {
                return Err(());
            }
            Ok(Some(RangeSpec::Closed { start, end }))
        }
        // `bytes=-` — malformed.
        (true, true) => Err(()),
    }
}

/// Resolve a `RangeSpec` against the object's total size into the
/// canonical `(offset, length)` the cache layer keys on. Returns
/// `None` when the range is unsatisfiable (caller returns 416).
fn resolve_range(spec: RangeSpec, total_size: u64) -> Option<(u64, u64)> {
    match spec {
        RangeSpec::Closed { start, end } => {
            if total_size == 0 || start >= total_size {
                return None;
            }
            // RFC 9110: the client may ask for past-end; we clamp.
            let last = end.min(total_size - 1);
            Some((start, last - start + 1))
        }
        RangeSpec::From { start } => {
            if total_size == 0 || start >= total_size {
                return None;
            }
            Some((start, total_size - start))
        }
        RangeSpec::Suffix { last } => {
            if total_size == 0 {
                return None;
            }
            // RFC 9110 §14.1.2: if suffix > total_size, return the
            // whole object. S3 matches this behaviour.
            let length = last.min(total_size);
            let offset = total_size - length;
            Some((offset, length))
        }
    }
}

/// Convert an RFC 3339 UTC timestamp (the shape produced by
/// `aws_sdk_s3::primitives::DateTimeFormat::DateTime`) into the
/// RFC 1123 `Last-Modified` form clients expect. Returns `None` if
/// the SDK re-parse fails — the caller omits the header in that
/// case rather than returning a lie.
fn rfc3339_to_rfc1123(s: &str) -> Option<String> {
    use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
    let dt = DateTime::from_str(s, DateTimeFormat::DateTime).ok()?;
    dt.fmt(DateTimeFormat::HttpDate).ok()
}

/// Consult the HEAD-LRU; fall through to `origin.head` on miss and
/// backfill the LRU.
///
/// Returns:
/// * `Ok(Some(meta))` — hit or miss+origin-ok
/// * `Ok(None)`       — origin 404 (`NoSuchKey`)
/// * `Err(resp)`      — transport / SDK error, already converted to
///   an S3-shaped XML error the caller can bubble out.
async fn head_meta(
    state: &Arc<ServerState>,
    bucket: &str,
    key: &str,
) -> Result<Option<HeadMeta>, Response> {
    if let Some(meta) = state.head_lru.get(bucket, key) {
        return Ok(Some((*meta).clone()));
    }
    // Track D4 — short-TTL negative cache. If we've confirmed within
    // the TTL that this object is a 404, return `Ok(None)` without
    // hitting S3. Iceberg delete-file speculation, puffin stats
    // probes, and dbt dry-runs all generate floods of HEADs against
    // keys we know are absent.
    if state.head_lru.is_known_missing(bucket, key) {
        crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
            .with_label_values(&["head_object", "negative_hit"])
            .inc_by(0);
        return Ok(None);
    }
    match state.origin.as_ref().head(bucket, key).await {
        Ok(Some(head)) => {
            let meta: HeadMeta = head.into();
            state
                .head_lru
                .insert(bucket.to_owned(), key.to_owned(), meta.clone());
            // Drop any stale negative entry — the object now exists.
            state.head_lru.forget_missing(bucket, key);
            Ok(Some(meta))
        }
        Ok(None) => {
            state.head_lru.record_missing(bucket, key);
            Ok(None)
        }
        Err(e) => Err(s3_internal_error("origin.head", &e.to_string())),
    }
}

/// `HEAD /:bucket/*key` — S3 `HeadObject`.
pub async fn handle_head_object(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let start = std::time::Instant::now();
    let (response, outcome) = match head_meta(&state, &bucket, &key).await {
        Ok(Some(meta)) => {
            let mut headers = HeaderMap::new();
            stamp_common_headers(&mut headers, &meta);
            // Track B3 — HEAD responses carry no body, but we still
            // count them so the byte-efficiency dashboard sees the
            // non-zero head-heavy workloads that Iceberg metadata
            // walks produce during planning.
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["head_object", "ok"])
                .inc_by(0);
            ((StatusCode::OK, headers).into_response(), "ok")
        }
        Ok(None) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["head_object", "not_found"])
                .inc_by(0);
            (no_such_key(&bucket, &key), "not_found")
        }
        Err(resp) => (resp, "error"),
    };
    // Track A1 / SHELF-G1 — observe shim HEAD latency. Without this
    // the production p95 dashboard stays empty because the native
    // /cache plane (which does observe) is unused under the current
    // s3.endpoint cutover topology.
    state
        .metrics
        .request_seconds
        .with_label_values(&["/s3/head_object", outcome])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// `GET /:bucket/*key` — S3 `GetObject` (honours `Range:` if set).
pub async fn handle_get_object(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    // Track A1 / SHELF-G1 — single timer for the whole GET so the
    // observation in `record_get_latency` covers HEAD-LRU lookup,
    // origin HEAD (if needed), single-flight wait, and Foyer get.
    let start = std::time::Instant::now();
    let range_spec = match parse_range_header(&headers) {
        Ok(r) => r,
        Err(()) => {
            // No size context yet → `bytes */0` is still a valid
            // `Content-Range` per RFC 9110.
            return record_get_latency(&state, start, "invalid_range", invalid_range(0));
        }
    };

    let meta = match head_meta(&state, &bucket, &key).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            return record_get_latency(&state, start, "not_found", no_such_key(&bucket, &key));
        }
        Err(resp) => return record_get_latency(&state, start, "error", resp),
    };
    let total_size = meta.content_length;

    let (offset, length, is_partial) = match range_spec {
        Some(spec) => match resolve_range(spec, total_size) {
            Some((offset, length)) => (offset, length, true),
            None => {
                return record_get_latency(
                    &state,
                    start,
                    "invalid_range",
                    invalid_range(total_size),
                );
            }
        },
        None => {
            let cap = state
                .s3_shim_max_full_object_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            if total_size > cap {
                return record_get_latency(
                    &state,
                    start,
                    "oversized",
                    not_implemented_oversized(total_size, cap),
                );
            }
            if total_size == 0 {
                let mut headers = HeaderMap::new();
                stamp_common_headers(&mut headers, &meta);
                return record_get_latency(
                    &state,
                    start,
                    "empty",
                    (StatusCode::OK, headers, Vec::<u8>::new()).into_response(),
                );
            }
            (0u64, total_size, false)
        }
    };

    // Content-addressed key: mirror the native read-path derivation
    // so shim + plugin reads collide on the same slot. Borrow the
    // etag bytes directly into `key_from_tuple`; allocating a `Vec`
    // here once per GET is ~30 B for the etag string + alloc-header
    // overhead and shows up as measurable allocator pressure under
    // sustained scan load (see plan B4).
    let etag_bytes: &[u8] = meta.etag.as_deref().map(str::as_bytes).unwrap_or_default();
    let key_obj = match key_from_tuple(etag_bytes, offset, length, 0) {
        Ok(k) => k,
        Err(e) => {
            return record_get_latency(
                &state,
                start,
                "error",
                s3_internal_error("key.derive", &e.to_string()),
            );
        }
    };

    let pool = pool_for(&key);
    let origin = state.origin.clone();
    let bucket_for_fetch = bucket.clone();
    let key_for_fetch = key.clone();
    let fetcher = async move {
        origin
            .as_ref()
            .get_range(&bucket_for_fetch, &key_for_fetch, offset, length)
            .await
    };

    let outcome = state
        .store
        .get_or_fetch(pool, key_obj, state.admission.as_ref(), fetcher)
        .await;

    // Bookkeeping parity with the native `/cache/...` data plane: a
    // shim read that hits Foyer is a cache hit and must bump
    // `shelf_hits_total{pool=...}`; a miss bumps `shelf_misses_total`.
    // Without this, operators watching the dashboard after an
    // `s3.endpoint` swap would see a flat 0-hit line and assume the
    // cache is broken. The data-plane path in `http.rs` does the
    // same dance against the same counters.
    let pool_label = match pool {
        Pool::Metadata => "metadata",
        Pool::RowGroup => "rowgroup",
    };
    // Track G-4 — derive the `schema.table` label once per request so
    // both the hit and miss arms can attribute traffic. Resolution
    // happens here (rather than inside each arm) so a future
    // ReadOutcome variant cannot accidentally skip the bump.
    let table_label_cow = table_label(&key);
    let table_label = table_label_cow.as_ref();
    let (bytes, shim_outcome) = match outcome {
        Ok(ReadOutcome::Hit(b, tier)) => {
            state
                .metrics
                .hits_total
                .with_label_values(&[pool_label])
                .inc();
            crate::metrics::HITS_BY_TABLE_TOTAL
                .with_label_values(&[pool_label, table_label])
                .inc();
            // Track A1 / SHELF-G1 — split DRAM vs NVMe hits so
            // `shelf_request_seconds{outcome=hit_memory|hit_disk}`
            // and the byte-efficiency dashboard can graph them
            // separately. The previous "hit" lump-sum hid the
            // case where DRAM has been overrun and every "hit"
            // is paying NVMe latency.
            (b, tier.outcome_label())
        }
        Ok(ReadOutcome::Miss(b)) => {
            state
                .metrics
                .misses_total
                .with_label_values(&[pool_label])
                .inc();
            crate::metrics::MISSES_BY_TABLE_TOTAL
                .with_label_values(&[pool_label, table_label])
                .inc();
            (b, "miss")
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["get_object", "error"])
                .inc_by(0);
            return record_get_latency(
                &state,
                start,
                "error",
                s3_internal_error("origin/store", &e.to_string()),
            );
        }
    };
    crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
        .with_label_values(&["get_object", shim_outcome])
        .inc_by(bytes.len() as u64);

    let mut headers = HeaderMap::new();
    stamp_common_headers(&mut headers, &meta);
    // Override `Content-Length` to the sliced length; `stamp_common`
    // reports the full-object size, which is wrong for a 206.
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("u64 is ASCII"),
    );
    let status = if is_partial {
        // `resolve_range` already caps `offset + length <= total_size`,
        // so this add cannot overflow in practice. Use saturating
        // arithmetic anyway so a future resolver bug yields a
        // malformed-but-finite header rather than a panic.
        let last = offset.saturating_add(length).saturating_sub(1);
        let cr = format!("bytes {}-{}/{}", offset, last, total_size);
        if let Ok(v) = HeaderValue::from_str(&cr) {
            headers.insert(header::CONTENT_RANGE, v);
        }
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    record_get_latency(
        &state,
        start,
        shim_outcome,
        (status, headers, bytes).into_response(),
    )
}

/// `PUT /:bucket/*key` — S3 `PutObject` (single-shot only — see
/// SHELF-21b for multipart).
///
/// Buffers the request body up to [`SHIM_MAX_PUT_BYTES`], forwards to
/// origin, and on 2xx invalidates the HEAD-LRU entry for the key so
/// subsequent reads through this shim see the fresh ETag. The Foyer
/// caches are deliberately **not** evicted — SHELF-04 keys are
/// content-addressed via ETag, so old entries become unreachable
/// orphans the moment the HEAD-LRU drops the stale tuple.
pub async fn handle_put_object(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let start = std::time::Instant::now();

    // Buffer the body. v1 of the shim is single-shot only; >256 MiB
    // single PUTs are vanishingly rare in Trino's S3 filesystem (it
    // uses multipart above ~16 MiB), and bounding the buffer here
    // keeps a misbehaving client from OOM-ing the daemon.
    let wire_bytes = match axum::body::to_bytes(body, SHIM_MAX_PUT_BYTES).await {
        Ok(b) => b,
        Err(err) => {
            return record_put_latency(
                &state,
                start,
                "oversized",
                error_response(
                    StatusCode::NOT_IMPLEMENTED,
                    s3_error_xml(
                        "EntityTooLarge",
                        &format!(
                            "Single-shot PUT capped at {} bytes; multipart upload \
                             support is tracked under SHELF-21b. Upstream error: {err}",
                            SHIM_MAX_PUT_BYTES
                        ),
                    ),
                    None,
                ),
            );
        }
    };

    // SHELF-25 — if the caller advertised AWS SigV4 streaming chunked
    // transfer encoding, strip the envelope before forwarding. Real S3
    // does this transparently; the shim re-uploads via the SDK's
    // regular PutObject (which signs the body bytes we hand it as-is),
    // so without this decode the chunk-size hex + `chunk-signature=…`
    // lines get persisted into S3 verbatim — see RCA H4 in
    // `docs/rollout-v1/rca-stage0bc.md`.
    let bytes = if is_aws_chunked(&headers) {
        match decode_aws_chunked(&wire_bytes) {
            Ok(b) => b,
            Err(err) => {
                return record_put_latency(
                    &state,
                    start,
                    "aws_chunked_decode_error",
                    aws_chunked_decode_error(err),
                );
            }
        }
    } else {
        wire_bytes
    };

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let result = state
        .origin
        .as_ref()
        .put_object(&bucket, &key, bytes.clone(), content_type.as_deref())
        .await;

    // SHELF-25 — emit the *decoded* size in the metric. Wire size
    // (`wire_bytes.len()`) was misleading for streaming-signed bodies
    // because it included the envelope overhead.
    let bytes_len = bytes.len() as u64;
    let response = match result {
        Ok(out) => {
            // SHELF-21 invalidation contract: drop the stale positive
            // HEAD-LRU entry, and clear any prior 404 negative cache
            // since the key now demonstrably exists. See
            // `head_lru::HeadLru::invalidate` for the rationale on
            // why we don't touch Foyer here.
            state.head_lru.invalidate(&bucket, &key);
            state.head_lru.forget_missing(&bucket, &key);

            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["put_object", "ok"])
                .inc_by(bytes_len);

            let mut hdrs = HeaderMap::new();
            if let Some(etag) = out.etag.as_deref() {
                let quoted = format!("\"{}\"", etag);
                if let Ok(v) = HeaderValue::from_str(&quoted) {
                    hdrs.insert(header::ETAG, v);
                }
            }
            if let Some(vid) = out.version_id.as_deref() {
                if let Ok(v) = HeaderValue::from_str(vid) {
                    hdrs.insert(HeaderName::from_static("x-amz-version-id"), v);
                }
            }
            if let Ok(v) = HeaderValue::from_str(&request_id()) {
                hdrs.insert(HeaderName::from_static("x-amz-request-id"), v);
            }
            (StatusCode::OK, hdrs).into_response()
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["put_object", "error"])
                .inc_by(0);
            s3_internal_error("origin.put_object", &e.to_string())
        }
    };

    let outcome = if response.status().is_success() {
        "ok"
    } else {
        "error"
    };
    record_put_latency(&state, start, outcome, response)
}

/// `DELETE /:bucket/*key` — S3 `DeleteObject` (idempotent).
///
/// Forwards to origin; on success records a short-TTL negative HEAD
/// entry so subsequent HEAD/GET requests short-circuit to 404
/// without round-tripping S3, and drops any stale positive entry as
/// a side effect of `record_missing`. Returns 204 NoContent on
/// success — same shape as real S3 — so Trino's S3 filesystem and
/// Iceberg's `RemoveOrphanFiles` see byte-identical behaviour.
pub async fn handle_delete_object(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let start = std::time::Instant::now();

    let response = match state.origin.as_ref().delete_object(&bucket, &key).await {
        Ok(()) => {
            // `record_missing` already calls `cache.remove(...)` so
            // we get the positive-entry drop for free.
            state.head_lru.record_missing(&bucket, &key);

            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["delete_object", "ok"])
                .inc_by(0);

            let mut hdrs = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&request_id()) {
                hdrs.insert(HeaderName::from_static("x-amz-request-id"), v);
            }
            (StatusCode::NO_CONTENT, hdrs).into_response()
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["delete_object", "error"])
                .inc_by(0);
            s3_internal_error("origin.delete_object", &e.to_string())
        }
    };

    let outcome = if response.status().is_success() {
        "ok"
    } else {
        "error"
    };
    state
        .metrics
        .request_seconds
        .with_label_values(&["/s3/delete_object", outcome])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// SHELF-21b — `POST /:bucket/*key?uploads`.
///
/// Pure passthrough: we don't allocate any cache state for in-progress
/// multipart uploads (the upstream S3 API is the source of truth for
/// `UploadId` lifecycle), so all we do is forward and template the
/// `InitiateMultipartUploadResult` envelope back.
async fn handle_initiate_multipart(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let start = std::time::Instant::now();
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let result = state
        .origin
        .as_ref()
        .create_multipart_upload(&bucket, &key, content_type)
        .await;
    let response = match result {
        Ok(upload_id) => {
            let body = render_initiate_multipart_upload(&bucket, &key, &upload_id);
            xml_ok(StatusCode::OK, body)
        }
        Err(e) => s3_internal_error("origin.create_multipart_upload", &e.to_string()),
    };
    record_path_latency(&state, start, "/s3/create_multipart_upload", &response);
    response
}

/// SHELF-21c — hard cap (5 GiB) on a single `UploadPart` body, matching
/// AWS's published per-part ceiling. The shim streams the body straight
/// through to S3 without buffering, so this cap exists only to reject
/// requests that would themselves be rejected by S3 — we surface the
/// 501 ourselves rather than waste a round trip on a doomed PUT.
const SHIM_MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// SHELF-25 — buffered+decoded `UploadPart` for `aws-chunked` bodies.
///
/// Split out from [`handle_upload_part`] because the streaming-into-
/// `ByteStream::from_body_1_x` fast-path is incompatible with chunked
/// decoding: the AWS SDK signs whatever bytes we hand it, so the
/// envelope has to be unwrapped *before* the SDK call. The cap is
/// the same 256 MiB ceiling as single-shot PUTs — Trino's default
/// part-size is 16 MiB. Anything bigger returns 501 with a clear
/// `Use STREAMING-UNSIGNED-PAYLOAD-TRAILER or non-streaming SigV4`
/// hint so the operator knows the workaround.
async fn handle_upload_part_aws_chunked(
    state: Arc<ServerState>,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Body,
) -> Response {
    let wire_bytes = match axum::body::to_bytes(body, SHIM_MAX_PUT_BYTES).await {
        Ok(b) => b,
        Err(err) => {
            return error_response(
                StatusCode::NOT_IMPLEMENTED,
                s3_error_xml(
                    "EntityTooLarge",
                    &format!(
                        "aws-chunked UploadPart capped at {} bytes (the shim has \
                         to buffer the whole part to strip the chunk envelope \
                         before re-uploading); for parts above that ceiling, \
                         disable streaming-signed payloads on the client (e.g. \
                         set `s3.payload-signing.enabled=false` on the Trino \
                         catalog or `payload_signing_enabled = false` on the \
                         AWS SDK request). Upstream error: {err}",
                        SHIM_MAX_PUT_BYTES
                    ),
                ),
                None,
            );
        }
    };
    let decoded = match decode_aws_chunked(&wire_bytes) {
        Ok(b) => b,
        Err(err) => return aws_chunked_decode_error(err),
    };
    let decoded_len = decoded.len() as u64;
    if decoded_len > SHIM_MAX_PART_BYTES {
        return error_response(
            StatusCode::NOT_IMPLEMENTED,
            s3_error_xml(
                "EntityTooLarge",
                &format!(
                    "decoded part body is {decoded_len} bytes; AWS caps \
                     UploadPart at {SHIM_MAX_PART_BYTES} bytes per part"
                ),
            ),
            None,
        );
    }
    let body_stream = aws_sdk_s3::primitives::ByteStream::from(decoded);
    let result = state
        .origin
        .as_ref()
        .upload_part(
            bucket,
            key,
            upload_id,
            part_number,
            body_stream,
            decoded_len,
        )
        .await;
    match result {
        Ok(etag) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["upload_part", "ok"])
                .inc_by(decoded_len);
            let mut hdrs = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&etag) {
                hdrs.insert(header::ETAG, v);
            }
            if let Ok(v) = HeaderValue::from_str(&request_id()) {
                hdrs.insert(HeaderName::from_static("x-amz-request-id"), v);
            }
            (StatusCode::OK, hdrs).into_response()
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["upload_part", "error"])
                .inc_by(0);
            s3_internal_error("origin.upload_part", &e.to_string())
        }
    }
}

/// SHELF-21b/c — `PUT /:bucket/*key?partNumber=N&uploadId=...`.
///
/// `partNumber` must parse to `1..=10_000` (S3's bound). SHELF-21c
/// streams the wire body directly into the AWS SDK via
/// [`ByteStream::from_body_1_x`] — no per-part buffering. We
/// require a `Content-Length` header (Trino's S3 client always
/// sends one) since SigV4 needs it for the operation hash.
///
/// SHELF-25 — `aws-chunked` parts take a separate buffered path
/// (see [`handle_upload_part_aws_chunked`]) since the streaming
/// fast-path is incompatible with envelope decoding.
async fn handle_upload_part(
    state: State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    part_number_str: String,
    upload_id: String,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let State(state) = state;
    let start = std::time::Instant::now();
    let part_number: i32 = match part_number_str.parse() {
        Ok(n) if (1..=10_000).contains(&n) => n,
        _ => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml(
                    "InvalidArgument",
                    &format!(
                        "partNumber must be an integer in [1, 10000], got {part_number_str:?}"
                    ),
                ),
                None,
            );
            record_path_latency(&state, start, "/s3/upload_part", &resp);
            return resp;
        }
    };

    // SHELF-25 — `aws-chunked` parts must be buffered + decoded before
    // we hand the bytes to the SDK; we cannot stream-decode in place
    // because the SDK does its own SigV4 signing over whatever bytes
    // we pass and the chunk-signature framing has to be off the wire
    // first. Cap at the same 256 MiB ceiling we use for single-shot
    // PUTs — Trino's part-size is 16 MiB by default so this is
    // generous; an oversized chunked part returns 501 with a clear
    // pointer to non-streaming SigV4 as the workaround.
    if is_aws_chunked(&headers) {
        let response = handle_upload_part_aws_chunked(
            state.clone(),
            &bucket,
            &key,
            &upload_id,
            part_number,
            body,
        )
        .await;
        record_path_latency(&state, start, "/s3/upload_part", &response);
        return response;
    }

    // SigV4 needs the body length up front; HTTP-1.1 requires it on
    // an unsigned-payload PUT. Trino's S3 client sets it; if we ever
    // see a client that doesn't, fail loud rather than silently
    // chunked-encoding (which the SDK might do at some indeterminate
    // payload boundary).
    let content_length = match parse_content_length(&headers) {
        Ok(Some(n)) => n,
        Ok(None) => {
            let resp = error_response(
                StatusCode::LENGTH_REQUIRED,
                s3_error_xml(
                    "MissingContentLength",
                    "UploadPart requires a Content-Length header",
                ),
                None,
            );
            record_path_latency(&state, start, "/s3/upload_part", &resp);
            return resp;
        }
        Err(()) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml("InvalidArgument", "Malformed Content-Length header"),
                None,
            );
            record_path_latency(&state, start, "/s3/upload_part", &resp);
            return resp;
        }
    };
    if content_length > SHIM_MAX_PART_BYTES {
        let resp = error_response(
            StatusCode::NOT_IMPLEMENTED,
            s3_error_xml(
                "EntityTooLarge",
                &format!(
                    "UploadPart capped at {SHIM_MAX_PART_BYTES} bytes per part \
                     (S3's hard limit); got Content-Length: {content_length}"
                ),
            ),
            None,
        );
        record_path_latency(&state, start, "/s3/upload_part", &resp);
        return resp;
    }
    // Stream the wire body straight through to the SDK — no
    // intermediate buffer. `ByteStream::from_body_1_x` requires
    // `Send + Sync`; axum's `Body` is `!Sync`, so we wrap it in
    // `SyncBody` (above) which uses `sync_wrapper::SyncWrapper` to
    // upgrade `&mut`-only access to `Sync`.
    let body_stream = aws_sdk_s3::primitives::ByteStream::from_body_1_x(SyncBody::new(body));
    let bytes_len = content_length;
    let result = state
        .origin
        .as_ref()
        .upload_part(
            &bucket,
            &key,
            &upload_id,
            part_number,
            body_stream,
            content_length,
        )
        .await;
    let response = match result {
        Ok(etag) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["upload_part", "ok"])
                .inc_by(bytes_len);
            let mut hdrs = HeaderMap::new();
            // ETag from origin already comes back quoted; pass through.
            if let Ok(v) = HeaderValue::from_str(&etag) {
                hdrs.insert(header::ETAG, v);
            }
            if let Ok(v) = HeaderValue::from_str(&request_id()) {
                hdrs.insert(HeaderName::from_static("x-amz-request-id"), v);
            }
            (StatusCode::OK, hdrs).into_response()
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["upload_part", "error"])
                .inc_by(0);
            s3_internal_error("origin.upload_part", &e.to_string())
        }
    };
    record_path_latency(&state, start, "/s3/upload_part", &response);
    response
}

/// SHELF-21b — `POST /:bucket/*key?uploadId=...` (CompleteMultipartUpload).
///
/// On success we run the same HEAD-LRU invalidation as a single-shot
/// PUT — the new object's ETag is composite (e.g. `xxx-N`), so the
/// SHELF-04 content-addressed key for any future GET will derive
/// fresh and naturally miss any stale Foyer entry.
async fn handle_complete_multipart(
    state: State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    upload_id: String,
    body: Body,
) -> Response {
    const MAX_COMPLETE_BODY: usize = 1024 * 1024; // S3 caps at 1000 parts → ~70 KB
    let State(state) = state;
    let start = std::time::Instant::now();
    let bytes = match axum::body::to_bytes(body, MAX_COMPLETE_BODY).await {
        Ok(b) => b,
        Err(err) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml(
                    "MalformedXML",
                    &format!(
                        "CompleteMultipartUpload body too large (cap {MAX_COMPLETE_BODY} bytes): {err}"
                    ),
                ),
                None,
            );
            record_path_latency(&state, start, "/s3/complete_multipart_upload", &resp);
            return resp;
        }
    };
    let body_str = match std::str::from_utf8(&bytes) {
        Ok(s) => s,
        Err(_) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml("MalformedXML", "CompleteMultipartUpload body must be UTF-8"),
                None,
            );
            record_path_latency(&state, start, "/s3/complete_multipart_upload", &resp);
            return resp;
        }
    };
    let parts = match parse_complete_multipart_upload(body_str) {
        Ok(p) => p,
        Err(e) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml("MalformedXML", &e),
                None,
            );
            record_path_latency(&state, start, "/s3/complete_multipart_upload", &resp);
            return resp;
        }
    };
    let result = state
        .origin
        .as_ref()
        .complete_multipart_upload(&bucket, &key, &upload_id, parts)
        .await;
    let response = match result {
        Ok(out) => {
            state.head_lru.invalidate(&bucket, &key);
            state.head_lru.forget_missing(&bucket, &key);
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["complete_multipart_upload", "ok"])
                .inc_by(0);
            let xml = render_complete_multipart_upload(&bucket, &key, out.etag.as_deref(), None);
            let mut resp = xml_ok(StatusCode::OK, xml);
            if let Some(vid) = out.version_id.as_deref() {
                if let Ok(v) = HeaderValue::from_str(vid) {
                    resp.headers_mut()
                        .insert(HeaderName::from_static("x-amz-version-id"), v);
                }
            }
            resp
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["complete_multipart_upload", "error"])
                .inc_by(0);
            s3_internal_error("origin.complete_multipart_upload", &e.to_string())
        }
    };
    record_path_latency(&state, start, "/s3/complete_multipart_upload", &response);
    response
}

/// SHELF-21b — `DELETE /:bucket/*key?uploadId=...` (AbortMultipartUpload).
///
/// No cache state to clear — the bytes never made it into the
/// HEAD-LRU since the multipart upload was never finalised. Idempotent
/// by virtue of `Origin::abort_multipart_upload` mapping 404 to
/// `Ok(())`.
async fn handle_abort_multipart(
    state: State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    upload_id: String,
) -> Response {
    let State(state) = state;
    let start = std::time::Instant::now();
    let result = state
        .origin
        .as_ref()
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .await;
    let response = match result {
        Ok(()) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["abort_multipart_upload", "ok"])
                .inc_by(0);
            let mut hdrs = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&request_id()) {
                hdrs.insert(HeaderName::from_static("x-amz-request-id"), v);
            }
            (StatusCode::NO_CONTENT, hdrs).into_response()
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["abort_multipart_upload", "error"])
                .inc_by(0);
            s3_internal_error("origin.abort_multipart_upload", &e.to_string())
        }
    };
    record_path_latency(&state, start, "/s3/abort_multipart_upload", &response);
    response
}

/// SHELF-21b — `GET /:bucket?list-type=2&...` (ListObjectsV2).
///
/// We're strict about `list-type=2` because the v1 protocol uses a
/// completely different parameter set (markers, no continuation
/// tokens). Trino + Iceberg only call v2; rejecting v1 keeps us
/// from silently shipping the wrong `<Marker>` shape.
async fn handle_list_objects_v2(
    State(state): State<Arc<ServerState>>,
    Path(bucket): Path<String>,
    Query(qs): Query<HashMap<String, String>>,
) -> Response {
    let start = std::time::Instant::now();
    if qs.get("list-type").map(String::as_str) != Some("2") {
        let resp = error_response(
            StatusCode::NOT_IMPLEMENTED,
            s3_error_xml(
                "NotImplemented",
                "Only ListObjectsV2 is supported (list-type=2). v1 ListObjects will be added if a real consumer needs it.",
            ),
            None,
        );
        record_path_latency(&state, start, "/s3/list_objects_v2", &resp);
        return resp;
    }
    let prefix = qs.get("prefix").map(String::as_str);
    let delimiter = qs.get("delimiter").map(String::as_str);
    let continuation_token = qs.get("continuation-token").map(String::as_str);
    let start_after = qs.get("start-after").map(String::as_str);
    let max_keys: Option<i32> = qs
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .filter(|n: &i32| (1..=1000).contains(n));
    let result = state
        .origin
        .as_ref()
        .list_objects_v2(
            &bucket,
            prefix,
            delimiter,
            continuation_token,
            start_after,
            max_keys,
        )
        .await;
    let response = match result {
        Ok(page) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["list_objects_v2", "ok"])
                .inc_by(page.contents.len() as u64);
            let req_echo = ListBucketRequestEcho {
                prefix,
                delimiter,
                continuation_token,
                start_after,
                max_keys: max_keys.unwrap_or(1000),
            };
            let xml = render_list_bucket_v2(&bucket, &req_echo, &page);
            xml_ok(StatusCode::OK, xml)
        }
        Err(e) => {
            crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
                .with_label_values(&["list_objects_v2", "error"])
                .inc_by(0);
            s3_internal_error("origin.list_objects_v2", &e.to_string())
        }
    };
    record_path_latency(&state, start, "/s3/list_objects_v2", &response);
    response
}

/// SHELF-21b — `POST /:bucket?delete` (bulk DeleteObjects).
///
/// AWS caps a single bulk delete at 1000 keys; we enforce the same
/// upstream bound so callers get the same `MalformedXML` shape they
/// would from S3 itself. The handler dispatches per-key
/// invalidations on the HEAD-LRU only for keys that *successfully*
/// deleted — partial failures don't lie about cache state.
async fn handle_bucket_post(
    State(state): State<Arc<ServerState>>,
    Path(bucket): Path<String>,
    Query(qs): Query<HashMap<String, String>>,
    body: Body,
) -> Response {
    let start = std::time::Instant::now();
    if !qs.contains_key("delete") {
        let resp = error_response(
            StatusCode::BAD_REQUEST,
            s3_error_xml(
                "InvalidArgument",
                "POST /<bucket> requires ?delete (bulk DeleteObjects)",
            ),
            None,
        );
        record_path_latency(&state, start, "/s3/delete_objects", &resp);
        return resp;
    }
    const MAX_DELETE_BODY: usize = 1024 * 1024; // 1000 keys × 1 KB margin
    let bytes = match axum::body::to_bytes(body, MAX_DELETE_BODY).await {
        Ok(b) => b,
        Err(err) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml(
                    "MalformedXML",
                    &format!("Delete body too large (cap {MAX_DELETE_BODY} bytes): {err}"),
                ),
                None,
            );
            record_path_latency(&state, start, "/s3/delete_objects", &resp);
            return resp;
        }
    };
    let body_str = match std::str::from_utf8(&bytes) {
        Ok(s) => s,
        Err(_) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml("MalformedXML", "Delete body must be UTF-8"),
                None,
            );
            record_path_latency(&state, start, "/s3/delete_objects", &resp);
            return resp;
        }
    };
    let quiet = parse_delete_quiet(body_str);
    let keys = match parse_delete_objects(body_str) {
        Ok(k) => k,
        Err(e) => {
            let resp = error_response(
                StatusCode::BAD_REQUEST,
                s3_error_xml("MalformedXML", &e),
                None,
            );
            record_path_latency(&state, start, "/s3/delete_objects", &resp);
            return resp;
        }
    };
    if keys.len() > 1000 {
        let resp = error_response(
            StatusCode::BAD_REQUEST,
            s3_error_xml(
                "MalformedXML",
                &format!("Delete request capped at 1000 keys, got {}", keys.len()),
            ),
            None,
        );
        record_path_latency(&state, start, "/s3/delete_objects", &resp);
        return resp;
    }
    let outcomes = match state
        .origin
        .as_ref()
        .delete_objects_bulk(&bucket, keys)
        .await
    {
        Ok(o) => o,
        Err(e) => {
            let resp = s3_internal_error("origin.delete_objects_bulk", &e.to_string());
            record_path_latency(&state, start, "/s3/delete_objects", &resp);
            return resp;
        }
    };
    for o in &outcomes {
        if o.error.is_none() {
            state.head_lru.record_missing(&bucket, &o.key);
        }
    }
    crate::metrics::S3_SHIM_RESPONSE_BYTES_TOTAL
        .with_label_values(&[
            "delete_objects",
            if outcomes.iter().all(|o| o.error.is_none()) {
                "ok"
            } else {
                "partial"
            },
        ])
        .inc_by(outcomes.iter().filter(|o| o.error.is_none()).count() as u64);
    let xml = render_delete_result(&outcomes, quiet);
    let response = xml_ok(StatusCode::OK, xml);
    record_path_latency(&state, start, "/s3/delete_objects", &response);
    response
}

/// Build a 200/204 response with `Content-Type: application/xml`,
/// `Content-Length`, and an `x-amz-request-id` header. Centralised so
/// every SHELF-21b handler emits identically-shaped success bodies.
fn xml_ok(status: StatusCode, body: String) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    if let Ok(v) = HeaderValue::from_str(&body.len().to_string()) {
        headers.insert(header::CONTENT_LENGTH, v);
    }
    if let Ok(v) = HeaderValue::from_str(&request_id()) {
        headers.insert(HeaderName::from_static("x-amz-request-id"), v);
    }
    (status, headers, body).into_response()
}

/// SHELF-21b — generic latency observer used by every multipart /
/// list / bulk-delete handler. Mirrors `record_put_latency` /
/// `record_get_latency` but takes the `path` label by reference so
/// we don't need a helper per verb. Outcome is derived from the
/// already-built `Response` so every exit point reports the same
/// way.
fn record_path_latency(
    state: &Arc<ServerState>,
    start: std::time::Instant,
    path: &'static str,
    response: &Response,
) {
    let outcome: &'static str = if response.status().is_success() {
        "ok"
    } else if response.status().is_client_error() {
        "client_error"
    } else {
        "error"
    };
    state
        .metrics
        .request_seconds
        .with_label_values(&[path, outcome])
        .observe(start.elapsed().as_secs_f64());
}

/// SHELF-21 — sibling of `record_get_latency` for the PUT path so
/// every `return` in `handle_put_object` ends up with the same
/// `path` label (`/s3/put_object`).
fn record_put_latency(
    state: &Arc<ServerState>,
    start: std::time::Instant,
    outcome: &'static str,
    response: Response,
) -> Response {
    state
        .metrics
        .request_seconds
        .with_label_values(&["/s3/put_object", outcome])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// Track A1 / SHELF-G1 — observe shim GET latency once per request,
/// regardless of which exit path the handler took. Pulled out as a
/// helper so every `return` site uses the same `path` label
/// (`/s3/get_object`) and the same `outcome` cardinality
/// (`hit_memory`, `hit_disk`, `miss`, `not_found`, `invalid_range`,
/// `oversized`, `empty`, `error`). Histograms are append-only on
/// the receiving Prometheus, so adding labels later is cheap;
/// removing them is an alert-rule rewrite.
fn record_get_latency(
    state: &Arc<ServerState>,
    start: std::time::Instant,
    outcome: &'static str,
    response: Response,
) -> Response {
    state
        .metrics
        .request_seconds
        .with_label_values(&["/s3/get_object", outcome])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// Common response decoration for both HEAD and GET 200/206.
///
/// `Content-Length` is seeded with the full-object size; ranged GETs
/// overwrite it after the fact with the sliced length.
fn stamp_common_headers(headers: &mut HeaderMap, meta: &HeadMeta) {
    if let Ok(v) = HeaderValue::from_str(&meta.content_length.to_string()) {
        headers.insert(header::CONTENT_LENGTH, v);
    }
    if let Some(etag) = meta.etag.as_deref() {
        if let Ok(v) = HeaderValue::from_str(etag) {
            headers.insert(header::ETAG, v);
        }
    }
    if let Some(lm) = meta.last_modified.as_deref() {
        if let Some(formatted) = rfc3339_to_rfc1123(lm) {
            if let Ok(v) = HeaderValue::from_str(&formatted) {
                headers.insert(header::LAST_MODIFIED, v);
            }
        }
    }
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(v) = HeaderValue::from_str(&request_id()) {
        headers.insert(HeaderName::from_static("x-amz-request-id"), v);
    }
}

fn no_such_key(bucket: &str, key: &str) -> Response {
    let body = s3_error_xml(
        "NoSuchKey",
        &format!("The specified key does not exist: {bucket}/{key}"),
    );
    error_response(StatusCode::NOT_FOUND, body, None)
}

fn invalid_range(total_size: u64) -> Response {
    let body = s3_error_xml(
        "InvalidRange",
        "The requested range is not satisfiable for this object.",
    );
    let content_range = format!("bytes */{}", total_size);
    error_response(StatusCode::RANGE_NOT_SATISFIABLE, body, Some(content_range))
}

fn not_implemented_oversized(size: u64, cap: u64) -> Response {
    let body = s3_error_xml(
        "NotImplemented",
        &format!(
            "Unbounded GetObject is capped at {cap} bytes on this shim; \
             requested object is {size} bytes. Issue a ranged read instead."
        ),
    );
    error_response(StatusCode::NOT_IMPLEMENTED, body, None)
}

fn s3_internal_error(kind: &str, detail: &str) -> Response {
    tracing::warn!(kind, detail, "s3_shim upstream error");
    let body = s3_error_xml("InternalError", &format!("{kind}: {detail}"));
    error_response(StatusCode::BAD_GATEWAY, body, None)
}

fn error_response(status: StatusCode, body: String, content_range: Option<String>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    if let Ok(v) = HeaderValue::from_str(&body.len().to_string()) {
        headers.insert(header::CONTENT_LENGTH, v);
    }
    if let Some(cr) = content_range {
        if let Ok(v) = HeaderValue::from_str(&cr) {
            headers.insert(header::CONTENT_RANGE, v);
        }
    }
    if let Ok(v) = HeaderValue::from_str(&request_id()) {
        headers.insert(HeaderName::from_static("x-amz-request-id"), v);
    }
    (status, headers, body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_for_routes_by_extension() {
        // The four cases spelled out in SHELF-22 + one "unknown"
        // sanity check. If the Java poolFor drifts, the shared test
        // in ShelfFileSystemTest will fail in lockstep with this one.
        assert_eq!(pool_for("tbl/rg-0.parquet"), Pool::RowGroup);
        assert_eq!(pool_for("tbl/metadata/v1.metadata.json"), Pool::Metadata);
        assert_eq!(pool_for("tbl/snap-42.avro"), Pool::Metadata);
        assert_eq!(pool_for("tbl/manifest-0.json"), Pool::Metadata);
        assert_eq!(pool_for("tbl/blob.bin"), Pool::RowGroup);
    }

    #[test]
    fn pool_for_is_case_insensitive_like_java_side() {
        // ShelfFileSystem.poolFor lower-cases before matching; we do
        // the same so an uppercased `.JSON` still lands in metadata.
        assert_eq!(pool_for("tbl/MANIFEST.JSON"), Pool::Metadata);
        assert_eq!(pool_for("tbl/Snap-7.AVRO"), Pool::Metadata);
    }

    // ---- Track G-4 — table_label parser ----

    #[test]
    fn table_label_parses_iceberg_data_path() {
        // Layout: <bucket>/<schema>/<table>/data/<partition>/<file>.parquet
        // The shim sees `key = "<schema>/<table>/data/..."` because
        // the bucket is the path-style first segment routed by axum.
        let lbl = table_label("cdp/icesheet/data/00000-0-abc.parquet");
        assert_eq!(lbl.as_ref(), "cdp.icesheet");
    }

    #[test]
    fn table_label_parses_iceberg_metadata_path() {
        let lbl = table_label("curiousjr_bq/bronze_page_open/metadata/00001-uuid.metadata.json");
        assert_eq!(lbl.as_ref(), "curiousjr_bq.bronze_page_open");
    }

    #[test]
    fn table_label_falls_back_to_other_for_alluxio_uploads() {
        // Legacy artefacts from the alluxio-proxy era should not
        // inflate cardinality; they map to the sentinel.
        let lbl = table_label(".alluxio_s3_api_metadata/uploads/foo");
        assert_eq!(lbl.as_ref(), "other");
    }

    #[test]
    fn table_label_falls_back_for_short_keys() {
        assert_eq!(table_label("foo.parquet").as_ref(), "other");
        assert_eq!(table_label("data/foo.parquet").as_ref(), "other");
    }

    #[test]
    fn table_label_rejects_non_identifier_segments() {
        // A 64-char hex blob in the schema slot is an attacker /
        // bug signal, not a real table — fold to `other`.
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let key = format!("{hex}/whatever/data/x.parquet");
        assert_eq!(table_label(&key).as_ref(), "other");
    }

    #[test]
    fn table_label_handles_leading_slash() {
        // Some clients (`aws s3 cp s3://bucket/key`) double the slash;
        // the parser must treat empty leading segments as no-ops.
        let lbl = table_label("/cdp/icesheet/data/x.parquet");
        assert_eq!(lbl.as_ref(), "cdp.icesheet");
    }

    #[test]
    fn s3_error_xml_has_code_and_message() {
        let body = s3_error_xml("NoSuchKey", "not here");
        // The spec calls for `<Code>([^<]+)</Code>` /
        // `<Message>([^<]+)</Message>`; a targeted substring
        // extractor is behaviourally identical and keeps the
        // dependency floor where it is (no new `regex` dep).
        let code = extract_between(&body, "<Code>", "</Code>").expect("Code tag");
        let msg = extract_between(&body, "<Message>", "</Message>").expect("Message tag");
        assert_eq!(code, "NoSuchKey");
        assert_eq!(msg, "not here");
        assert!(body.starts_with("<?xml"));
        assert!(body.contains("<Error>"));
    }

    #[test]
    fn s3_error_xml_escapes_angle_brackets_in_message() {
        let body = s3_error_xml("InvalidArgument", "payload was <root/>");
        assert!(body.contains("&lt;root/&gt;"));
        // Raw sequence must not leak through; XML-parsing clients
        // would mis-tokenise the envelope if it did.
        assert!(!body.contains("<root/>"));
    }

    #[test]
    fn request_id_is_16_hex_chars() {
        let id = request_id();
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ---- Range parsing (SHELF-22 Trino wiring) ----
    //
    // Trino's `S3Input.readTail(n)` (`io.trino.filesystem.s3`) is the
    // pivotal client: it issues `Range: bytes=-<n>` suffix reads for
    // Parquet + Avro footers. The earlier parser treated that as
    // malformed and responded 416, which broke every Iceberg query
    // the moment we pointed `s3.endpoint` at the shim. These tests
    // pin the RFC-9110 shapes that unblock the Trino read path.

    fn range_headers(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::RANGE, HeaderValue::from_str(v).unwrap());
        h
    }

    #[test]
    fn parse_range_closed() {
        let h = range_headers("bytes=0-99");
        assert_eq!(
            parse_range_header(&h).unwrap(),
            Some(RangeSpec::Closed { start: 0, end: 99 })
        );
    }

    #[test]
    fn parse_range_open_ended() {
        let h = range_headers("bytes=1024-");
        assert_eq!(
            parse_range_header(&h).unwrap(),
            Some(RangeSpec::From { start: 1024 })
        );
    }

    #[test]
    fn parse_range_suffix_used_by_trino_readtail() {
        // `S3Input.readTail(8)` → `bytes=-8`. Critical path for
        // Parquet footer magic-number checks and Avro footer sync
        // markers.
        let h = range_headers("bytes=-8");
        assert_eq!(
            parse_range_header(&h).unwrap(),
            Some(RangeSpec::Suffix { last: 8 })
        );
    }

    #[test]
    fn parse_range_malformed_cases() {
        assert!(parse_range_header(&range_headers("bytes=")).is_err());
        assert!(parse_range_header(&range_headers("bytes=-")).is_err());
        assert!(parse_range_header(&range_headers("bytes=-0")).is_err());
        assert!(parse_range_header(&range_headers("bytes=10-5")).is_err());
        assert!(parse_range_header(&range_headers("bytes=abc-10")).is_err());
        // Multi-range: S3 rejects these for GetObject; we match.
        assert!(parse_range_header(&range_headers("bytes=0-10,20-30")).is_err());
        // Missing unit prefix.
        assert!(parse_range_header(&range_headers("0-10")).is_err());
    }

    #[test]
    fn parse_range_absent_when_no_header() {
        assert_eq!(parse_range_header(&HeaderMap::new()).unwrap(), None);
    }

    #[test]
    fn resolve_range_closed_basic() {
        let spec = RangeSpec::Closed { start: 0, end: 99 };
        assert_eq!(resolve_range(spec, 1_000), Some((0, 100)));
    }

    #[test]
    fn resolve_range_closed_clamps_past_end() {
        // RFC 9110: past-end is not unsatisfiable as long as start is
        // within the object; we clamp the end to total-1.
        let spec = RangeSpec::Closed {
            start: 900,
            end: 10_000,
        };
        assert_eq!(resolve_range(spec, 1_000), Some((900, 100)));
    }

    #[test]
    fn resolve_range_rejects_start_at_or_past_end() {
        let spec = RangeSpec::Closed {
            start: 1_000,
            end: 2_000,
        };
        assert_eq!(resolve_range(spec, 1_000), None);
    }

    #[test]
    fn resolve_range_suffix_within_object() {
        // Typical Parquet footer: last 8 bytes of a 5 MiB file.
        let spec = RangeSpec::Suffix { last: 8 };
        assert_eq!(
            resolve_range(spec, 5 * 1024 * 1024),
            Some((5 * 1024 * 1024 - 8, 8))
        );
    }

    #[test]
    fn resolve_range_suffix_larger_than_object_returns_full_object() {
        // RFC 9110 §14.1.2: `bytes=-N` where N > size returns the
        // whole object, not an error. S3 behaves the same.
        let spec = RangeSpec::Suffix { last: 10_000 };
        assert_eq!(resolve_range(spec, 1_000), Some((0, 1_000)));
    }

    #[test]
    fn resolve_range_from_returns_tail() {
        let spec = RangeSpec::From { start: 750 };
        assert_eq!(resolve_range(spec, 1_000), Some((750, 250)));
    }

    #[test]
    fn resolve_range_from_past_end_is_unsatisfiable() {
        let spec = RangeSpec::From { start: 1_000 };
        assert_eq!(resolve_range(spec, 1_000), None);
    }

    #[test]
    fn resolve_range_on_empty_object_is_always_unsatisfiable() {
        assert_eq!(
            resolve_range(RangeSpec::Closed { start: 0, end: 0 }, 0),
            None
        );
        assert_eq!(resolve_range(RangeSpec::From { start: 0 }, 0), None);
        assert_eq!(resolve_range(RangeSpec::Suffix { last: 8 }, 0), None);
    }

    fn extract_between<'a>(haystack: &'a str, open: &str, close: &str) -> Option<&'a str> {
        let s = haystack.find(open)? + open.len();
        let e = haystack[s..].find(close)? + s;
        Some(&haystack[s..e])
    }
}
