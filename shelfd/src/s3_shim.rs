//! S3-compatibility **read** shim (SHELF-22).
//!
//! This module serves a minimal subset of the S3 REST protocol on its
//! own listener (default `0.0.0.0:9092`) so generic clients — boto3,
//! DuckDB, Polars, `aws s3 cp` — can read through Shelf without any
//! AWS credentials and without the Trino plugin.
//!
//! In scope:
//!
//! - `HEAD /:bucket/*key`  -> `HeadObject`
//! - `GET  /:bucket/*key`  -> `GetObject` (optionally `Range:
//!   bytes=<start>-<end>`)
//!
//! Explicitly **out of scope** (see `docs/design-notes/
//! SHELF-22-s3-compat-shim.md` for the full matrix): SigV4
//! authentication, presigned URLs, multipart uploads, `ListObjects`,
//! `PutObject`, `DeleteObject`, virtual-hosted-style addressing.
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

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::head_lru::HeadMeta;
use crate::http::ServerState;
use crate::origin::Origin;
use crate::store::{key_from_tuple, Pool, ReadOutcome};

/// Build the shim router. Pure function, no I/O.
///
/// Keep the route shape path-style (`/:bucket/*key`) so swapping a
/// client's `endpoint_url` from real S3 to `http://shelfd:9092` is a
/// one-line change.
pub fn router(state: Arc<ServerState>) -> axum::Router {
    axum::Router::new()
        .route(
            "/:bucket/*key",
            get(handle_get_object).head(handle_head_object),
        )
        .with_state(state)
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

/// Parse `Range: bytes=<start>-<end>`; `end` is INCLUSIVE in HTTP.
///
/// Returns `(offset, length)` on success.
/// `Ok(None)` ⇒ no `Range` header at all.
/// `Err(())` ⇒ malformed header (InvalidRange).
fn parse_range_header(headers: &HeaderMap) -> Result<Option<(u64, u64)>, ()> {
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
    let start: u64 = start.parse().map_err(|_| ())?;
    if end.is_empty() {
        return Err(());
    }
    let end: u64 = end.parse().map_err(|_| ())?;
    if end < start {
        return Err(());
    }
    Ok(Some((start, end - start + 1)))
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
    match state.origin.as_ref().head(bucket, key).await {
        Ok(Some(head)) => {
            let meta: HeadMeta = head.into();
            state
                .head_lru
                .insert(bucket.to_owned(), key.to_owned(), meta.clone());
            Ok(Some(meta))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(s3_internal_error("origin.head", &e.to_string())),
    }
}

/// `HEAD /:bucket/*key` — S3 `HeadObject`.
pub async fn handle_head_object(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    match head_meta(&state, &bucket, &key).await {
        Ok(Some(meta)) => {
            let mut headers = HeaderMap::new();
            stamp_common_headers(&mut headers, &meta);
            (StatusCode::OK, headers).into_response()
        }
        Ok(None) => no_such_key(&bucket, &key),
        Err(resp) => resp,
    }
}

/// `GET /:bucket/*key` — S3 `GetObject` (honours `Range:` if set).
pub async fn handle_get_object(
    State(state): State<Arc<ServerState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let range = match parse_range_header(&headers) {
        Ok(r) => r,
        Err(()) => {
            // No size context yet → `bytes */0` is still a valid
            // `Content-Range` per RFC 9110.
            return invalid_range(0);
        }
    };

    let meta = match head_meta(&state, &bucket, &key).await {
        Ok(Some(m)) => m,
        Ok(None) => return no_such_key(&bucket, &key),
        Err(resp) => return resp,
    };
    let total_size = meta.content_length;

    let (offset, length, is_partial) = match range {
        Some((start, len)) => {
            if total_size == 0 || start >= total_size || start + len > total_size {
                return invalid_range(total_size);
            }
            (start, len, true)
        }
        None => {
            let cap = state
                .s3_shim_max_full_object_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            if total_size > cap {
                return not_implemented_oversized(total_size, cap);
            }
            if total_size == 0 {
                let mut headers = HeaderMap::new();
                stamp_common_headers(&mut headers, &meta);
                return (StatusCode::OK, headers, Vec::<u8>::new()).into_response();
            }
            (0u64, total_size, false)
        }
    };

    // Content-addressed key: mirror the native read-path derivation
    // so shim + plugin reads collide on the same slot.
    let etag_bytes = meta
        .etag
        .as_deref()
        .map(str::as_bytes)
        .unwrap_or_default()
        .to_vec();
    let key_obj = match key_from_tuple(&etag_bytes, offset, length, 0) {
        Ok(k) => k,
        Err(e) => return s3_internal_error("key.derive", &e.to_string()),
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

    let bytes = match outcome {
        Ok(ReadOutcome::Hit(b)) | Ok(ReadOutcome::Miss(b)) => b,
        Err(e) => return s3_internal_error("origin/store", &e.to_string()),
    };

    let mut headers = HeaderMap::new();
    stamp_common_headers(&mut headers, &meta);
    // Override `Content-Length` to the sliced length; `stamp_common`
    // reports the full-object size, which is wrong for a 206.
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("u64 is ASCII"),
    );
    let status = if is_partial {
        let cr = format!("bytes {}-{}/{}", offset, offset + length - 1, total_size);
        if let Ok(v) = HeaderValue::from_str(&cr) {
            headers.insert(header::CONTENT_RANGE, v);
        }
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    (status, headers, bytes).into_response()
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

    fn extract_between<'a>(haystack: &'a str, open: &str, close: &str) -> Option<&'a str> {
        let s = haystack.find(open)? + open.len();
        let e = haystack[s..].find(close)? + s;
        Some(&haystack[s..e])
    }
}
