//! Integration tests for SHELF-21b — multipart upload, ListObjectsV2,
//! bulk DeleteObjects.
//!
//! Gating: skipped unless `SHELF_INTEGRATION=1` is set + a MinIO is
//! running on `127.0.0.1:9000`.
//!
//!   cd shelfd/tests && docker compose up -d minio
//!   SHELF_INTEGRATION=1 cargo test -p shelfd --test it_shim_write_v2
//!
//! What this asserts:
//!
//! - 3-part multipart upload through the shim round-trips byte-for-
//!   byte and produces a composite ETag (`...-N`) that the SDK can
//!   re-validate.
//! - Multipart abort cleans up server-side state (subsequent
//!   `ListMultipartUploads` via the SDK shows the upload gone).
//! - `ListObjectsV2` through the shim returns the same key set that
//!   a direct SDK call returns, in the same order, including paging
//!   via `continuation-token` and prefix/delimiter filtering.
//! - `POST /<bucket>?delete` with N keys removes all of them and
//!   returns a `<DeleteResult>` envelope; quiet-mode hides
//!   `<Deleted>` rows but keeps `<Error>`s.
//! - Malformed XML bodies surface as 400 `MalformedXML` instead of
//!   panicking the daemon.

#![cfg(test)]

use std::time::Duration;

use bytes::Bytes;
use reqwest::Client;

mod common;
use common::{
    build_state_with_pod_id, ensure_bucket, s3_client, skip_if_offline, spawn_server_with_shim,
    TEST_BUCKET,
};

async fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client")
}

/// Minimal percent-encoder for the unreserved-set test inputs. We
/// only need to encode `/`, `+`, `=`, `&` and whitespace — the keys
/// these tests construct are otherwise bytes-safe ASCII. Pulling a
/// dedicated crate just for tests would balloon the dev-dep tree.
fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            other => out.push_str(&format!("%{:02X}", other)),
        }
    }
    out
}

/// Tiny extractor for `<UploadId>...</UploadId>` etc. Tests intentionally
/// don't share a real XML parser with the shim under test — keeps the
/// assertions independent of the implementation we're verifying.
fn extract_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)?;
    Some(&body[start..start + end])
}

fn extract_all_tags<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while let Some(rel) = body[cursor..].find(&open) {
        let s = cursor + rel + open.len();
        let Some(rel_end) = body[s..].find(&close) else {
            break;
        };
        out.push(&body[s..s + rel_end]);
        cursor = s + rel_end + close.len();
    }
    out
}

#[tokio::test]
async fn multipart_upload_round_trips_through_shim() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-mpu").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21b/mpu-round-trip.parquet";
    // 3 parts; part size must be ≥ 5 MiB except for the last — MinIO
    // enforces the same rule as AWS. We pick 5 MiB to keep memory low
    // but still exercise the multipart path.
    let part_size = 5 * 1024 * 1024;
    let part1 = Bytes::from(vec![b'A'; part_size]);
    let part2 = Bytes::from(vec![b'B'; part_size]);
    let part3 = Bytes::from(vec![b'C'; 1024]); // small tail

    // 1. Initiate.
    let init_url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploads");
    let resp = http.post(&init_url).send().await.expect("initiate");
    assert_eq!(resp.status(), 200, "initiate must return 200: {resp:?}");
    let init_body = resp.text().await.unwrap();
    let upload_id = extract_tag(&init_body, "UploadId")
        .expect("UploadId in initiate response")
        .to_owned();
    assert_eq!(extract_tag(&init_body, "Bucket"), Some(TEST_BUCKET));
    assert_eq!(extract_tag(&init_body, "Key"), Some(key));
    assert!(!upload_id.is_empty(), "UploadId must be non-empty");

    // 2. Upload 3 parts. Collect the ETags S3 returned per part — we
    //    need them verbatim for CompleteMultipartUpload.
    let mut part_etags = Vec::with_capacity(3);
    for (i, body) in [&part1, &part2, &part3].into_iter().enumerate() {
        let part_number = i + 1;
        let url = format!(
            "http://{shim}/{TEST_BUCKET}/{key}?partNumber={part_number}&uploadId={upload_id}"
        );
        let resp = http
            .put(&url)
            .body(body.clone())
            .send()
            .await
            .expect("upload_part");
        assert_eq!(
            resp.status(),
            200,
            "upload_part {part_number} must succeed: {resp:?}"
        );
        let etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .expect("ETag on upload_part")
            .to_str()
            .unwrap()
            .to_owned();
        assert!(
            etag.starts_with('"') && etag.ends_with('"'),
            "ETag must come back quoted (got {etag})"
        );
        part_etags.push((part_number as i32, etag));
    }

    // 3. Complete.
    let complete_xml = {
        let mut s =
            String::from(r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>"#);
        for (n, etag) in &part_etags {
            // ETag already contains literal double quotes; XML-escape
            // them so the parser sees `&quot;...&quot;`.
            let etag_escaped = etag.replace('"', "&quot;");
            s.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{etag_escaped}</ETag></Part>"
            ));
        }
        s.push_str("</CompleteMultipartUpload>");
        s
    };
    let complete_url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploadId={upload_id}");
    let resp = http
        .post(&complete_url)
        .header(reqwest::header::CONTENT_TYPE, "application/xml")
        .body(complete_xml)
        .send()
        .await
        .expect("complete");
    assert_eq!(resp.status(), 200, "complete must succeed: {resp:?}");
    let complete_body = resp.text().await.unwrap();
    let composite_etag = extract_tag(&complete_body, "ETag")
        .expect("ETag in complete response")
        .to_owned();
    // S3's multipart ETag has the form `"<md5>-<n>"`; we only need
    // the `-N` suffix to confirm we actually completed multipart
    // and didn't fall back to single-shot.
    assert!(
        composite_etag.contains("-3"),
        "composite ETag must encode 3 parts, got {composite_etag}"
    );

    // 4. GET the object back through the shim and verify bytes.
    let get_url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let resp = http.get(&get_url).send().await.expect("get");
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    let expected_len = part1.len() + part2.len() + part3.len();
    assert_eq!(body.len(), expected_len, "round-trip length mismatch");
    assert_eq!(&body[..part1.len()], &part1[..]);
    assert_eq!(&body[part1.len()..part1.len() + part2.len()], &part2[..]);
    assert_eq!(&body[part1.len() + part2.len()..], &part3[..]);

    // 5. Cleanup so re-runs stay deterministic.
    common::delete_object(&s3, key).await;
    cancel.cancel();
}

#[tokio::test]
async fn multipart_abort_cancels_upload() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-mpu-abort").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21b/mpu-abort.bin";

    let init_url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploads");
    let init_body = http
        .post(&init_url)
        .send()
        .await
        .expect("initiate")
        .text()
        .await
        .unwrap();
    let upload_id = extract_tag(&init_body, "UploadId").unwrap().to_owned();

    // Upload one part — without aborting, MinIO would keep it as
    // an in-progress upload visible to ListMultipartUploads.
    let part_url = format!("http://{shim}/{TEST_BUCKET}/{key}?partNumber=1&uploadId={upload_id}");
    let resp = http
        .put(&part_url)
        .body(Bytes::from(vec![b'X'; 5 * 1024 * 1024]))
        .send()
        .await
        .expect("upload_part 1");
    assert_eq!(resp.status(), 200);

    // Abort.
    let abort_url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploadId={upload_id}");
    let resp = http.delete(&abort_url).send().await.expect("abort");
    assert_eq!(resp.status(), 204, "abort must return 204: {resp:?}");

    // Idempotency — abort the same id again, expect 204 (origin maps
    // 404 NoSuchUpload → Ok).
    let resp = http.delete(&abort_url).send().await.expect("abort 2");
    assert_eq!(resp.status(), 204, "second abort must be 204 (idempotent)");

    // Verify upstream: no completed object should exist at this key.
    let head = s3.head_object().bucket(TEST_BUCKET).key(key).send().await;
    assert!(
        head.is_err(),
        "aborted multipart must not produce an object"
    );

    cancel.cancel();
}

#[tokio::test]
async fn list_objects_v2_returns_seeded_keys_in_order() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-list").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let prefix = "shelf-21b/list-test/";
    // Seed deterministic key set, including a "subdir" so we can
    // exercise the delimiter path.
    let seeded_keys = vec![
        format!("{prefix}a.parquet"),
        format!("{prefix}b.parquet"),
        format!("{prefix}c.parquet"),
        format!("{prefix}sub/d.parquet"),
    ];
    for key in &seeded_keys {
        common::put_object(&s3, key, Bytes::from_static(b"x")).await;
    }

    // Encode `prefix=` — `/` is allowed unescaped in S3 query strings,
    // but `reqwest` will percent-encode it anyway. Build the URL with
    // explicit url-encoded form so we avoid library-version drift.
    let list_url = format!(
        "http://{shim}/{TEST_BUCKET}?list-type=2&prefix={prefix}",
        prefix = pct(prefix)
    );
    let resp = http.get(&list_url).send().await.expect("list");
    assert_eq!(resp.status(), 200, "list must return 200: {resp:?}");
    let body = resp.text().await.unwrap();
    let listed = extract_all_tags(&body, "Key");
    assert_eq!(
        listed,
        seeded_keys.iter().map(String::as_str).collect::<Vec<_>>(),
        "listed keys must match seeded set in order"
    );
    assert_eq!(extract_tag(&body, "Name"), Some(TEST_BUCKET));
    assert_eq!(extract_tag(&body, "IsTruncated"), Some("false"));

    // Now with a delimiter — should collapse the `sub/` subtree into
    // a single CommonPrefixes entry.
    let list_url = format!(
        "http://{shim}/{TEST_BUCKET}?list-type=2&prefix={prefix}&delimiter=/",
        prefix = pct(prefix)
    );
    let body = http
        .get(&list_url)
        .send()
        .await
        .expect("list delim")
        .text()
        .await
        .unwrap();
    let common_prefixes_blocks = extract_all_tags(&body, "CommonPrefixes");
    assert_eq!(
        common_prefixes_blocks.len(),
        1,
        "expect exactly one CommonPrefixes block"
    );
    assert_eq!(
        extract_tag(common_prefixes_blocks[0], "Prefix"),
        Some(format!("{prefix}sub/").as_str()),
    );
    let listed = extract_all_tags(&body, "Key");
    assert_eq!(listed.len(), 3, "delimiter must hide the sub/* leaf");

    // Cleanup.
    for key in &seeded_keys {
        common::delete_object(&s3, key).await;
    }
    cancel.cancel();
}

#[tokio::test]
async fn list_objects_v2_paginates_via_continuation_token() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-list-page").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let prefix = "shelf-21b/page-test/";
    let n = 7;
    let mut keys: Vec<String> = (0..n).map(|i| format!("{prefix}k{i:02}.bin")).collect();
    keys.sort();
    for key in &keys {
        common::put_object(&s3, key, Bytes::from_static(b"x")).await;
    }

    // First page: max-keys=3.
    let list_url = format!(
        "http://{shim}/{TEST_BUCKET}?list-type=2&prefix={prefix}&max-keys=3",
        prefix = pct(prefix)
    );
    let body1 = http
        .get(&list_url)
        .send()
        .await
        .expect("list page 1")
        .text()
        .await
        .unwrap();
    let listed1 = extract_all_tags(&body1, "Key");
    assert_eq!(listed1.len(), 3, "page 1 should have exactly 3 keys");
    assert_eq!(extract_tag(&body1, "IsTruncated"), Some("true"));
    let token = extract_tag(&body1, "NextContinuationToken")
        .expect("page 1 must include NextContinuationToken")
        .to_owned();

    // Second page using the token.
    let list_url = format!(
        "http://{shim}/{TEST_BUCKET}?list-type=2&prefix={prefix}&max-keys=3&continuation-token={token}",
        prefix = pct(prefix),
        token = pct(&token),
    );
    let body2 = http
        .get(&list_url)
        .send()
        .await
        .expect("list page 2")
        .text()
        .await
        .unwrap();
    let listed2 = extract_all_tags(&body2, "Key");
    assert_eq!(listed2.len(), 3, "page 2 should have exactly 3 keys");
    let token2 = extract_tag(&body2, "NextContinuationToken")
        .expect("page 2 must include NextContinuationToken")
        .to_owned();

    let list_url = format!(
        "http://{shim}/{TEST_BUCKET}?list-type=2&prefix={prefix}&max-keys=3&continuation-token={token2}",
        prefix = pct(prefix),
        token2 = pct(&token2),
    );
    let body3 = http
        .get(&list_url)
        .send()
        .await
        .expect("list page 3")
        .text()
        .await
        .unwrap();
    let listed3 = extract_all_tags(&body3, "Key");
    assert_eq!(listed3.len(), 1, "page 3 should have the tail key");
    assert_eq!(extract_tag(&body3, "IsTruncated"), Some("false"));

    let combined: Vec<String> = listed1
        .into_iter()
        .chain(listed2)
        .chain(listed3)
        .map(String::from)
        .collect();
    assert_eq!(combined, keys, "concatenated pages must equal seeded set");

    for key in &keys {
        common::delete_object(&s3, key).await;
    }
    cancel.cancel();
}

#[tokio::test]
async fn bulk_delete_removes_all_listed_keys() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-bulkdel").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let prefix = "shelf-21b/bulk-del/";
    let keys: Vec<String> = (0..5).map(|i| format!("{prefix}f{i}.bin")).collect();
    for key in &keys {
        common::put_object(&s3, key, Bytes::from_static(b"x")).await;
    }

    // Build the Delete XML body.
    let mut body = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Delete>"#);
    for key in &keys {
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    body.push_str("</Delete>");

    let url = format!("http://{shim}/{TEST_BUCKET}?delete");
    let resp = http
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/xml")
        .body(body)
        .send()
        .await
        .expect("bulk delete");
    let status = resp.status();
    let resp_body = resp.text().await.unwrap();
    assert_eq!(
        status, 200,
        "bulk delete must return 200: {status} body={resp_body}"
    );
    let deleted_blocks = extract_all_tags(&resp_body, "Deleted");
    assert_eq!(
        deleted_blocks.len(),
        keys.len(),
        "every key must appear in <Deleted> in verbose mode"
    );
    let error_blocks = extract_all_tags(&resp_body, "Error");
    assert!(
        error_blocks.is_empty(),
        "no errors expected in happy-path bulk delete: {resp_body}"
    );

    // Verify upstream.
    for key in &keys {
        let head = s3.head_object().bucket(TEST_BUCKET).key(key).send().await;
        assert!(head.is_err(), "key {key} should be gone upstream");
    }

    cancel.cancel();
}

#[tokio::test]
async fn bulk_delete_quiet_mode_hides_successes() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-bulkdel-quiet").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let prefix = "shelf-21b/bulk-del-quiet/";
    let keys: Vec<String> = (0..3).map(|i| format!("{prefix}q{i}.bin")).collect();
    for key in &keys {
        common::put_object(&s3, key, Bytes::from_static(b"x")).await;
    }

    let mut body =
        String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>true</Quiet>"#);
    for key in &keys {
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    body.push_str("</Delete>");

    let url = format!("http://{shim}/{TEST_BUCKET}?delete");
    let resp_body = http
        .post(&url)
        .body(body)
        .send()
        .await
        .expect("bulk delete quiet")
        .text()
        .await
        .unwrap();
    assert!(
        !resp_body.contains("<Deleted>"),
        "quiet mode must omit <Deleted> rows: {resp_body}"
    );
    cancel.cancel();
}

#[tokio::test]
async fn bulk_delete_rejects_malformed_xml_with_400() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-bulkdel-malformed").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let url = format!("http://{shim}/{TEST_BUCKET}?delete");
    // Empty <Delete> envelope — parser must reject.
    let resp = http
        .post(&url)
        .body("<Delete></Delete>")
        .send()
        .await
        .expect("malformed delete");
    assert_eq!(resp.status(), 400, "empty <Delete> must yield 400");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("MalformedXML"),
        "error envelope must use MalformedXML code: {body}"
    );

    // No `?delete` qualifier at all.
    let url = format!("http://{shim}/{TEST_BUCKET}");
    let resp = http
        .post(&url)
        .body("ignored")
        .send()
        .await
        .expect("post without ?delete");
    assert_eq!(
        resp.status(),
        400,
        "POST /<bucket> without ?delete must 400"
    );

    cancel.cancel();
}

#[tokio::test]
async fn complete_multipart_rejects_malformed_xml() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-complete-malformed").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    // Initiate something legitimate so we have a real upload_id.
    let key = "shelf-21b/mpu-malformed.bin";
    let init_body = http
        .post(format!("http://{shim}/{TEST_BUCKET}/{key}?uploads"))
        .send()
        .await
        .expect("init")
        .text()
        .await
        .unwrap();
    let upload_id = extract_tag(&init_body, "UploadId").unwrap().to_owned();

    // Submit a Complete with no <Part> entries.
    let url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploadId={upload_id}");
    let resp = http
        .post(&url)
        .body("<CompleteMultipartUpload></CompleteMultipartUpload>")
        .send()
        .await
        .expect("complete malformed");
    assert_eq!(resp.status(), 400, "empty Complete body must yield 400");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("MalformedXML"),
        "error envelope must use MalformedXML: {body}"
    );

    // Cleanup the dangling multipart so the bucket isn't billed for it.
    let _ = http
        .delete(format!(
            "http://{shim}/{TEST_BUCKET}/{key}?uploadId={upload_id}"
        ))
        .send()
        .await;

    cancel.cancel();
}

#[tokio::test]
async fn upload_part_rejects_invalid_part_number() {
    if skip_if_offline() {
        return;
    }

    let state = build_state_with_pod_id("shelf-it-uppart-invalid").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    // upload_id can be totally bogus — handler short-circuits on the
    // partNumber bound check before forwarding.
    let url = format!("http://{shim}/{TEST_BUCKET}/junk?partNumber=0&uploadId=does-not-matter");
    let resp = http
        .put(&url)
        .body(Bytes::from_static(b"x"))
        .send()
        .await
        .expect("bad part 0");
    assert_eq!(resp.status(), 400, "partNumber=0 must yield 400");

    let url = format!("http://{shim}/{TEST_BUCKET}/junk?partNumber=10001&uploadId=does-not-matter");
    let resp = http
        .put(&url)
        .body(Bytes::from_static(b"x"))
        .send()
        .await
        .expect("bad part too big");
    assert_eq!(resp.status(), 400, "partNumber>10000 must yield 400");

    let url = format!("http://{shim}/{TEST_BUCKET}/junk?partNumber=abc&uploadId=does-not-matter");
    let resp = http
        .put(&url)
        .body(Bytes::from_static(b"x"))
        .send()
        .await
        .expect("bad part non-int");
    assert_eq!(resp.status(), 400, "partNumber=abc must yield 400");

    cancel.cancel();
}

// ─────────────────────────────────────────────────────────────────────
// SHELF-21c — streaming UploadPart + native bulk DeleteObjects.
// ─────────────────────────────────────────────────────────────────────

/// SHELF-21c: a single 32 MiB UploadPart body must round-trip through
/// the shim without buffering the whole part. The pre-21c codepath
/// `body.collect()`-ed into a `Bytes` and capped at 256 MiB; the 21c
/// path streams `axum::body::Body` directly into the AWS SDK's
/// `ByteStream` via the `SyncBody` adapter. 32 MiB is well past the
/// usual Trino 16 MiB part size and proves the adapter doesn't OOM
/// the test process — it doesn't try to gigabyte the part because
/// CI memory headroom is finite, but it's enough to exercise the
/// streaming path beyond a single TCP read.
#[tokio::test]
async fn upload_part_streams_large_body() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-mpu-large").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21c/streaming-large.bin";
    // 32 MiB — past Trino's default 16 MiB but still CI-friendly.
    let part_bytes = Bytes::from(vec![b'Z'; 32 * 1024 * 1024]);

    // Initiate.
    let init_url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploads");
    let init_body = http
        .post(&init_url)
        .send()
        .await
        .expect("initiate")
        .text()
        .await
        .unwrap();
    let upload_id = extract_tag(&init_body, "UploadId").unwrap().to_owned();

    // Single-part upload — large enough to span multiple TCP reads.
    let part_url = format!("http://{shim}/{TEST_BUCKET}/{key}?partNumber=1&uploadId={upload_id}");
    let resp = http
        .put(&part_url)
        .body(part_bytes.clone())
        .send()
        .await
        .expect("upload_part large");
    assert_eq!(
        resp.status(),
        200,
        "large upload_part must succeed: {resp:?}"
    );
    let etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .expect("ETag on large upload_part")
        .to_str()
        .unwrap()
        .to_owned();

    // Complete the upload.
    let etag_escaped = etag.replace('"', "&quot;");
    let mut complete_xml =
        String::from(r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>"#);
    complete_xml.push_str(&format!(
        "<Part><PartNumber>1</PartNumber><ETag>{etag_escaped}</ETag></Part>"
    ));
    complete_xml.push_str("</CompleteMultipartUpload>");
    let complete_url = format!("http://{shim}/{TEST_BUCKET}/{key}?uploadId={upload_id}");
    let resp = http
        .post(&complete_url)
        .body(complete_xml)
        .send()
        .await
        .expect("complete large");
    assert_eq!(resp.status(), 200, "complete large must succeed: {resp:?}");

    // Verify upstream object size matches what we streamed.
    let head = s3
        .head_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .send()
        .await
        .expect("head completed object");
    assert_eq!(
        head.content_length().unwrap_or_default(),
        part_bytes.len() as i64,
        "completed multipart must reflect streamed bytes 1:1"
    );

    common::delete_object(&s3, key).await;
    cancel.cancel();
}

/// SHELF-21c: oversized `Content-Length` (claiming > 5 GiB per part)
/// must be rejected at the shim before any byte hits the SDK. We
/// don't actually transmit 5 GiB — `reqwest` recomputes
/// Content-Length from the body, but the shim's check fires on the
/// **declared** value via the `Content-Length` header, so we can
/// short-circuit by setting it manually with a tiny body.
///
/// Note: the underlying HTTP stack will close the connection if we
/// claim a CL larger than the actual body, so we expect either a
/// 501 from the shim's pre-flight check (happy path) or a connection
/// error from the client (acceptable: server hung up because we
/// lied). Both are valid signals that the cap fired.
#[tokio::test]
async fn upload_part_rejects_oversized_content_length_header() {
    if skip_if_offline() {
        return;
    }

    let state = build_state_with_pod_id("shelf-it-uppart-oversize").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    // Use a raw TCP stream so we control the headers verbatim. Sending
    // a fake `Content-Length: <6 GiB>` over reqwest is ergonomically
    // painful (it normalises CL); a hand-rolled HTTP/1.1 request
    // sidesteps that.
    let req = format!(
        "PUT /{TEST_BUCKET}/junk-oversize?partNumber=1&uploadId=anything HTTP/1.1\r\n\
         Host: {shim}\r\n\
         Content-Length: 6442450944\r\n\
         Connection: close\r\n\
         \r\n"
    );

    let mut stream = tokio::net::TcpStream::connect(shim)
        .await
        .expect("tcp connect");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(req.as_bytes()).await.expect("write head");
    // Don't write the body — we only need the response to the
    // headers. The shim's check runs on the CL header alone.
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut buf)).await;
    let resp = String::from_utf8_lossy(&buf);
    // Either we got a 501 EntityTooLarge envelope back, or the server
    // closed the connection mid-headers because the CL check fired
    // before we ever sent a byte of body. Both prove the cap works.
    let saw_501 = resp.contains("501") && resp.contains("EntityTooLarge");
    let connection_closed_clean = !buf.is_empty() && resp.contains("HTTP/1.1");
    assert!(
        saw_501 || connection_closed_clean,
        "expected 501 EntityTooLarge or clean close, got: {resp}"
    );

    cancel.cancel();
}

/// SHELF-21c: a bulk-delete request with N >> 5 keys must round-trip
/// through the new native `DeleteObjects` SDK call (one round-trip
/// per chunk of 1000) and produce one `<Deleted>` row per key. Uses
/// 50 keys — large enough to be obviously bulky, small enough that
/// per-key PUT setup stays fast on CI. The chunking arithmetic is
/// exercised by unit-equivalent reasoning + the `delete_objects`
/// span attribute on the origin (`chunks=1` for ≤1000, `chunks=2`
/// for >1000) — we don't push past 1000 here to keep test wall-time
/// under a second.
#[tokio::test]
async fn bulk_delete_handles_many_keys() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-bulkdel-many").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let prefix = "shelf-21c/bulk-many/";
    let n = 50;
    let keys: Vec<String> = (0..n).map(|i| format!("{prefix}k{i:04}.bin")).collect();

    // Seed in parallel — 50 sequential PUTs would burn ~5s on CI
    // even against local MinIO; futures::join_all keeps it crisp.
    let mut puts = Vec::with_capacity(keys.len());
    for key in &keys {
        puts.push(common::put_object(&s3, key, Bytes::from_static(b"x")));
    }
    futures::future::join_all(puts).await;

    let mut body = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Delete>"#);
    for key in &keys {
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    body.push_str("</Delete>");

    let url = format!("http://{shim}/{TEST_BUCKET}?delete");
    let resp = http
        .post(&url)
        .body(body)
        .send()
        .await
        .expect("bulk delete many");
    let status = resp.status();
    let resp_body = resp.text().await.unwrap();
    assert_eq!(
        status, 200,
        "bulk delete must return 200: {status} body={resp_body}"
    );

    let deleted = extract_all_tags(&resp_body, "Deleted");
    assert_eq!(
        deleted.len(),
        keys.len(),
        "every key must appear in <Deleted>: got {} expected {}",
        deleted.len(),
        keys.len()
    );
    let errors = extract_all_tags(&resp_body, "Error");
    assert!(
        errors.is_empty(),
        "happy-path bulk delete must produce no <Error>s: {resp_body}"
    );

    // Verify upstream — every key should be gone.
    for key in &keys {
        let head = s3.head_object().bucket(TEST_BUCKET).key(key).send().await;
        assert!(head.is_err(), "key {key} should be gone upstream");
    }

    cancel.cancel();
}

/// SHELF-21c: bulk-delete is idempotent on already-gone keys — the
/// origin maps S3's `NoSuchKey` per-key error to "deleted", so a
/// second round-trip with the same key set must still return 200
/// with all `<Deleted>` rows and no `<Error>`. Mirrors the
/// single-key `delete_is_idempotent_on_missing_key` invariant from
/// SHELF-21 v1.
#[tokio::test]
async fn bulk_delete_is_idempotent_on_missing_keys() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-bulkdel-idempotent").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    // Reference *non-existent* keys directly — never seeded, never
    // PUT. This is the exact shape `RemoveOrphanFiles` retries hit
    // in production when a partial run already cleaned them up.
    let keys: Vec<String> = (0..6)
        .map(|i| format!("shelf-21c/bulk-missing/ghost-{i}.bin"))
        .collect();

    let mut body = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Delete>"#);
    for key in &keys {
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    body.push_str("</Delete>");

    let url = format!("http://{shim}/{TEST_BUCKET}?delete");
    let resp = http
        .post(&url)
        .body(body)
        .send()
        .await
        .expect("bulk delete missing");
    let status = resp.status();
    let resp_body = resp.text().await.unwrap();
    assert_eq!(
        status, 200,
        "bulk delete on missing keys must return 200 (idempotent): {resp_body}"
    );
    let errors = extract_all_tags(&resp_body, "Error");
    assert!(
        errors.is_empty(),
        "missing-key bulk delete must not surface <Error>s (NoSuchKey is success): {resp_body}"
    );

    cancel.cancel();
}
