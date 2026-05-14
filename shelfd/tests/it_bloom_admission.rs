//! SHELF-46 — End-to-end test that bloom-aware admission promotes
//! Parquet footer suffix reads into `Pool::Metadata`.
//!
//! Gated on `--features integration` (read the trap: without the
//! feature the suite silently exits 0 and looks like it passed; the
//! tests only actually run when the docker-compose MinIO is up).
//!
//! We do **not** seed a real Parquet file here — bloom-aware
//! admission's footer-suffix branch is structurally a length-vs-end
//! heuristic, so a 64 KiB blob ending at the object tail exercises
//! the same code path as a real footer. The optional bloom-block
//! parser path lives in unit tests under `shelfd::parquet_admit`
//! since wiring the `parquet_meta` cargo feature into the docker-
//! compose harness is out of scope for this ticket.

#![cfg(test)]

mod common;

use bytes::Bytes;
use common::{
    build_state_with_bloom, ensure_bucket, put_object, s3_client, skip_if_offline,
    spawn_server_with_shim, TEST_BUCKET,
};

#[tokio::test]
async fn bloom_admission_classifies_footer_suffix_read() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-bloom-footer";
    // 256 KiB object so a 64 KiB suffix is a clean tail read but
    // strictly less than the whole object — i.e. exercises the
    // `offset > 0 && offset + length == total_size` branch of the
    // classifier.
    let payload: Bytes = (0u8..=255)
        .cycle()
        .take(256 * 1024)
        .collect::<Vec<u8>>()
        .into();
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_bloom("shelf-bloom").await;
    let baseline_footer = shelfd::metrics::BLOOM_ADMIT_TOTAL
        .with_label_values(&["footer"])
        .get();
    let baseline_metadata_misses = state
        .metrics
        .misses_total
        .with_label_values(&["metadata"])
        .get();
    let baseline_rowgroup_misses = state
        .metrics
        .misses_total
        .with_label_values(&["rowgroup"])
        .get();

    let (_native, shim, cancel) = spawn_server_with_shim(state.clone()).await;
    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");

    // Issue a `bytes=-65536` suffix range — the same shape Trino's
    // `S3Input.readTail(n)` uses for Parquet/Avro footers.
    let http = reqwest::Client::new();
    let resp = http
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=-65536")
        .send()
        .await
        .expect("suffix get");
    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 65_536);

    // The classifier must have bumped `kind="footer"` for this read.
    let post_footer = shelfd::metrics::BLOOM_ADMIT_TOTAL
        .with_label_values(&["footer"])
        .get();
    assert!(
        post_footer > baseline_footer,
        "shelf_bloom_admit_total{{kind=\"footer\"}} must increment on a footer-suffix read \
         (baseline={baseline_footer}, post={post_footer})"
    );

    // Cache miss must have landed in the metadata pool, not the
    // rowgroup pool — that is the whole point of the policy.
    let post_metadata_misses = state
        .metrics
        .misses_total
        .with_label_values(&["metadata"])
        .get();
    let post_rowgroup_misses = state
        .metrics
        .misses_total
        .with_label_values(&["rowgroup"])
        .get();
    assert!(
        post_metadata_misses > baseline_metadata_misses,
        "metadata-pool miss must fire (baseline={baseline_metadata_misses}, \
         post={post_metadata_misses})"
    );
    assert_eq!(
        post_rowgroup_misses, baseline_rowgroup_misses,
        "rowgroup-pool miss counter must NOT advance for a footer read \
         (baseline={baseline_rowgroup_misses}, post={post_rowgroup_misses})"
    );

    cancel.cancel();
}

#[tokio::test]
async fn bloom_admission_leaves_mid_file_reads_alone() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-bloom-midfile";
    // Object key intentionally ends in `.parquet` so the default
    // pool router sends it to the rowgroup pool. A mid-file read
    // (offset 0, length 16) must NOT be reclassified by the bloom
    // policy — `kind="not_applicable"` should bump and the read
    // must land in the rowgroup pool, not metadata.
    let key_parquet = format!("{key}.parquet");
    let payload: Bytes = (0u8..=255)
        .cycle()
        .take(256 * 1024)
        .collect::<Vec<u8>>()
        .into();
    put_object(&client, &key_parquet, payload.clone()).await;

    let state = build_state_with_bloom("shelf-bloom-mid").await;
    let baseline_na = shelfd::metrics::BLOOM_ADMIT_TOTAL
        .with_label_values(&["not_applicable"])
        .get();
    let baseline_rowgroup_misses = state
        .metrics
        .misses_total
        .with_label_values(&["rowgroup"])
        .get();

    let (_native, shim, cancel) = spawn_server_with_shim(state.clone()).await;
    let url = format!("http://{shim}/{TEST_BUCKET}/{key_parquet}");

    let http = reqwest::Client::new();
    let resp = http
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()
        .await
        .expect("mid-file get");
    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 16);
    assert_eq!(&body[..], &payload[0..16]);

    let post_na = shelfd::metrics::BLOOM_ADMIT_TOTAL
        .with_label_values(&["not_applicable"])
        .get();
    assert!(
        post_na > baseline_na,
        "shelf_bloom_admit_total{{kind=\"not_applicable\"}} must increment on a mid-file read \
         (baseline={baseline_na}, post={post_na})"
    );

    let post_rowgroup_misses = state
        .metrics
        .misses_total
        .with_label_values(&["rowgroup"])
        .get();
    assert!(
        post_rowgroup_misses > baseline_rowgroup_misses,
        "rowgroup-pool miss must fire for a `.parquet` mid-file read \
         (baseline={baseline_rowgroup_misses}, post={post_rowgroup_misses})"
    );

    cancel.cancel();
}
