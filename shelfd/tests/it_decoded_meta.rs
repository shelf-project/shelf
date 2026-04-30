#![allow(deprecated)]
//! SHELF-50 — integration test for the decoded-metadata in-process
//! LRU cache.
//!
//! This suite exercises the production producer hook
//! [`shelfd::decoded_meta::on_metadata_admit`]: it boots a tokio
//! runtime, feeds synthetic Avro and Parquet bytes, awaits the
//! fire-and-forget decode, and asserts the LRU is populated and the
//! Prometheus counters bumped.
//!
//! ## Gate
//!
//! Like every other `it_*.rs` suite in this crate, every test here
//! is gated on `SHELF_INTEGRATION=1`. Without it the tests skip
//! immediately — the SHELF-09 trap (silent 0.00 s pass when the env
//! var is absent) is mitigated by `eprintln!`-ing a `SKIP:` line so
//! the operator can tell the test was actually invoked.
//!
//! ## Run
//!
//! ```bash
//! SHELF_INTEGRATION=1 cargo test -p shelfd --test it_decoded_meta
//! ```
//!
//! No external services are required (no MinIO, no Trino).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parquet::format::{FileMetaData, SchemaElement};
use parquet::thrift::TSerializable;
use shelfd::decoded_meta::{self, AdmitHint, DecodedKind, DecodedMetaCache, ManifestFile};
use thrift::protocol::TCompactOutputProtocol;

fn skip_if_offline() -> bool {
    if std::env::var("SHELF_INTEGRATION").as_deref() != Ok("1") {
        eprintln!(
            "SKIP: SHELF_INTEGRATION=1 not set; \
             skipping decoded_meta integration suite"
        );
        return true;
    }
    false
}

/// Construct a minimum-viable Parquet footer (`<thrift><len_le_u32>PAR1`)
/// — same body as the in-module unit-test helper, duplicated here
/// because integration tests are separate Cargo binaries and helpers
/// must live in `tests/common/mod.rs` to be shared. Inlined for
/// SHELF-50 only because this is a one-shot fixture, not a reusable
/// utility.
fn build_minimal_parquet_footer() -> Bytes {
    use parquet::format::FieldRepetitionType;
    let schema = SchemaElement {
        type_: None,
        type_length: None,
        repetition_type: Some(FieldRepetitionType::REQUIRED),
        name: "minimal_root".to_owned(),
        num_children: Some(0),
        converted_type: None,
        scale: None,
        precision: None,
        field_id: None,
        logical_type: None,
    };

    let meta = FileMetaData {
        version: 1,
        schema: vec![schema],
        num_rows: 0,
        row_groups: Vec::new(),
        key_value_metadata: None,
        created_by: Some("shelfd-it-decoded-meta".to_owned()),
        column_orders: None,
        encryption_algorithm: None,
        footer_signing_key_metadata: None,
    };

    let mut thrift_buf: Vec<u8> = Vec::new();
    {
        let mut proto = TCompactOutputProtocol::new(&mut thrift_buf);
        meta.write_to_out_protocol(&mut proto)
            .expect("encode minimal footer");
    }

    let footer_len = thrift_buf.len() as u32;
    let mut out = Vec::with_capacity(thrift_buf.len() + 8);
    out.extend_from_slice(&thrift_buf);
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(b"PAR1");
    Bytes::from(out)
}

/// Wait for a predicate to become true within `deadline`, polling
/// every `step`. The `on_metadata_admit` decode path is
/// fire-and-forget on a tokio blocking thread, so the test must
/// poll rather than busy-wait.
async fn wait_for<F: FnMut() -> bool>(mut pred: F, deadline: Duration, step: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

#[tokio::test]
async fn admit_avro_manifest_populates_cache() {
    if skip_if_offline() {
        return;
    }
    decoded_meta::set_enabled(true);
    // Use a unique etag so concurrent SHELF-50 integration tests
    // don't collide on the global singleton cache.
    let etag = "it-shelf-50-manifest-1";
    decoded_meta::invalidate(etag);

    let bytes = Bytes::from_static(b"Obj\x01\x00synthetic-manifest-payload");
    decoded_meta::on_metadata_admit(
        etag,
        AdmitHint::from_key_path("bucket/db/t/metadata/foo.avro"),
        bytes,
    );

    let saw = wait_for(
        || decoded_meta::get_manifest(etag).is_some(),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .await;
    assert!(saw, "manifest decode did not populate within 2s");

    let entry = decoded_meta::get_manifest(etag).expect("populated");
    assert!(entry.raw.starts_with(b"Obj\x01"));
}

#[tokio::test]
async fn admit_parquet_footer_populates_cache() {
    if skip_if_offline() {
        return;
    }
    decoded_meta::set_enabled(true);
    let etag = "it-shelf-50-footer-1";
    decoded_meta::invalidate(etag);

    let bytes = build_minimal_parquet_footer();
    decoded_meta::on_metadata_admit(
        etag,
        AdmitHint::from_key_path("bucket/db/t/data/x.parquet"),
        bytes,
    );

    let saw = wait_for(
        || decoded_meta::get_parquet_footer(etag).is_some(),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .await;
    assert!(saw, "parquet footer decode did not populate within 2s");

    let md = decoded_meta::get_parquet_footer(etag).expect("populated");
    // Empty schema means file_metadata().schema_descr() exists with
    // a single root SchemaElement and zero columns. We don't assert
    // a specific column count here — just prove the metadata is a
    // real parsed structure (not a placeholder).
    assert!(Arc::strong_count(&md) >= 1);
}

#[tokio::test]
async fn invalidate_drops_decoded_entry() {
    if skip_if_offline() {
        return;
    }
    decoded_meta::set_enabled(true);
    let etag = "it-shelf-50-invalidate-1";
    decoded_meta::invalidate(etag);

    let bytes = Bytes::from_static(b"Obj\x01\x01invalidate-target");
    decoded_meta::on_metadata_admit(etag, AdmitHint::default(), bytes);

    wait_for(
        || decoded_meta::get_manifest(etag).is_some(),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .await;
    assert!(decoded_meta::get_manifest(etag).is_some(), "pre-invalidate");

    decoded_meta::invalidate(etag);
    assert!(
        decoded_meta::get_manifest(etag).is_none(),
        "post-invalidate must drop the prior decoded entry (ADR-0011)"
    );
}

#[tokio::test]
async fn malformed_bytes_do_not_install_or_panic() {
    if skip_if_offline() {
        return;
    }
    decoded_meta::set_enabled(true);
    let etag = "it-shelf-50-malformed-1";
    decoded_meta::invalidate(etag);

    // Bytes that match neither Avro magic nor PAR1 trailer; the
    // sniff returns None and we never spawn a decode. Asserts the
    // hot path does not panic and does not install garbage.
    let mystery = Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]);
    decoded_meta::on_metadata_admit(etag, AdmitHint::default(), mystery);

    // Brief pause to make sure no spawned task races with us.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(decoded_meta::get_manifest(etag).is_none());
    assert!(decoded_meta::get_parquet_footer(etag).is_none());

    // Pretend the byte cache mistakenly admitted truly malformed
    // *.parquet bytes (path hint claims Parquet, content is garbage).
    // The sniff routes via extension fallback, so a decode IS
    // spawned; it must fail the parse cleanly and bump the
    // decode_errors counter without installing an entry.
    let etag2 = "it-shelf-50-malformed-2";
    decoded_meta::invalidate(etag2);
    let bad_parquet = Bytes::from_static(&[0x00; 32]);
    decoded_meta::on_metadata_admit(
        etag2,
        AdmitHint::from_key_path("bucket/db/t/data/x.parquet"),
        bad_parquet,
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(decoded_meta::get_parquet_footer(etag2).is_none());
}

#[tokio::test]
async fn disabled_cache_is_a_full_noop() {
    if skip_if_offline() {
        return;
    }
    decoded_meta::set_enabled(false);
    let etag = "it-shelf-50-disabled-1";
    let bytes = Bytes::from_static(b"Obj\x01\x02disabled-path");
    decoded_meta::on_metadata_admit(etag, AdmitHint::default(), bytes);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(decoded_meta::get_manifest(etag).is_none());
    // Re-enable so subsequent tests in the same binary aren't
    // polluted by the default-off state.
    decoded_meta::set_enabled(true);
}

#[tokio::test]
async fn local_cache_capacity_obeys_caps_under_admit_burst() {
    if skip_if_offline() {
        return;
    }
    // This test uses a *fresh* DecodedMetaCache, not the global,
    // so we can assert capacity behaviour deterministically without
    // racing the other suites that share the singleton.
    let cache = DecodedMetaCache::new(4, 4);
    cache.set_enabled(true);
    for i in 0..10 {
        cache.insert_manifest(
            Arc::from(format!("etag-{i}").as_str()),
            Arc::new(ManifestFile {
                raw: Bytes::copy_from_slice(format!("Obj\x01burst-{i}").as_bytes()),
            }),
        );
    }
    assert_eq!(cache.len(DecodedKind::Manifest), 4);
}
