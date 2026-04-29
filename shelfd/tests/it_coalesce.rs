//! SHELF-49 — coalesced range-GET integration test.
//!
//! Drives the existing MinIO docker-compose fixture
//! (`shelf/shelfd/tests/docker-compose.yml`) directly with three
//! concurrent ranges over the same `(bucket, key, etag)` triple,
//! wired through the production `S3OriginFetcher` adapter into
//! `Coalescer`. Asserts the dispatcher collapses them into exactly
//! one origin GET (verified via the
//! `shelf_origin_request_seconds{op="get_range", outcome="ok"}`
//! histogram count delta — same hook the production hot path
//! observes from `S3Origin::get_range`).
//!
//! Gated on `SHELF_INTEGRATION=1` so a stock `cargo test --release`
//! without a running MinIO does not pretend to pass — the AGENTS.md
//! "exit ~0.00s pretending to pass" trap. The skip path prints a
//! one-line marker so CI logs make the gate obvious.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use shelfd::coalesce::{Coalescer, S3OriginFetcher};
use shelfd::config::CoalesceConfig;
use shelfd::metrics::REGISTRY;

mod common;
use common::{
    build_state_with_pod_id, ensure_bucket, put_object, s3_client, skip_if_offline, TEST_BUCKET,
};

/// Total origin GET sample count for `(bucket, op="get_range",
/// outcome="ok")` across the global histogram.
fn origin_get_range_ok_count(bucket: &str) -> u64 {
    let families = REGISTRY.gather();
    let mut total: u64 = 0;
    for fam in families {
        if fam.name() != "shelf_origin_request_seconds" {
            continue;
        }
        for m in fam.get_metric() {
            let mut bucket_ok = false;
            let mut op_ok = false;
            let mut outcome_ok = false;
            for label in m.get_label() {
                match label.name() {
                    "bucket" if label.value() == bucket => bucket_ok = true,
                    "op" if label.value() == "get_range" => op_ok = true,
                    "outcome" if label.value() == "ok" => outcome_ok = true,
                    _ => {}
                }
            }
            if bucket_ok && op_ok && outcome_ok {
                total += m.get_histogram().get_sample_count();
            }
        }
    }
    total
}

/// Total `shelf_coalesce_ranges_total{outcome="coalesced"}` across
/// the process. Used as a positive-side assertion that the
/// dispatcher's "coalesced" arm actually fired (rather than relying
/// only on the indirect origin-call delta).
fn coalesced_outcome_count() -> u64 {
    let families = REGISTRY.gather();
    let mut total: u64 = 0;
    for fam in families {
        if fam.name() != "shelf_coalesce_ranges_total" {
            continue;
        }
        for m in fam.get_metric() {
            for label in m.get_label() {
                if label.name() == "outcome" && label.value() == "coalesced" {
                    total += m.get_counter().get_value() as u64;
                }
            }
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread")]
async fn three_concurrent_adjacent_ranges_collapse_to_one_origin_get() {
    if skip_if_offline() {
        return;
    }

    let client = s3_client().await;
    ensure_bucket(&client).await;

    // Seed a 1 MiB object whose bytes are deterministic on offset.
    // Same byte pattern as the unit-test mock so a hex inspection
    // of the body matches across both layers.
    let mut buf = Vec::with_capacity(1024 * 1024);
    for i in 0..buf.capacity() {
        buf.push((i & 0xff) as u8);
    }
    let key = format!("shelf-49-coalesce-{}.bin", std::process::id());
    put_object(&client, &key, Bytes::from(buf.clone())).await;

    // Reuse `build_state_with_pod_id` solely to get a configured
    // `S3Origin` against MinIO; we don't need the rest of
    // `ServerState` for this test (the Coalescer wraps the origin
    // directly).
    let state = build_state_with_pod_id("shelf-49-it").await;
    let origin = state.origin.clone();

    // Generous wait window so all three followers are in the group
    // when the dispatcher drains. 5 ms is comfortably above the
    // tokio scheduler tick + the time it takes to spawn three
    // tasks; the production default is 200 µs.
    let cfg = CoalesceConfig {
        enabled: true,
        max_gap_bytes: 64 * 1024,
        max_coalesced_bytes: 1024 * 1024,
        wait_window_micros: 5_000,
        consecutive_failures: 5,
        cool_off: Duration::from_secs(30),
    };
    let fetcher: Arc<dyn shelfd::coalesce::RangeFetcher> = Arc::new(S3OriginFetcher::new(origin));
    let coalescer = Coalescer::new(cfg, fetcher);

    // Read object's ETag — the dispatcher keys on `(bucket, key,
    // etag)`, and the same triple must be fed to every `fetch` for
    // the three ranges to share a group.
    let head = client
        .head_object()
        .bucket(TEST_BUCKET)
        .key(&key)
        .send()
        .await
        .expect("head_object");
    let etag = head.e_tag().expect("etag present").to_owned();

    let before_origin = origin_get_range_ok_count(TEST_BUCKET);
    let before_coalesced = coalesced_outcome_count();

    let f1 = {
        let c = coalescer.clone();
        let etag = etag.clone();
        let key = key.clone();
        tokio::spawn(async move { c.fetch(TEST_BUCKET, &key, &etag, 0, 4_096).await })
    };
    let f2 = {
        let c = coalescer.clone();
        let etag = etag.clone();
        let key = key.clone();
        tokio::spawn(async move { c.fetch(TEST_BUCKET, &key, &etag, 4_096, 4_096).await })
    };
    let f3 = {
        let c = coalescer.clone();
        let etag = etag.clone();
        let key = key.clone();
        tokio::spawn(async move { c.fetch(TEST_BUCKET, &key, &etag, 8_192, 4_096).await })
    };

    let r1 = f1.await.expect("join f1").expect("fetch f1");
    let r2 = f2.await.expect("join f2").expect("fetch f2");
    let r3 = f3.await.expect("join f3").expect("fetch f3");

    // Byte-identity per slice — the dispatcher must hand each
    // requester *its* offset/length, not the merged buffer.
    assert_eq!(r1.len(), 4_096);
    assert_eq!(r2.len(), 4_096);
    assert_eq!(r3.len(), 4_096);
    for (off, slice) in [(0u64, &r1), (4_096, &r2), (8_192, &r3)] {
        for (i, b) in slice.iter().enumerate() {
            let want = ((off + i as u64) & 0xff) as u8;
            assert_eq!(*b, want, "mismatch at offset {}", off + i as u64);
        }
    }

    let after_origin = origin_get_range_ok_count(TEST_BUCKET);
    let after_coalesced = coalesced_outcome_count();

    assert_eq!(
        after_origin - before_origin,
        1,
        "expected exactly one origin GET to land for three coalesced ranges; \
         got delta = {}",
        after_origin - before_origin,
    );
    assert_eq!(
        after_coalesced - before_coalesced,
        3,
        "expected three input ranges to be labelled `coalesced`; got delta = {}",
        after_coalesced - before_coalesced,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_coalescer_is_a_pass_through() {
    if skip_if_offline() {
        return;
    }

    let client = s3_client().await;
    ensure_bucket(&client).await;
    let mut buf = Vec::with_capacity(64 * 1024);
    for i in 0..buf.capacity() {
        buf.push((i & 0xff) as u8);
    }
    let key = format!("shelf-49-disabled-{}.bin", std::process::id());
    put_object(&client, &key, Bytes::from(buf.clone())).await;

    let state = build_state_with_pod_id("shelf-49-it-disabled").await;
    let origin = state.origin.clone();

    let cfg = CoalesceConfig {
        enabled: false,
        ..CoalesceConfig::default()
    };
    let fetcher: Arc<dyn shelfd::coalesce::RangeFetcher> = Arc::new(S3OriginFetcher::new(origin));
    let coalescer = Coalescer::new(cfg, fetcher);

    let head = client
        .head_object()
        .bucket(TEST_BUCKET)
        .key(&key)
        .send()
        .await
        .expect("head_object");
    let etag = head.e_tag().expect("etag present").to_owned();

    let before_origin = origin_get_range_ok_count(TEST_BUCKET);
    let before_coalesced = coalesced_outcome_count();

    // Two concurrent adjacent ranges that *would* coalesce if the
    // dispatcher were active. With `enabled = false` each must
    // round-trip to origin independently and `outcome=coalesced`
    // must NOT increment.
    let f1 = {
        let c = coalescer.clone();
        let etag = etag.clone();
        let key = key.clone();
        tokio::spawn(async move { c.fetch(TEST_BUCKET, &key, &etag, 0, 1024).await })
    };
    let f2 = {
        let c = coalescer.clone();
        let etag = etag.clone();
        let key = key.clone();
        tokio::spawn(async move { c.fetch(TEST_BUCKET, &key, &etag, 1024, 1024).await })
    };
    let _ = f1.await.expect("join").expect("f1");
    let _ = f2.await.expect("join").expect("f2");

    let after_origin = origin_get_range_ok_count(TEST_BUCKET);
    let after_coalesced = coalesced_outcome_count();

    assert_eq!(
        after_origin - before_origin,
        2,
        "disabled coalescer must hit origin once per requester; got {}",
        after_origin - before_origin,
    );
    assert_eq!(
        after_coalesced - before_coalesced,
        0,
        "disabled coalescer must not bump outcome=coalesced; got {}",
        after_coalesced - before_coalesced,
    );
}
