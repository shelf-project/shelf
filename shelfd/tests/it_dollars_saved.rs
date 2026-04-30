//! SHELF-40 integration test — `shelf_s3_dollars_saved_total`
//! advances by the **exact** cents predicted by `shelf-cost`'s
//! formula after a deterministic sequence of cached reads through
//! the S3 shim.
//!
//! Gated on `SHELF_INTEGRATION=1` like every other suite under
//! `shelfd/tests/`. The MinIO fixture is shared with `it_s3_shim.rs`
//! / `it_read_path.rs` to keep the docker-compose surface narrow.
//!
//! ## What this asserts
//!
//! 1. Cold reads bump *no* dollars-saved (cache miss path doesn't
//!    enter the `Hit` arm).
//! 2. Warm reads bump
//!    `shelf_s3_dollars_saved_total{region, outcome="hit_memory"}`
//!    by **exactly** `WARM_READS × per_hit_cents`, where
//!    `per_hit_cents` is computed independently in this test from
//!    the same `shelf_cost::CostModel::dollars_saved` formula.
//!
//! ## Why an exact-equality assertion (not a band)
//!
//! The dollar formula is integer-cents fixed-point. There is no
//! float, no clock, no thread-scheduling variance that could
//! cause two runs to disagree on the cents-saved by N hits of
//! known size — so the test pins to the **exact** cents.
//! Approximate-equality bands would mask unit confusions
//! (cents-vs-microcents, GiB-vs-GB) the pin is explicitly there
//! to catch.
//!
//! ## How we force a strictly-positive `per_hit_cents`
//!
//! The shim wires every hit as `PeerAz::SameAz` (see
//! `shelfd::cost::DEFAULT_PEER_AZ`), and same-AZ data transfer is
//! free in both us-east-1 and ap-south-1, so a vanilla 64 KiB
//! same-AZ memory hit only contributes 40 µ¢ of "GET avoided" —
//! which floors to **0 cents** per hit and is unobservable.
//! To make the wiring observable we override `same_az` to a high
//! enough rate (`100_000_000 µ¢/GiB` ≈ $1/GiB — clearly synthetic,
//! NOT a real-world coefficient) so a 1 MiB hit contributes
//! `1 MiB × 100_000_000 µ¢/GiB ÷ 2^30 ÷ 1_000_000 = 0` cents per
//! hit too — drat. The bus that makes a 1 MiB hit visible is the
//! NAT term: with `nat_traversal_basis_points = 10_000` and the
//! preset `nat_processing_micro_cents_per_gib = 4_500_000` the
//! formula gives:
//!
//!   `1 MiB × 4_500_000 µ¢/GiB × 10000 / 10000 / 2^30 / 1_000_000`
//!   = `4 µ¢` of NAT spend per 1 MiB hit — still floors to 0.
//!
//! So we deliberately **override** the NAT coefficient to a
//! synthetic value (`4_500_000_000 µ¢/GiB`, ~1000× the real
//! $0.045/GiB rate) for this test. With 1 MiB body and 100 % NAT
//! traversal that is exactly **4 ¢ per hit**. 100 hits ⇒ exactly
//! 400 ¢ delta, asserted exactly. The override is clearly marked
//! as a SHELF-40-test fixture, NOT a published rate, so refresh
//! reviews don't mistake it for a real coefficient.

#![cfg(test)]

mod common;

use bytes::Bytes;
use common::{
    build_state_with_pod_id_and_cost, ensure_bucket, put_object, s3_client, skip_if_offline,
    spawn_server_with_shim, TEST_BUCKET,
};
use shelf_cost::{CostConfig, CostModel, HitEvent};

/// Number of warm reads. 100 is enough to make the assertion
/// robust against any one warm-up tick and well within the
/// MinIO fixture's body-size budget.
const WARM_READS: usize = 100;

/// Body size driven through the shim. 4 MiB chosen because
///   `bytes × nat_rate_µ¢_per_GiB / 2^30`
/// must clear the µ¢ → ¢ truncation step on a single hit. With
/// the synthetic NAT rate below (`2^30` µ¢/GiB), a 4 MiB body
/// produces exactly 4 ¢/hit. 4 MiB also stays under the 8 MiB
/// admission threshold in `common::test_config()` so the body
/// lands in the metadata pool ⇒ memory hit (= `outcome=hit_memory`).
const BODY_LEN: usize = 4 * (1 << 20);

/// Synthetic NAT-processing rate used **only** by this test, in
/// µ¢/GiB. The real us-east-1 coefficient is `4_500_000` µ¢/GiB
/// ($0.045/GiB); we override to `2^30` so the per-hit math has
/// no fractional cents and the assertion can be exact:
///
///   `4 MiB × 2^30 µ¢/GiB / 2^30 = 4 × 2^20 µ¢ = 4_194_304 µ¢`
///   → `4_194_304 / 1_000_000 = 4` cents (truncated).
///
/// 100 hits × 4 cents = 400 cents ≡ $4 — that's the exact
/// assertion. This synthetic value is NOT a published AWS price;
/// don't copy-paste it into a values-overlay.
const SYNTHETIC_NAT_RATE_PER_GIB: i64 = 1_073_741_824;

#[tokio::test]
async fn s3_shim_warm_reads_bump_dollars_saved_counter_by_expected_cents() {
    if skip_if_offline() {
        return;
    }

    let body = Bytes::from(vec![0xAB; BODY_LEN]);
    let key = "shelf-40-dollars-fixture";

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;
    put_object(&s3, key, body.clone()).await;

    // Force a strictly-positive whole-cent per-hit contribution
    // by overriding the NAT term to 100 %. The override path
    // exercises exactly the same `CostConfig` codepath operators
    // use in `cache.cost.*`, so this test is also a regression
    // pin for the override loader.
    let cfg = CostConfig {
        enabled: true,
        region: "us-east-1".to_owned(),
        nat_processing_micro_cents_per_gib: Some(SYNTHETIC_NAT_RATE_PER_GIB),
        nat_traversal_basis_points: Some(10_000), // 100 % NAT traversal
        ..CostConfig::default()
    };
    let cost = shelfd::cost::CostState::from_config(&cfg).expect("cost state");
    let region = cost.region().to_owned();
    let state = build_state_with_pod_id_and_cost("shelf-40-dollars", cost.clone()).await;
    let (_native, shim, _cancel) = spawn_server_with_shim(state.clone()).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();

    // First read warms the cache (miss path — no dollars-saved).
    let resp = http.get(&url).send().await.expect("get cold");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Snapshot the counter *before* the warm reads.
    let before = shelfd::metrics::S3_DOLLARS_SAVED_TOTAL
        .with_label_values(&[region.as_str(), "hit_memory"])
        .get();

    // Drive WARM_READS warm reads. Every one is a memory hit
    // (the metadata pool absorbs anything ≤ admission threshold;
    // see `common::test_config()` — 8 MiB threshold ⇒ a 1 MiB
    // body lands in DRAM).
    for _ in 0..WARM_READS {
        let resp = http.get(&url).send().await.expect("get warm");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    let after = shelfd::metrics::S3_DOLLARS_SAVED_TOTAL
        .with_label_values(&[region.as_str(), "hit_memory"])
        .get();
    let observed_delta = after - before;

    // Compute the *exact* expected delta from the public formula.
    // The shim wires every hit as `peer_az=SameAz` via
    // `shelfd::cost::DEFAULT_PEER_AZ`, so we use that for the
    // independent calculation here too.
    let model = CostModel::from_config(&cfg).unwrap();
    let per_hit = model
        .dollars_saved(HitEvent::Memory {
            bytes_returned: BODY_LEN as u64,
            peer_az: shelfd::cost::DEFAULT_PEER_AZ,
        })
        .as_cents_u64();
    let expected_delta = (WARM_READS as u64) * per_hit;

    assert!(
        per_hit > 0,
        "fixture mis-sized: per_hit_cents={per_hit}; \
         the test cannot detect wiring drift if per-hit cents floors to 0",
    );
    assert_eq!(
        observed_delta, expected_delta,
        "shelf_s3_dollars_saved_total{{outcome=hit_memory}} drifted: \
         observed_delta={observed_delta}, expected_delta={expected_delta}, \
         per_hit={per_hit}, warm_reads={WARM_READS}",
    );
}
