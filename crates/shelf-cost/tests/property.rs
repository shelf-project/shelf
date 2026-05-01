//! Property tests for the SHELF-40 cost model.
//!
//! Two invariants the design note pinned as guardrails against
//! future drift:
//!
//! 1. **Additive identity (no float drift).** For any sequence of
//!    N hits, summing per-hit `dollars_saved` matches feeding the
//!    same byte-and-flag stream through the model and summing the
//!    result. `Cents` is integer, so this should be _exactly_
//!    equal — there is no float in the formula. A drift here
//!    means someone introduced an `f64` in the hot path; fail
//!    early.
//!
//! 2. **Validation rejects negative coefficients.** A YAML typo
//!    (`-40` for the GET cost) must not register a counter that
//!    accumulates in the wrong direction.
//!
//! `proptest` was chosen over `quickcheck` because the rest of the
//! workspace has no shrinking-helper dep and `proptest`'s test
//! cases shrink without manual `Arbitrary` boilerplate.

use proptest::prelude::*;
use shelf_cost::{CostConfig, CostConfigError, CostModel, HitEvent, PeerAz};

fn arb_event() -> impl Strategy<Value = HitEvent> {
    // Bytes capped at 4 GiB so a single hit can't overflow into the
    // i128 saturation path; that path is exercised separately in
    // the unit tests.
    let bytes = 0u64..=(4u64 * 1024 * 1024 * 1024);
    let az = prop_oneof![Just(PeerAz::SameAz), Just(PeerAz::CrossAz)];
    (bytes, az, 0u8..3u8).prop_map(|(b, az, kind)| match kind {
        0 => HitEvent::Memory {
            bytes_returned: b,
            peer_az: az,
        },
        1 => HitEvent::Disk {
            bytes_returned: b,
            peer_az: az,
        },
        _ => HitEvent::Peer {
            bytes_returned: b,
            peer_az: az,
        },
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// Sequence of N hits accumulates *exactly* — no rounding drift
    /// across iterations.
    #[test]
    fn sum_of_individual_equals_aggregate(events in proptest::collection::vec(arb_event(), 1..128)) {
        let model = CostModel::for_region("us-east-1").unwrap();
        let individually: i128 = events
            .iter()
            .map(|e| model.dollars_saved(*e).as_cents_i64() as i128)
            .sum();
        // Aggregate via Rust `Sum` impl on `Cents` to also exercise
        // the operator overloads.
        let aggregate: shelf_cost::Cents = events.iter().map(|e| model.dollars_saved(*e)).sum();
        prop_assert_eq!(aggregate.as_cents_i64() as i128, individually);
    }

    /// `dollars_saved` is non-negative on a non-negative coefficient
    /// table — no operator config can flip the sign on the integer
    /// counter without `validate()` having already returned an error.
    #[test]
    fn dollars_saved_is_non_negative(event in arb_event()) {
        let model = CostModel::for_region("ap-south-1").unwrap();
        let saved = model.dollars_saved(event);
        prop_assert!(saved.as_cents_i64() >= 0, "negative cents on non-negative model: {saved}");
    }

    /// Cross-AZ hits cost *at least as much as* same-AZ hits for any
    /// non-zero byte count on a region whose cross-AZ coefficient
    /// is non-zero (both presets are). Sub-GiB hits may still
    /// floor to the same whole-cent number (cross_az = 1_000_000
    /// µ¢/GiB is just barely enough to clear 1 cent at 1 GiB), so
    /// we assert `>=`, not `>`.
    #[test]
    fn cross_az_costs_more_than_same_az(bytes in (16u64 * 1024 * 1024)..(4u64 * 1024 * 1024 * 1024)) {
        let model = CostModel::for_region("us-east-1").unwrap();
        let same = model.dollars_saved(HitEvent::Memory {
            bytes_returned: bytes,
            peer_az: PeerAz::SameAz,
        });
        let cross = model.dollars_saved(HitEvent::Memory {
            bytes_returned: bytes,
            peer_az: PeerAz::CrossAz,
        });
        prop_assert!(cross >= same, "cross-AZ should cost at least as much as same-AZ");
    }

    /// A negative coefficient passed via [`CostConfig`] **never**
    /// builds a model — the loader rejects it before the counter
    /// is ever touched.
    #[test]
    fn negative_coefficient_via_config_is_rejected(
        get_cost in i64::MIN..=-1i64,
    ) {
        let cfg = CostConfig {
            enabled: true,
            region: "us-east-1".to_owned(),
            get_request_micro_cents: Some(get_cost),
            same_az_micro_cents_per_gib: None,
            cross_az_micro_cents_per_gib: None,
            nat_processing_micro_cents_per_gib: None,
            nat_traversal_basis_points: None,
            amortized_dollars_per_hour: None,
        };
        let err = cfg.into_model().unwrap_err();
        prop_assert!(matches!(err, CostConfigError::NegativeCoefficient));
    }
}
