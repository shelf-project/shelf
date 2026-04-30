//! Region-specific cost coefficients.
//!
//! Every constant in this file MUST cite the AWS pricing page it
//! came from and the date the value was captured. The sibling
//! `crates/shelf-cost/README.md` carries the refresh runbook; CI
//! does not yet auto-refresh these (tracked as a follow-up).
//!
//! ## Coefficient unit conventions
//!
//! - **GET requests**: stored as **micro-cents per request**
//!   (`µ¢/req`). One cent = `1_000_000 µ¢`. AWS publishes the rate
//!   as `$0.0004 per 1k requests` ≡ `0.00004 ¢/req` ≡ `40 µ¢/req`.
//! - **Data transfer**: stored as **micro-cents per GiB**
//!   (`µ¢/GiB`, GiB = 2^30 bytes). AWS publishes data-transfer
//!   rates per GiB, so storing `µ¢/GiB` lets us record
//!   `$0.01/GiB = 1 000 000 µ¢/GiB` exactly, with **zero**
//!   rounding residue. `dollars_saved` then divides by `2^30`
//!   inside i128 arithmetic to produce the per-event amount —
//!   see [`crate::CostModel::dollars_saved`].
//!
//!   *Earlier revisions stored `µ¢/byte`. That representation
//!   could not hold $0.01/GiB without rounding 0.000_931 µ¢/byte
//!   either to `0` (under-charge by 100 %) or to `1` (over-charge
//!   by 1 075×). We rejected both and moved to `µ¢/GiB`.*
//!
//! ## Why µ¢ as the storage unit?
//!
//! Both currency *and* the underlying byte/request granularity have
//! to round-trip without losing precision. The AWS GET-request
//! price (`40 µ¢/req`) cannot be represented exactly in cents
//! without going below the integer floor; storing as µ¢ keeps
//! everything in fixed-point and matches the contract in
//! [`crate::CostModel::dollars_saved`].

use crate::CostModel;
use std::sync::OnceLock;

/// `us-east-1` (N. Virginia).
///
/// Source: https://aws.amazon.com/s3/pricing/  (Standard Tier 1
///                                                requests)
/// Source: https://aws.amazon.com/ec2/pricing/on-demand/
///                                                (Data Transfer)
/// AS_OF: 2026-04 (refreshed quarterly per `crates/shelf-cost/README.md`).
///
/// - GET, Standard, Tier-1: $0.0004 per 1 000 requests.
///   = 0.00004 ¢/req
///   = **40 µ¢/req**.
/// - Cross-AZ data transfer (same region): $0.01/GiB
///   = 1 ¢/GiB
///   = **1 000 000 µ¢/GiB** (exact; zero rounding residue).
/// - NAT-gateway data processing: $0.045/GiB
///   = 4.5 ¢/GiB
///   = **4 500 000 µ¢/GiB** (exact). Only billed when
///   `nat_traversal_basis_points > 0` is explicitly set by the
///   operator.
/// - Same-AZ data transfer: $0.00/GiB ⇒ **0 µ¢/GiB**.
pub static US_EAST_1_PRESET: OnceLock<CostModel> = OnceLock::new();

#[allow(non_upper_case_globals)] // `static ref`-style accessor; clippy false positive.
pub fn us_east_1() -> &'static CostModel {
    US_EAST_1_PRESET.get_or_init(|| CostModel {
        region_id: "us-east-1".to_owned(),
        get_request_micro_cents: 40,
        same_az_micro_cents_per_gib: 0,
        cross_az_micro_cents_per_gib: 1_000_000,
        nat_processing_micro_cents_per_gib: 4_500_000,
        nat_traversal_basis_points: 0,
    })
}

/// `ap-south-1` (Mumbai).
///
/// Source: https://aws.amazon.com/s3/pricing/  (Mumbai is in the
///                                                same Tier-1 GET
///                                                price band as
///                                                us-east-1.)
/// Source: https://aws.amazon.com/ec2/pricing/on-demand/  (Data
///                                                Transfer; the
///                                                cross-AZ rate is
///                                                identical for
///                                                this region.)
/// Source: https://aws.amazon.com/vpc/pricing/  (NAT-gateway data
///                                                processing in
///                                                Asia Pacific
///                                                (Mumbai).)
/// AS_OF: 2026-04 (refreshed quarterly per `crates/shelf-cost/README.md`).
///
/// - GET, Standard, Tier-1: $0.0004 per 1 000 requests
///   = **40 µ¢/req** (same as us-east-1).
/// - Cross-AZ data transfer: $0.01/GiB
///   = **1 000 000 µ¢/GiB** (exact; zero rounding residue).
/// - NAT-gateway data processing: $0.045/GiB
///   = **4 500 000 µ¢/GiB** (exact).
/// - Same-AZ data transfer: $0.00/GiB ⇒ **0 µ¢/GiB**.
///
/// Mumbai EC2 list prices for the actual instance hours (e.g.
/// `m6a.4xlarge` $0.864/hr) are **not** modelled here — that
/// belongs to the SHELF-61 amortisation followup, which lives in
/// the operator-specific values overlay under
/// `infra/<operator>/charts/shelf/values-prod.yaml`.
pub static AP_SOUTH_1_PRESET: OnceLock<CostModel> = OnceLock::new();

pub fn ap_south_1() -> &'static CostModel {
    AP_SOUTH_1_PRESET.get_or_init(|| CostModel {
        region_id: "ap-south-1".to_owned(),
        get_request_micro_cents: 40,
        same_az_micro_cents_per_gib: 0,
        cross_az_micro_cents_per_gib: 1_000_000,
        nat_processing_micro_cents_per_gib: 4_500_000,
        nat_traversal_basis_points: 0,
    })
}

// Backwards-compat aliases — modern call sites can use the constant
// names directly via `coefficients::US_EAST_1.materialise()` for
// readability.
//
// We expose them as functions wrapped in `Lazy`-style accessors so
// the CostModel's `String region_id` doesn't have to be allocated
// every time the const is referenced; cloning out is the path
// `CostModel::for_region` uses.
pub struct StaticPreset {
    inner: fn() -> &'static CostModel,
}

impl StaticPreset {
    /// Materialise the static preset into an owned `CostModel`.
    /// Named `materialise` rather than `clone` because the latter
    /// is reserved for the standard trait (which we cannot
    /// implement on a `static` of a `pub`-but-non-`Clone`-friendly
    /// shape without exposing the internal function pointer).
    pub fn materialise(&self) -> CostModel {
        (self.inner)().clone()
    }

    /// Borrow the underlying preset without allocating.
    pub fn get(&self) -> &'static CostModel {
        (self.inner)()
    }
}

impl std::fmt::Debug for StaticPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StaticPreset")
            .field(&(self.inner)())
            .finish()
    }
}

#[allow(non_upper_case_globals)]
pub static US_EAST_1: StaticPreset = StaticPreset { inner: us_east_1 };
#[allow(non_upper_case_globals)]
pub static AP_SOUTH_1: StaticPreset = StaticPreset { inner: ap_south_1 };

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn us_east_1_constants_match_published_prices() {
        let m = us_east_1();
        // $0.0004 / 1k requests.
        assert_eq!(m.get_request_micro_cents, 40);
        // $0.01/GiB ⇒ 1 000 000 µ¢/GiB exact.
        assert_eq!(m.cross_az_micro_cents_per_gib, 1_000_000);
        // $0.00/GiB.
        assert_eq!(m.same_az_micro_cents_per_gib, 0);
        // $0.045/GiB ⇒ 4 500 000 µ¢/GiB exact.
        assert_eq!(m.nat_processing_micro_cents_per_gib, 4_500_000);
        // NAT term off by default.
        assert_eq!(m.nat_traversal_basis_points, 0);
    }

    #[test]
    fn ap_south_1_constants_match_published_prices() {
        let m = ap_south_1();
        assert_eq!(m.get_request_micro_cents, 40);
        assert_eq!(m.cross_az_micro_cents_per_gib, 1_000_000);
        assert_eq!(m.same_az_micro_cents_per_gib, 0);
        assert_eq!(m.nat_processing_micro_cents_per_gib, 4_500_000);
        assert_eq!(m.nat_traversal_basis_points, 0);
    }

    #[test]
    fn presets_validate_clean() {
        us_east_1().validate().expect("preset is valid");
        ap_south_1().validate().expect("preset is valid");
    }

    #[test]
    fn static_alias_materialises_to_owned_model() {
        let m: CostModel = US_EAST_1.materialise();
        assert_eq!(m.region_id, "us-east-1");
        let m: CostModel = AP_SOUTH_1.materialise();
        assert_eq!(m.region_id, "ap-south-1");
    }
}
