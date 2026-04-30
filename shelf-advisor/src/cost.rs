//! Minimal `Cents` newtype — local placeholder for SHELF-40's
//! `crates/shelf-cost::Cents` while PR #68 is still open.
//!
//! Why duplicate the type rather than depend on the open PR's crate:
//! `feat/shelf-40-dollars-saved-counter` is not yet on `origin/main`,
//! so taking it as a workspace dep here would force a hard pre-merge
//! ordering and rebase ratchet between SHELF-52 and SHELF-40. Once
//! PR #68 lands, the SHELF-52 follow-up swap is mechanical:
//!
//! ```text
//!   - use crate::cost::Cents;
//!   + use shelf_cost::Cents;
//! ```
//!
//! The shape, semantics, and serde representation here are
//! deliberately compatible with that target.
//!
//! Unit and tariff notes
//! ---------------------
//!
//! `Cents` is integer cents (USD); fractional cents round down via
//! `from_bytes_rewrite`. The `S3_REWRITE_TARIFF_CENTS_PER_GIB`
//! constant is a deliberately rough proxy for the per-GiB cost of
//! a full table rewrite (Trino workers + S3 PUT/GET request charges
//! amortised over an ap-south-1 ~128 MiB Iceberg parquet output
//! file size). It is exposed so operators / tests can override it
//! and so the design note can document the placeholder explicitly.

use serde::{Deserialize, Serialize};

/// Default tariff for per-GiB rewrite cost in integer cents.
///
/// 4 cents/GiB is a placeholder anchored on the request-charge math
/// below; it is **not** a measured number and is documented as such
/// in `docs/design-notes/SHELF-52-bloom-write-advisor.md`. Callers
/// can override via `BloomWriteConfig::cost_cents_per_gib` once the
/// real `shelf_cost::Cents` lands. Numbers anchored to AGENTS.md
/// "S3 cost" section: AWS lists `$0.0004/1k GETs` and
/// `$0.005/1k PUTs`; at ~128 MiB per Iceberg file an 8 GiB rewrite
/// fan-out is ~64 file-pairs (read + write) — request charges fit
/// comfortably under 0.5 cents per GiB. The remainder is a
/// conservative compute-amortisation pad pending SHELF-40 wiring.
pub const S3_REWRITE_TARIFF_CENTS_PER_GIB: u64 = 4;

/// Bytes per GiB.
pub const GIB: u64 = 1024 * 1024 * 1024;

/// Integer-cent newtype.
///
/// Comparable, hashable, total-ordered, serde-roundtrippable. The
/// operator-facing JSON shape is a bare integer (`"cost_cents": 42`)
/// to match the SHELF-40 / shelf_cost design.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Cents(pub u64);

impl Cents {
    /// Compute rewrite cost (read + write) in cents from a total
    /// rewrite-byte budget. Uses [`S3_REWRITE_TARIFF_CENTS_PER_GIB`]
    /// unless an explicit `tariff_cents_per_gib` is supplied.
    ///
    /// `total_rewrite_bytes` is expected to be `2 * table_total_bytes`
    /// (read + write); callers compute that themselves so the unit
    /// stays explicit at the call site.
    pub fn from_bytes_rewrite(total_rewrite_bytes: u64, tariff_cents_per_gib: u64) -> Self {
        let gib = total_rewrite_bytes / GIB;
        Cents(gib.saturating_mul(tariff_cents_per_gib))
    }

    /// Returns the raw integer cents value.
    pub fn as_cents(&self) -> u64 {
        self.0
    }

    /// Render as `$X.XX` for human-facing reports.
    pub fn fmt_dollars(&self) -> String {
        format!("${}.{:02}", self.0 / 100, self.0 % 100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cents_default_is_zero() {
        assert_eq!(Cents::default(), Cents(0));
    }

    #[test]
    fn from_bytes_rewrite_default_tariff() {
        // 1 GiB rewrite at 4 cents/GiB tariff → 4 cents.
        assert_eq!(
            Cents::from_bytes_rewrite(GIB, S3_REWRITE_TARIFF_CENTS_PER_GIB),
            Cents(4)
        );
        // 2 TiB rewrite (~2048 GiB) at 4 cents/GiB → 8192 cents = $81.92.
        assert_eq!(
            Cents::from_bytes_rewrite(2 * 1024 * GIB, S3_REWRITE_TARIFF_CENTS_PER_GIB),
            Cents(2 * 1024 * 4)
        );
    }

    #[test]
    fn from_bytes_rewrite_floors_sub_gib() {
        // 100 MiB < 1 GiB → 0 cents (integer floor; explicitly honest).
        assert_eq!(
            Cents::from_bytes_rewrite(100 * 1024 * 1024, S3_REWRITE_TARIFF_CENTS_PER_GIB),
            Cents(0)
        );
    }

    #[test]
    fn from_bytes_rewrite_saturates_on_overflow() {
        // u64::MAX bytes × 4 cents/GiB must not panic.
        let _ = Cents::from_bytes_rewrite(u64::MAX, S3_REWRITE_TARIFF_CENTS_PER_GIB);
    }

    #[test]
    fn fmt_dollars_zero_pads_pennies() {
        assert_eq!(Cents(5).fmt_dollars(), "$0.05");
        assert_eq!(Cents(100).fmt_dollars(), "$1.00");
        assert_eq!(Cents(1234).fmt_dollars(), "$12.34");
    }

    #[test]
    fn cents_is_serde_transparent() {
        let json = serde_json::to_string(&Cents(42)).expect("ser");
        assert_eq!(json, "42");
        let back: Cents = serde_json::from_str("42").expect("de");
        assert_eq!(back, Cents(42));
    }
}
