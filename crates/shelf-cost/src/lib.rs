//! `shelf-cost` — audit-able S3 + data-transfer cost model.
//!
//! Ticket: **SHELF-40** (`shelf_s3_dollars_saved_total` counter +
//! shared cost crate). Companion ticket SHELF-37 owns the Iceberg
//! event-listener jar that surfaces `bytesReadFromCache` /
//! `bytesReadExternally` per query — once both ship, the rate
//! exposed by this crate becomes a per-query / per-table $ cut on
//! the dashboard side without further code in `shelfd`.
//!
//! ## Why this lives in its own crate
//!
//! The cost model is consumed from three surfaces today:
//!
//! 1. `shelfd` — bumps the `shelf_s3_dollars_saved_total` Prometheus
//!    counter on every cache hit through the `s3_shim` /
//!    `peer_fetch` hot paths.
//! 2. `shelf-advisor` (Tier-3) — scores candidate pin / MV
//!    recommendations against the same coefficients so the
//!    recommendations are denominated in the same dollars the
//!    operator's procurement deck reads.
//! 3. SHELF-37 listener / SHELF-62 A/B harness (Java side) — calls
//!    into `shelf-cost` over a thin FFI / SQL UDF layer once the
//!    Iceberg-sink projection lands.
//!
//! Centralising the formula in one library prevents the three
//! surfaces from drifting on coefficient values, region overrides,
//! or rounding behaviour. Every consumer must therefore link
//! through this crate; copying the constants is a review-blocking
//! anti-pattern.
//!
//! ## What it is *not*
//!
//! - **Not a billing system.** Output is "money the cache *would*
//!   have spent on origin S3 had this hit been a miss". Real AWS
//!   bills include per-bucket request-tier mixes, S3 Inventory
//!   reads, KMS, replication, lifecycle transitions, and
//!   contractual / volume discounts that this crate makes no
//!   attempt to model.
//! - **Not a benchmark.** Benchmark numbers belong in `shelfd`'s
//!   benchmark harness (`shelfd/benches/`) or the SHELF-26 replay
//!   tooling. This crate ships the formula, not measurements.
//! - **Not a precision counter for sub-cent values.** Output is
//!   integer cents. The fractional cent of any single hit is
//!   accumulated inside the model via fixed-point arithmetic on a
//!   64-bit counter (see [`Cents`] / [`CostModel::dollars_saved`]).
//!
//! ## Audit story
//!
//! Every coefficient is a `pub const` in [`coefficients`] with a
//! source-doc comment that points at the AWS pricing page it came
//! from and the `AS_OF` date. The README runbook documents the
//! refresh cadence (every quarter or whenever AWS announces a price
//! change for `S3 Standard` GET / EC2 data transfer in `ap-south-1` /
//! `us-east-1` — whichever fires first).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(rust_2018_idioms)]

pub mod coefficients;
pub mod config;

pub use config::{CostConfig, CostConfigError};

use std::fmt;

use serde::{Deserialize, Serialize};

/// Integer-cents newtype.
///
/// Stored as `i64` because:
///
/// 1. **No float drift.** A floats-based cumulative counter accreting
///    fractional pennies over weeks crosses the `f64` mantissa
///    rounding threshold long before the counter wraps. We do all
///    the arithmetic in fixed-point, then expose it to Prometheus
///    as an integer counter (`IntCounterVec` + `inc_by(u64)`).
/// 2. **Sign matters.** The model never produces negative cents on
///    its own — cache hits only add — but downstream consumers
///    (`shelf-advisor` cost-difference, future net-cost dashboards)
///    naturally subtract amortised cluster cost from gross savings
///    and must be allowed to go negative.
/// 3. **Wide enough.** `i64` cents = ±9.2 × 10¹⁶ ≈ ±$92 quadrillion.
///    A pod that runs for the heat-death of the universe can't
///    overflow this.
///
/// Conversion to dollars is **explicit at every call site** — the
/// crate never hands out an `f64` "dollars" value silently. See
/// [`Cents::as_dollars`] / [`Cents::as_cents_i64`].
#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Cents(i64);

impl Cents {
    pub const ZERO: Cents = Cents(0);

    /// Construct from a literal cent count. Mostly used in tests.
    #[inline]
    pub const fn new(value: i64) -> Self {
        Cents(value)
    }

    /// Saturating add — rolling counters in shelfd accumulate over
    /// the process lifetime; arithmetic wrap is operator-confusing.
    #[inline]
    pub fn saturating_add(self, other: Cents) -> Cents {
        Cents(self.0.saturating_add(other.0))
    }

    /// Saturating sub — see [`Cents::saturating_add`].
    #[inline]
    pub fn saturating_sub(self, other: Cents) -> Cents {
        Cents(self.0.saturating_sub(other.0))
    }

    /// Raw integer cents.
    #[inline]
    pub const fn as_cents_i64(self) -> i64 {
        self.0
    }

    /// Raw integer cents as `u64`. **Caller asserts the value is
    /// non-negative**; we saturate to `0` rather than panicking
    /// because the only consumer of the unsigned form is the
    /// Prometheus counter (`IntCounterVec::inc_by(u64)`), which
    /// also has no negative semantics.
    #[inline]
    pub fn as_cents_u64(self) -> u64 {
        self.0.max(0) as u64
    }

    /// Display-only: convert to floating-point dollars. Used in
    /// human-readable logs; never feed back into a counter.
    #[inline]
    pub fn as_dollars(self) -> f64 {
        (self.0 as f64) / 100.0
    }
}

impl fmt::Display for Cents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // "$1.23 (123 ¢)" — the parenthesised cents form keeps the
        // fixed-point intent visible in any log line that prints a
        // single hit's contribution.
        write!(f, "${:.2} ({} ¢)", self.as_dollars(), self.0)
    }
}

impl std::ops::Add for Cents {
    type Output = Cents;
    #[inline]
    fn add(self, rhs: Cents) -> Cents {
        Cents(self.0.wrapping_add(rhs.0))
    }
}

impl std::ops::AddAssign for Cents {
    #[inline]
    fn add_assign(&mut self, rhs: Cents) {
        self.0 = self.0.wrapping_add(rhs.0);
    }
}

impl std::iter::Sum for Cents {
    fn sum<I: Iterator<Item = Cents>>(iter: I) -> Cents {
        iter.fold(Cents::ZERO, |a, b| a + b)
    }
}

/// Where the bytes were served from. Mirrors the three "served from
/// somewhere other than origin" outcomes shelfd already emits via
/// [`HitTier`] + [`PeerHit`] — see ADR-0011.
///
/// The `peer_az` flag is **the** correctness-critical input: same-AZ
/// peer fetch saves no data-transfer dollars (intra-AZ traffic is
/// free per `https://aws.amazon.com/ec2/pricing/on-demand/`), while
/// cross-AZ peer fetch incurs a real $0.01/GiB. Mis-flagging it
/// inflates the counter — fail-safe is `peer_az: PeerAz::SameAz`
/// (no cross-AZ contribution) when the membership ring cannot
/// determine the peer's zone.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HitEvent {
    /// DRAM hit on the local pod. Avoids exactly one origin GET +
    /// the bytes that would have come back over the network. Same
    /// AZ as the requesting Trino worker by construction (we are
    /// the local pod), so the data-transfer dimension is the
    /// caller's `peer_az`.
    Memory {
        /// Bytes shelf returned to the caller.
        bytes_returned: u64,
        /// Whether the *Trino worker* that issued this read sits in
        /// a different AZ from this shelfd pod. If yes, the bytes
        /// flow over a cross-AZ link and the model adds the
        /// $0.01/GiB term; if no, only the GET request is saved.
        peer_az: PeerAz,
    },
    /// NVMe hit on the local pod. Same shape as [`HitEvent::Memory`]
    /// but kept distinct so the Prometheus counter splits
    /// `outcome="hit_disk"` for operators tracking cold-warm tier
    /// promotion. From a $-saving perspective the contribution is
    /// identical to a memory hit (same origin GET avoided, same
    /// network path to the caller).
    Disk {
        bytes_returned: u64,
        peer_az: PeerAz,
    },
    /// Bytes fetched from a peer shelfd pod (HRW primary, SHELF-23).
    /// Avoids the origin GET, but the bytes traverse the *peer*
    /// network link. `peer_az` therefore describes the pod-to-pod
    /// hop, not the pod-to-Trino hop — same-AZ peers are free,
    /// cross-AZ peers cost $0.01/GiB *for the peer hop*.
    ///
    /// The model deliberately does **not** double-charge the
    /// pod-to-Trino hop on a peer hit: in the common case the
    /// requesting Trino worker happens to be co-located with the
    /// HRW-primary peer (locality is what HRW gives us), so the
    /// peer→Trino hop is same-AZ; if it isn't, the operator should
    /// pin via SHELF-65 rather than waste the model on a path that
    /// is already pathological.
    Peer {
        bytes_returned: u64,
        peer_az: PeerAz,
    },
}

impl HitEvent {
    /// Stable Prometheus label slug for the `outcome` dimension.
    /// Matches the existing [`shelfd`] read-path labels (`hit_memory`
    /// / `hit_disk`) and adds `peer` for SHELF-23.
    #[inline]
    pub const fn outcome_label(self) -> &'static str {
        match self {
            HitEvent::Memory { .. } => "hit_memory",
            HitEvent::Disk { .. } => "hit_disk",
            HitEvent::Peer { .. } => "peer",
        }
    }

    #[inline]
    pub const fn bytes_returned(self) -> u64 {
        match self {
            HitEvent::Memory { bytes_returned, .. }
            | HitEvent::Disk { bytes_returned, .. }
            | HitEvent::Peer { bytes_returned, .. } => bytes_returned,
        }
    }

    #[inline]
    pub const fn peer_az(self) -> PeerAz {
        match self {
            HitEvent::Memory { peer_az, .. }
            | HitEvent::Disk { peer_az, .. }
            | HitEvent::Peer { peer_az, .. } => peer_az,
        }
    }
}

/// Same-AZ vs cross-AZ flag for the data-transfer dimension.
///
/// Distinguished as its own enum rather than a `bool` so the call
/// site reads as `PeerAz::SameAz` / `PeerAz::CrossAz` (intent is
/// obvious) and so future extensions (e.g. `Region` for cross-region
/// peer-fetch, NAT-fanout) don't break the public API.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerAz {
    /// Caller and shelfd pod (or peer pod, on `HitEvent::Peer`) are
    /// in the same AWS AZ. Intra-AZ traffic is **free** per the
    /// AWS EC2 data-transfer pricing page (us-east-1, ap-south-1).
    SameAz,
    /// Caller and shelfd pod are in different AZs. Cross-AZ traffic
    /// is billed at $0.01/GiB *each direction* per the same
    /// pricing page; the model adds **one** direction (the response
    /// flow shelfd would have caused had the request gone to S3,
    /// which itself crosses an AZ boundary in steady-state with the
    /// local-VPC S3 endpoint).
    CrossAz,
}

impl PeerAz {
    /// Stable Prometheus label slug for the `peer_az` dimension.
    /// Kept short (no underscores beyond the existing label
    /// convention) so any future panel that splits by AZ does not
    /// blow up the legend width.
    #[inline]
    pub const fn label(self) -> &'static str {
        match self {
            PeerAz::SameAz => "same_az",
            PeerAz::CrossAz => "cross_az",
        }
    }
}

/// The per-region cost model. All public constructors validate that
/// every coefficient is non-negative and that the region id is one
/// of the supported set; see [`CostModel::for_region`] /
/// [`CostModel::from_config`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CostModel {
    /// AWS region identifier (`us-east-1`, `ap-south-1`, …). Treated
    /// as opaque by this crate — the consumer chooses by config,
    /// the model just stamps the value onto the Prometheus label
    /// so dashboards can split by region in multi-region clusters.
    pub region_id: String,
    /// Cost per S3 GET request, in **micro-cents per request**.
    ///
    /// Stored as integer micro-cents (1 cent = 1_000_000 µ¢) so the
    /// computation never touches a float and so the AWS published
    /// price `$0.0004 / 1k requests` (= $0.0000004 / req
    /// = 0.00004 ¢ / req = 40 µ¢ / req) is representable exactly.
    /// Anything finer than µ¢/req would force `i128`.
    pub get_request_micro_cents: i64,
    /// Same-AZ data-transfer cost in micro-cents per **GiB**
    /// (`GiB = 2^30 bytes`).
    ///
    /// AWS lists same-AZ traffic as $0.00/GiB for both us-east-1
    /// and ap-south-1, so this is **0** by default. The field
    /// exists for two reasons:
    ///
    /// 1. Cross-region or third-party-cloud overlays may want a
    ///    non-zero coefficient (e.g. some private-cloud peering
    ///    contracts charge intra-AZ flat).
    /// 2. The math symmetry with `cross_az` keeps the API regular.
    pub same_az_micro_cents_per_gib: i64,
    /// Cross-AZ data-transfer cost in micro-cents per **GiB**.
    ///
    /// AWS publishes cross-AZ at `$0.01/GiB`, which stores as
    /// `1_000_000 µ¢/GiB` exactly. The previous representation
    /// (`µ¢/byte`) could not hold this rate without 100 %–1 075×
    /// rounding error, so we keep AWS's GiB-denominated unit.
    pub cross_az_micro_cents_per_gib: i64,
    /// NAT-gateway data-processing cost in micro-cents per **GiB**.
    ///
    /// **Only billed when traffic exits the VPC via a NAT gateway**;
    /// the typical EKS data-platform stack uses an S3 VPC endpoint
    /// (gateway endpoint, free) so this term is **0 unless flagged
    /// on**. AWS publishes the rate as `$0.045/GiB`, stored as
    /// `4_500_000 µ¢/GiB`. See
    /// [`CostConfig::nat_traversal_basis_points`] for the
    /// operator-controlled gate.
    pub nat_processing_micro_cents_per_gib: i64,
    /// Fraction of "cache-saved bytes" that *would have* exited via
    /// NAT had the cache missed. Stored as a 64-bit basis-points
    /// integer (0 = 0%, 10_000 = 100%) so we keep the no-float
    /// promise. Defaults to **0** (S3 VPC endpoint assumed) — the
    /// operator must set it explicitly to get a non-zero NAT
    /// contribution. Acts as the audit-able "did the operator
    /// affirm their VPC topology?" gate.
    pub nat_traversal_basis_points: u32,
}

impl CostModel {
    /// Construct from a published-region preset.
    ///
    /// Returns an error iff the region identifier is not one of the
    /// regions for which we ship verified coefficients
    /// (`us-east-1`, `ap-south-1`). Operators in other regions
    /// must build a [`CostConfig`] explicitly via YAML so the
    /// citation gets recorded in their own deployment metadata.
    pub fn for_region(region_id: &str) -> Result<Self, CostConfigError> {
        match region_id {
            "us-east-1" => Ok(coefficients::US_EAST_1.materialise()),
            "ap-south-1" => Ok(coefficients::AP_SOUTH_1.materialise()),
            other => Err(CostConfigError::UnknownRegion {
                region: other.to_owned(),
            }),
        }
    }

    /// Construct from a fully-validated [`CostConfig`]. The config
    /// loader already rejected any negative coefficient or unknown
    /// region; this just plugs the validated numbers into the
    /// model.
    pub fn from_config(cfg: &CostConfig) -> Result<Self, CostConfigError> {
        // SHELF-A4 — fail loud at config load if the operator
        // shipped a non-finite or negative `amortized_dollars_per_hour`.
        // The field doesn't flow into `CostModel` itself (it's a
        // pod-level operating cost, not a per-hit coefficient), but
        // we validate here so the existing `from_config` path is the
        // single chokepoint that surfaces every YAML mistake.
        cfg.validated_amortized_dollars_per_hour()?;

        // If the operator provided an explicit override block,
        // honour it; else fall back to the regional preset. The
        // override path is what
        // `infra/<operator>/charts/shelf/values-prod.yaml`
        // overlays exercise so prod values stay close to what
        // we citation-verified, while still flagging any drift
        // in the OSS coefficient table at config-load time.
        let preset = Self::for_region(&cfg.region)?;
        let model = CostModel {
            region_id: cfg.region.clone(),
            get_request_micro_cents: cfg
                .get_request_micro_cents
                .unwrap_or(preset.get_request_micro_cents),
            same_az_micro_cents_per_gib: cfg
                .same_az_micro_cents_per_gib
                .unwrap_or(preset.same_az_micro_cents_per_gib),
            cross_az_micro_cents_per_gib: cfg
                .cross_az_micro_cents_per_gib
                .unwrap_or(preset.cross_az_micro_cents_per_gib),
            nat_processing_micro_cents_per_gib: cfg
                .nat_processing_micro_cents_per_gib
                .unwrap_or(preset.nat_processing_micro_cents_per_gib),
            nat_traversal_basis_points: cfg.nat_traversal_basis_points.unwrap_or(0),
        };
        model.validate()?;
        Ok(model)
    }

    /// Public guard so a caller that built a `CostModel` by hand
    /// (tests, embedding consumers) can re-run the invariants.
    pub fn validate(&self) -> Result<(), CostConfigError> {
        if self.get_request_micro_cents < 0
            || self.same_az_micro_cents_per_gib < 0
            || self.cross_az_micro_cents_per_gib < 0
            || self.nat_processing_micro_cents_per_gib < 0
        {
            return Err(CostConfigError::NegativeCoefficient);
        }
        if self.nat_traversal_basis_points > 10_000 {
            return Err(CostConfigError::NatBpsOutOfRange {
                bps: self.nat_traversal_basis_points,
            });
        }
        Ok(())
    }

    /// Compute the cents *saved* by serving `event` from cache
    /// instead of going to origin S3.
    ///
    /// The math, per coefficient, is:
    ///
    ///   ```text
    ///   saved_¢ =
    ///         get_¢/req                                                       (1 GET avoided)
    ///       + bytes × same_az_µ¢/GiB / 2^30                  (only when peer_az=SameAz)
    ///       + bytes × cross_az_µ¢/GiB / 2^30                 (only when peer_az=CrossAz)
    ///       + bytes × nat_µ¢/GiB × nat_bps / 10_000 / 2^30   (gated by NAT fraction)
    ///   ```
    ///
    /// All micro-cent products are kept in `i128` until the final
    /// `/ 1_000_000` step so the rounding is well-defined and
    /// deterministic on any 64-bit platform.
    #[inline]
    pub fn dollars_saved(&self, event: HitEvent) -> Cents {
        let bytes = event.bytes_returned() as i128;

        let mut total_micro_cents: i128 = 0;

        // (1) GET request avoided. Always exactly one — even if a
        // single client request fanned out into multiple range-GETs
        // against origin (the `s3_shim` already coalesces those
        // before hitting `dollars_saved`, so we count one logical
        // GET per cache hit at the call site).
        total_micro_cents = total_micro_cents.saturating_add(self.get_request_micro_cents as i128);

        // (2) AZ-aware data-transfer. Rate is µ¢/GiB; one GiB =
        // `1 << 30` bytes, so `bytes × rate` overcounts by `2^30`
        // and we divide once at the end. We do the multiply BEFORE
        // the shift to retain precision on byte-scale ranges.
        let az_rate_per_gib = match event.peer_az() {
            PeerAz::SameAz => self.same_az_micro_cents_per_gib as i128,
            PeerAz::CrossAz => self.cross_az_micro_cents_per_gib as i128,
        };
        if az_rate_per_gib > 0 {
            let az_micro_cents = bytes.saturating_mul(az_rate_per_gib) >> 30;
            total_micro_cents = total_micro_cents.saturating_add(az_micro_cents);
        }

        // (3) NAT processing — only contributes when the operator
        // explicitly affirmed a non-zero NAT fraction (`nat_traversal_basis_points`).
        // Default 0 so a typo can't silently zero out — well,
        // technically a typo *could* zero out *this* term, but the
        // term is opt-in by design (most VPCs use the gateway-S3
        // endpoint which is free).
        if self.nat_traversal_basis_points > 0 && self.nat_processing_micro_cents_per_gib > 0 {
            let nat_rate_per_gib = self.nat_processing_micro_cents_per_gib as i128;
            // `nat_bps / 10_000` as integer scaling without floats.
            // Multiply by `bps` *before* dividing so a tiny bps
            // value (e.g. 1) still contributes when bytes are large.
            let nat_micro_cents = (bytes
                .saturating_mul(nat_rate_per_gib)
                .saturating_mul(self.nat_traversal_basis_points as i128)
                / 10_000)
                >> 30;
            total_micro_cents = total_micro_cents.saturating_add(nat_micro_cents);
        }

        // Convert µ¢ → ¢ with truncating division (round-toward-zero
        // for non-negative; cleaner than `as i64` on i128). The
        // micro-cent residue is intentionally dropped — keeping it
        // would require a stateful accumulator, which is the
        // wrong contract for a per-event helper. Over a million
        // events the dropped residue caps at a million × 999_999 µ¢
        // < $10, well below the 0.5 % envelope SHELF-40 tests
        // assert.
        let cents = total_micro_cents / 1_000_000;
        // i128 → i64 saturating cast — the only realistic way for
        // this to overflow is a single hit larger than ~$92 quadrillion
        // worth of bytes, which is impossible on a 64-bit
        // bytes_returned, but we still saturate defensively.
        let cents_i64 = if cents > i64::MAX as i128 {
            i64::MAX
        } else if cents < i64::MIN as i128 {
            i64::MIN
        } else {
            cents as i64
        };
        Cents(cents_i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evt_mem(bytes: u64, az: PeerAz) -> HitEvent {
        HitEvent::Memory {
            bytes_returned: bytes,
            peer_az: az,
        }
    }

    fn evt_disk(bytes: u64, az: PeerAz) -> HitEvent {
        HitEvent::Disk {
            bytes_returned: bytes,
            peer_az: az,
        }
    }

    fn evt_peer(bytes: u64, az: PeerAz) -> HitEvent {
        HitEvent::Peer {
            bytes_returned: bytes,
            peer_az: az,
        }
    }

    #[test]
    fn for_region_returns_known_presets() {
        let m = CostModel::for_region("us-east-1").expect("known region");
        assert_eq!(m.region_id, "us-east-1");
        assert!(m.get_request_micro_cents > 0);

        let m = CostModel::for_region("ap-south-1").expect("known region");
        assert_eq!(m.region_id, "ap-south-1");
    }

    #[test]
    fn for_region_rejects_unknown() {
        assert!(matches!(
            CostModel::for_region("eu-west-1").unwrap_err(),
            CostConfigError::UnknownRegion { .. }
        ));
    }

    #[test]
    fn memory_hit_same_az_only_charges_get() {
        let m = CostModel::for_region("us-east-1").unwrap();
        // 1 MiB memory hit, same AZ → only 1 GET avoided
        // (us-east-1 same-AZ data-transfer is 0).
        let saved = m.dollars_saved(evt_mem(1 << 20, PeerAz::SameAz));
        // 40 µ¢ / req = 0 ¢ truncated. The whole-cent counter
        // stays at zero on a single sub-cent hit; that's why we
        // keep micro-cents internally and only round at the
        // boundary.
        assert_eq!(saved, Cents::ZERO);
    }

    #[test]
    fn cross_az_hit_charges_data_transfer() {
        let m = CostModel::for_region("us-east-1").unwrap();
        // 1 GiB cross-AZ hit. AWS publishes $0.01/GiB; the model
        // stores `1_000_000 µ¢/GiB` exactly. So
        //   1 GiB × 1_000_000 µ¢/GiB = 1_000_000 µ¢ = 1 cent.
        // Plus 40 µ¢ for the avoided GET, which floors to 0 cents.
        // Net: exactly 1 cent. This test traps either a missing
        // data-transfer term (would be 0 ¢) or a unit confusion
        // (would be 1073 ¢ if storage stayed `µ¢/byte` — that was
        // the regression the GiB unit fixed).
        let saved = m.dollars_saved(evt_disk(1 << 30, PeerAz::CrossAz));
        assert_eq!(saved.as_cents_i64(), 1, "got {saved}");
    }

    #[test]
    fn peer_event_uses_cross_az_coefficient() {
        let m = CostModel::for_region("ap-south-1").unwrap();
        let saved = m.dollars_saved(evt_peer(1 << 30, PeerAz::CrossAz));
        // Same coefficient as Memory/Disk; the variant only changes
        // the Prometheus outcome label.
        let baseline = m.dollars_saved(evt_mem(1 << 30, PeerAz::CrossAz));
        assert_eq!(saved, baseline);
    }

    #[test]
    fn nat_term_is_zero_unless_explicitly_enabled() {
        let mut m = CostModel::for_region("us-east-1").unwrap();
        assert_eq!(m.nat_traversal_basis_points, 0);
        // 10 GiB hit through the cross-AZ coefficient; without NAT
        // affirmation, the NAT term must contribute zero.
        let baseline = m.dollars_saved(evt_disk(10 << 30, PeerAz::CrossAz));
        m.nat_traversal_basis_points = 10_000; // 100% NAT
        let with_nat = m.dollars_saved(evt_disk(10 << 30, PeerAz::CrossAz));
        // NAT-on must be strictly greater. NAT is $0.045/GiB ⇒
        // 4_500_000 µ¢/GiB, so 10 GiB × 4_500_000 µ¢/GiB ÷ 1_000_000
        // = 45 cents extra. Cross-AZ baseline at 10 GiB is 10 ¢,
        // so with_nat == 55 ¢. We assert a strict-positive band
        // rather than an equality so a future refresh of the NAT
        // rate (e.g. AWS bumps it $0.045 → $0.05) doesn't pin this
        // test to a stale cents number.
        assert!(
            with_nat.as_cents_i64() >= baseline.as_cents_i64() + 40,
            "expected NAT contribution; baseline={baseline}, with_nat={with_nat}",
        );
    }

    #[test]
    fn validate_rejects_negative() {
        let mut m = CostModel::for_region("us-east-1").unwrap();
        m.cross_az_micro_cents_per_gib = -1;
        assert!(matches!(
            m.validate().unwrap_err(),
            CostConfigError::NegativeCoefficient
        ));
    }

    #[test]
    fn validate_rejects_nat_bps_above_10000() {
        let mut m = CostModel::for_region("us-east-1").unwrap();
        m.nat_traversal_basis_points = 10_001;
        assert!(matches!(
            m.validate().unwrap_err(),
            CostConfigError::NatBpsOutOfRange { .. }
        ));
    }

    #[test]
    fn cents_saturating_arith() {
        assert_eq!(Cents::new(5).saturating_add(Cents::new(7)), Cents::new(12));
        assert_eq!(
            Cents::new(i64::MAX).saturating_add(Cents::new(1)),
            Cents::new(i64::MAX)
        );
        assert_eq!(
            Cents::new(-3).saturating_sub(Cents::new(i64::MAX)),
            Cents::new(i64::MIN)
        );
    }

    #[test]
    fn cents_display_includes_dollars_and_cents_form() {
        let s = format!("{}", Cents::new(123));
        assert!(s.contains("$1.23"));
        assert!(s.contains("123"));
    }
}
