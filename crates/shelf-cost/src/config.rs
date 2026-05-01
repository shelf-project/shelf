//! Serde-validated config loader for the `shelf-cost` crate.
//!
//! Maps `cache.cost.*` YAML (see `charts/shelf/values.yaml`) into a
//! [`CostConfig`] which `shelfd` then turns into a runtime
//! [`CostModel`] via `CostModel::from_config`. Every field is
//! optional — an unset field falls back to the regional preset in
//! [`crate::coefficients`] so an operator who only flips
//! `cache.cost.region` (the common case) still gets sensible
//! coefficients without having to copy them by hand.
//!
//! The loader's job is to:
//!
//! 1. Reject negative coefficients **at load time** so a typo in
//!    YAML cannot silently zero out the counter.
//! 2. Reject unknown regions for which no published preset exists
//!    so an operator running in `eu-west-1` is told up-front that
//!    they must supply explicit coefficients (they go in their
//!    own values overlay with citations).
//! 3. Enforce the NAT-fraction's basis-points domain (0..=10_000).
//!
//! All errors are carried as [`CostConfigError`] variants — never
//! `panic!` / `unwrap`. The single allowed `expect` at construction
//! time is in [`crate::coefficients`]'s `OnceLock::get_or_init`,
//! which is bug-not-config.

use serde::{Deserialize, Serialize};

use crate::CostModel;

/// YAML-shaped operator configuration for the cost model.
///
/// Defaults live on the **field level** (not at the struct level)
/// because the absence of a field expresses "use the preset" —
/// not "use the empty value". Serde's `#[serde(default)]` on each
/// field expresses that intent and falls back to the matching
/// preset in `CostModel::from_config`.
///
/// The struct is `#[serde(deny_unknown_fields)]` so a chart
/// regression that adds a typo'd key (`reigon`, `gets_micro_cents`)
/// fails noisily at load time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CostConfig {
    /// Master switch. **Defaults to `true`** — the counter is on
    /// out of the box (with the regional preset) and operators
    /// flip `cache.cost.enabled: false` to disable. The cost of
    /// keeping it on is one atomic add per cache hit (see
    /// SHELF-40 design note benchmark).
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// AWS region identifier — `us-east-1` or `ap-south-1` are the
    /// presets that ship with citations. Anything else requires
    /// explicit `*_micro_cents_*` overrides.
    ///
    /// Defaults to `us-east-1` (matching `origin.region` default
    /// in `charts/shelf/values.yaml`). Operator-specific overlays
    /// (e.g. an `ap-south-1` Mumbai cluster) flip this in their
    /// own values file.
    #[serde(default = "default_region")]
    pub region: String,

    /// Override for `$/req` (stored as µ¢/req). Unset ⇒ inherit
    /// the regional preset.
    #[serde(default)]
    pub get_request_micro_cents: Option<i64>,

    /// Override for the same-AZ rate (µ¢/GiB; `GiB = 2^30 bytes`).
    #[serde(default)]
    pub same_az_micro_cents_per_gib: Option<i64>,

    /// Override for the cross-AZ rate (µ¢/GiB).
    #[serde(default)]
    pub cross_az_micro_cents_per_gib: Option<i64>,

    /// Override for NAT-gateway data-processing rate (µ¢/GiB).
    #[serde(default)]
    pub nat_processing_micro_cents_per_gib: Option<i64>,

    /// Fraction of cache-saved bytes that *would have* exited via
    /// NAT, as basis points (0..=10_000). Default `None` ⇒ 0 in
    /// the model. Setting this is the operator's affirmation that
    /// their VPC topology actually charges NAT for the modelled
    /// traffic; without it, the NAT term contributes zero.
    ///
    /// (Type is `Option<u32>` not `Option<u16>` because operators
    /// occasionally express it as a percentage like `9500` for a
    /// long-tail "95% of traffic crosses NAT" cluster, and we
    /// don't want a u16 overflow to fold into the u16 wrap.)
    #[serde(default)]
    pub nat_traversal_basis_points: Option<u32>,

    /// SHELF-A4 — amortised pool operating cost in **dollars per
    /// hour**. When set, shelfd publishes
    /// `shelf_s3_dollars_saved_net_total` = gross savings minus
    /// `amortized_dollars_per_hour × elapsed`. When unset (`None`),
    /// the net counter is **silent** — anti-overclaim guard so a
    /// procurement-facing dashboard can never read "we saved $X net"
    /// off a series the operator never authorised.
    ///
    /// The companion gauge `shelf_pool_amortized_dollars_per_hour`
    /// is **always** published; an unset (or zero) value is the
    /// dashboard signal that net accounting is dormant.
    ///
    /// Type is `f64` because real-world per-cluster pool costs
    /// (`6 × m5a.4xlarge × $0.864/hr ≈ $5.18/hr` for the canonical
    /// data-platform overlay) are fractional dollars and operators
    /// copy the number from the AWS Cost Explorer / EKS bill which
    /// reports cents. The loader rejects negative / non-finite
    /// values at config-load time so a typo cannot silently zero
    /// out the term.
    #[serde(default)]
    pub amortized_dollars_per_hour: Option<f64>,
}

fn default_enabled() -> bool {
    true
}

fn default_region() -> String {
    "us-east-1".to_owned()
}

// Manual `Default` impl because the auto-derive would emit
// `enabled = false` and `region = String::new()` — both wrong for
// SHELF-40's "default-on, default-region us-east-1" contract. Serde
// reuses these helpers at deserialise time via
// `#[serde(default = "...")]` on each field, so the YAML loader
// behaviour and the in-Rust `Default` impl stay in lockstep.
impl Default for CostConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            region: default_region(),
            get_request_micro_cents: None,
            same_az_micro_cents_per_gib: None,
            cross_az_micro_cents_per_gib: None,
            nat_processing_micro_cents_per_gib: None,
            nat_traversal_basis_points: None,
            amortized_dollars_per_hour: None,
        }
    }
}

/// Errors the loader returns. All variants surface in the boot log
/// of `shelfd` and refuse to register the counter, so a misconfig
/// fails loud.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CostConfigError {
    #[error("cache.cost: unknown region {region:?}; supported: us-east-1, ap-south-1")]
    UnknownRegion { region: String },

    #[error("cache.cost: every coefficient must be non-negative; check the µ¢ overrides")]
    NegativeCoefficient,

    #[error("cache.cost.nat_traversal_basis_points = {bps} is out of range (must be 0..=10_000)")]
    NatBpsOutOfRange { bps: u32 },

    #[error(
        "cache.cost.amortized_dollars_per_hour = {value} is invalid (must be finite and >= 0)"
    )]
    InvalidAmortization { value: f64 },

    #[error("cache.cost: failed to parse YAML: {0}")]
    Yaml(String),
}

impl CostConfig {
    /// Build a [`CostModel`] from this config. Equivalent to
    /// `CostModel::from_config(self)` — re-exposed here so callers
    /// who already hold a `CostConfig` don't need a second import.
    pub fn into_model(&self) -> Result<CostModel, CostConfigError> {
        CostModel::from_config(self)
    }

    /// SHELF-A4 — surface the validated amortisation rate without
    /// requiring the caller to materialise a [`CostModel`]. Returns
    /// `Ok(None)` when the operator left the field unset (the
    /// anti-overclaim default). `Err(InvalidAmortization)` on
    /// `NaN` / `Inf` / negative — same fail-loud discipline as the
    /// other coefficient validations.
    pub fn validated_amortized_dollars_per_hour(&self) -> Result<Option<f64>, CostConfigError> {
        match self.amortized_dollars_per_hour {
            None => Ok(None),
            Some(v) if v.is_finite() && v >= 0.0 => Ok(Some(v)),
            Some(v) => Err(CostConfigError::InvalidAmortization { value: v }),
        }
    }

    /// Convenience for tests + dev clusters: parse from a YAML
    /// string. Production wiring lives in `shelfd`'s `config.rs`
    /// which embeds this struct as a sub-config of the daemon's
    /// top-level [`shelfd::config::Config`].
    pub fn from_yaml(yaml: &str) -> Result<Self, CostConfigError> {
        serde_yaml::from_str::<CostConfig>(yaml).map_err(|e| CostConfigError::Yaml(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_loads_us_east_1_preset() {
        let cfg = CostConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.region, "us-east-1");
        let m = cfg.into_model().expect("preset loads");
        assert_eq!(m.get_request_micro_cents, 40);
    }

    #[test]
    fn explicit_region_overrides_default() {
        let yaml = "region: ap-south-1\n";
        let cfg = CostConfig::from_yaml(yaml).expect("parses");
        let m = cfg.into_model().unwrap();
        assert_eq!(m.region_id, "ap-south-1");
    }

    #[test]
    fn unknown_region_rejected_at_load() {
        let yaml = "region: not-a-real-region\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        let err = cfg.into_model().unwrap_err();
        assert!(matches!(err, CostConfigError::UnknownRegion { .. }));
    }

    #[test]
    fn negative_coefficient_is_rejected() {
        let yaml = "region: us-east-1\nget_request_micro_cents: -1\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        let err = cfg.into_model().unwrap_err();
        assert_eq!(err, CostConfigError::NegativeCoefficient);
    }

    #[test]
    fn negative_cross_az_is_rejected() {
        let yaml = "region: us-east-1\ncross_az_micro_cents_per_gib: -42\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        let err = cfg.into_model().unwrap_err();
        assert_eq!(err, CostConfigError::NegativeCoefficient);
    }

    #[test]
    fn nat_bps_out_of_range_is_rejected() {
        let yaml = "region: us-east-1\nnat_traversal_basis_points: 12000\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        let err = cfg.into_model().unwrap_err();
        assert!(matches!(err, CostConfigError::NatBpsOutOfRange { .. }));
    }

    #[test]
    fn unknown_field_is_rejected() {
        let yaml = "region: us-east-1\nreigon: typo\n"; // `reigon` typo
        let err = CostConfig::from_yaml(yaml).unwrap_err();
        assert!(matches!(err, CostConfigError::Yaml(_)));
    }

    #[test]
    fn override_takes_priority_over_preset() {
        let yaml = "
region: us-east-1
get_request_micro_cents: 99
";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        let m = cfg.into_model().unwrap();
        assert_eq!(m.get_request_micro_cents, 99);
    }

    #[test]
    fn nat_bps_zero_keeps_term_off() {
        let yaml = "region: us-east-1\nnat_traversal_basis_points: 0\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        let m = cfg.into_model().unwrap();
        assert_eq!(m.nat_traversal_basis_points, 0);
    }

    // SHELF-A4 amortisation field tests.

    #[test]
    fn amortization_unset_by_default() {
        let cfg = CostConfig::default();
        assert!(cfg.amortized_dollars_per_hour.is_none());
        assert_eq!(cfg.validated_amortized_dollars_per_hour().unwrap(), None);
    }

    #[test]
    fn amortization_explicit_zero_is_publishable() {
        let yaml = "region: us-east-1\namortized_dollars_per_hour: 0.0\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        assert_eq!(
            cfg.validated_amortized_dollars_per_hour().unwrap(),
            Some(0.0)
        );
    }

    #[test]
    fn amortization_explicit_positive_validates() {
        let yaml = "region: us-east-1\namortized_dollars_per_hour: 5.18\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        assert_eq!(
            cfg.validated_amortized_dollars_per_hour().unwrap(),
            Some(5.18)
        );
        // `from_config` validates as a side effect.
        cfg.into_model().expect("model constructs");
    }

    #[test]
    fn amortization_negative_is_rejected() {
        let yaml = "region: us-east-1\namortized_dollars_per_hour: -1.0\n";
        let cfg = CostConfig::from_yaml(yaml).expect("yaml parses");
        assert!(matches!(
            cfg.validated_amortized_dollars_per_hour().unwrap_err(),
            CostConfigError::InvalidAmortization { .. }
        ));
        assert!(matches!(
            cfg.into_model().unwrap_err(),
            CostConfigError::InvalidAmortization { .. }
        ));
    }

    #[test]
    fn amortization_nan_is_rejected() {
        // f64::NAN cannot reach this path through plain YAML
        // (serde_yaml rejects `.nan` literal for f64 fields), but
        // a constructed-by-hand config still must trip the check.
        let cfg = CostConfig {
            amortized_dollars_per_hour: Some(f64::NAN),
            ..CostConfig::default()
        };
        assert!(matches!(
            cfg.validated_amortized_dollars_per_hour().unwrap_err(),
            CostConfigError::InvalidAmortization { .. }
        ));
    }
}
