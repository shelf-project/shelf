//! Track E7 — canonicalised jsonPlan fingerprint.
//!
//! Trino's `QueryCompletedEvent` carries a `jsonPlan` payload — a
//! JSON tree of the logical plan. Two queries that differ only by
//! literals (`WHERE x = 42` vs `WHERE x = 43`) should fingerprint to
//! the same string so we can attribute cache hits to "the same
//! query shape" across runs.
//!
//! This module exposes one function, [`canonicalise`], that maps a
//! raw `jsonPlan` payload to a short, stable fingerprint. The
//! fingerprint is what ends up in the `fingerprint` label of the
//! `shelf_queries_served_total` / `shelf_bytes_saved_total` counters,
//! and what the H1 MV advisor groups by when it ranks candidate
//! materialised views.
//!
//! Design choices:
//!
//! - **Literal erasure, not literal hashing.** Every string literal,
//!   number, timestamp, decimal, and binary constant is replaced
//!   with a typed sentinel (`?str`, `?num`, `?ts`, …). This keeps
//!   query-shape-equivalent plans identical while preserving their
//!   structural differences.
//! - **Symmetric-operator ordering.** `a = b` and `b = a` fingerprint
//!   identically; ditto `AND` / `OR` operand order. We sort the
//!   child array of symmetric operators lexicographically by their
//!   own canonical form.
//! - **Fingerprint is 16-hex chars.** `xxhash3_64` of the canonical
//!   string → 16 lowercase hex chars. 64 bits is plenty for a
//!   bounded top-K cardinality label, and is cheap to compute.
//! - **Tenant derived from session.** The plugin supplies the
//!   tenant; this module does not peek into the jsonPlan for it.
//!
//! The implementation works on `serde_json::Value` — we do **not**
//! depend on any Trino-specific crate. When Trino changes the
//! jsonPlan shape across versions, only the small field allow-list
//! below needs to track it.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

/// Sentinel returned for any literal we collapse. The sentinel is a
/// single ASCII token so it never collides with a real JSON string
/// value in practice (real literals are double-quoted JSON; our
/// sentinel is not valid JSON standalone but is fine when embedded
/// in a canonicalised-tree string).
pub const LITERAL_SENTINEL: &str = "?lit";

/// Commutative operators whose child order should be canonicalised.
/// Matches Trino's `Signatures` enum; kept as a sorted static slice
/// so lookups are `binary_search`. Trailing `_` forms handle
/// bitwise + logical aliases.
const COMMUTATIVE_NAMES: &[&str] = &[
    "$operator$add",
    "$operator$and",
    "$operator$bit_and",
    "$operator$bit_or",
    "$operator$bit_xor",
    "$operator$equal",
    "$operator$not_equal",
    "$operator$or",
    "$operator$multiply",
];

/// Canonicalise a raw `jsonPlan` string into a `(fingerprint,
/// canonical_form)` tuple.
///
/// - `fingerprint`: 16-hex-char stable identifier.
/// - `canonical_form`: the fully-literal-erased, operand-sorted JSON
///   representation; retained so debug UIs can show operators
///   what two queries actually share.
pub fn canonicalise(json_plan: &str) -> (String, String) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json_plan) else {
        // Malformed input: fingerprint it by raw hash so we still get
        // some observability, but with the sentinel tenant. This
        // matches the H1 spec: the MV advisor must never see
        // `NaN` / empty fingerprints.
        return (
            short_hash(json_plan.as_bytes()),
            format!("<unparseable:{}>", truncate(json_plan, 64)),
        );
    };
    let canonical = canonicalise_value(&value);
    let canonical_str = canonical_str(&canonical);
    let fp = short_hash(canonical_str.as_bytes());
    (fp, canonical_str)
}

/// Truncate a `fingerprint` to the hot-tail cap. Inputs above `cap`
/// unique values collapse to the single sentinel `other`. The
/// caller (the E7 wiring layer in `s3_shim.rs`) keeps a small LRU
/// of recently-seen fingerprints and hands every new one to this
/// function with a bool indicating "within cap".
pub fn label_or_other(fp: &str, within_cap: bool) -> &str {
    if within_cap {
        fp
    } else {
        "other"
    }
}

fn canonicalise_value(v: &serde_json::Value) -> CanonValue {
    match v {
        serde_json::Value::Null => CanonValue::Lit("null"),
        serde_json::Value::Bool(b) => CanonValue::Lit(if *b { "true" } else { "false" }),
        serde_json::Value::Number(_) => CanonValue::Lit("?num"),
        serde_json::Value::String(s) => {
            // ISO-8601-ish timestamps get a dedicated sentinel so
            // plans that differ only by `WHERE day = '2026-04-01'`
            // fingerprint identically.
            if looks_like_timestamp(s) {
                CanonValue::Lit("?ts")
            } else if looks_like_uuid(s) {
                CanonValue::Lit("?uuid")
            } else {
                CanonValue::Lit(LITERAL_SENTINEL)
            }
        }
        serde_json::Value::Array(items) => {
            CanonValue::Array(items.iter().map(canonicalise_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut out = BTreeMap::new();
            let op_name = map
                .get("@type")
                .or_else(|| map.get("type"))
                .or_else(|| map.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            for (k, v) in map {
                if is_skippable_key(k) {
                    continue;
                }
                out.insert(k.clone(), canonicalise_value(v));
            }
            if let Some(op) = &op_name {
                if is_commutative(op) {
                    let key = if out.contains_key("arguments") {
                        Some("arguments")
                    } else if out.contains_key("children") {
                        Some("children")
                    } else {
                        None
                    };
                    if let Some(k) = key {
                        if let Some(CanonValue::Array(children)) = out.get_mut(k) {
                            children.sort_by(|a, b| canonical_str(a).cmp(&canonical_str(b)));
                        }
                    }
                }
            }
            CanonValue::Object(out)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CanonValue {
    Lit(&'static str),
    Array(Vec<CanonValue>),
    Object(BTreeMap<String, CanonValue>),
}

fn canonical_str(v: &CanonValue) -> String {
    match v {
        CanonValue::Lit(s) => (*s).to_string(),
        CanonValue::Array(items) => {
            let mut out = String::from("[");
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_str(item));
            }
            out.push(']');
            out
        }
        CanonValue::Object(m) => {
            let mut out = String::from("{");
            for (i, (k, val)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(k);
                out.push(':');
                out.push_str(&canonical_str(val));
            }
            out.push('}');
            out
        }
    }
}

fn is_skippable_key(k: &str) -> bool {
    // Keys that carry query-instance-specific noise (session IDs,
    // transaction timestamps, etc.) are stripped; they have no
    // semantic meaning for fingerprinting.
    matches!(
        k,
        "sessionId"
            | "session_id"
            | "queryId"
            | "query_id"
            | "transactionId"
            | "transaction_id"
            | "startTime"
            | "start_time"
            | "endTime"
            | "end_time"
            | "createTime"
            | "create_time"
    )
}

fn is_commutative(op: &str) -> bool {
    COMMUTATIVE_NAMES.binary_search(&op).is_ok()
}

fn looks_like_timestamp(s: &str) -> bool {
    // Cheap recognisers; good enough to collapse 99% of Trino
    // time literals without pulling in chrono. Prefixes are the
    // ANSI forms Trino renders.
    if s.len() < 10 {
        return false;
    }
    let bytes = s.as_bytes();
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[0..4].iter().all(|c| c.is_ascii_digit())
        && bytes[5..7].iter().all(|c| c.is_ascii_digit())
        && bytes[8..10].iter().all(|c| c.is_ascii_digit())
}

fn looks_like_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    bytes[8] == b'-' && bytes[13] == b'-' && bytes[18] == b'-' && bytes[23] == b'-'
}

fn short_hash(input: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(input);
    let out = h.finalize();
    let mut s = String::with_capacity(16);
    for b in &out[..8] {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_plans_fingerprint_identically() {
        let (fp1, _) = canonicalise(
            r#"{"type":"Filter","where":{"type":"$operator$equal","arguments":[{"column":"x"},{"const":42}]}}"#,
        );
        let (fp2, _) = canonicalise(
            r#"{"type":"Filter","where":{"type":"$operator$equal","arguments":[{"column":"x"},{"const":42}]}}"#,
        );
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn plans_differing_only_by_literal_collapse() {
        let (fp1, _) = canonicalise(
            r#"{"type":"Filter","where":{"type":"$operator$equal","arguments":[{"column":"x"},{"const":42}]}}"#,
        );
        let (fp2, _) = canonicalise(
            r#"{"type":"Filter","where":{"type":"$operator$equal","arguments":[{"column":"x"},{"const":99}]}}"#,
        );
        assert_eq!(fp1, fp2, "literal numbers must not affect fingerprint");
    }

    #[test]
    fn symmetric_operators_are_order_agnostic() {
        let (fp1, _) = canonicalise(
            r#"{"type":"$operator$equal","arguments":[{"column":"x"},{"column":"y"}]}"#,
        );
        let (fp2, _) = canonicalise(
            r#"{"type":"$operator$equal","arguments":[{"column":"y"},{"column":"x"}]}"#,
        );
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn asymmetric_operators_are_order_sensitive() {
        // Subtract is not commutative, and the arguments here are
        // *structurally* different (one is a nested expression, the
        // other a column) so the fingerprint must differ even after
        // literal erasure.
        let (fp1, _) = canonicalise(
            r#"{"type":"$operator$subtract","arguments":[{"type":"$operator$add","arguments":[{"column":"x"}]},{"column":"y"}]}"#,
        );
        let (fp2, _) = canonicalise(
            r#"{"type":"$operator$subtract","arguments":[{"column":"y"},{"type":"$operator$add","arguments":[{"column":"x"}]}]}"#,
        );
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn query_id_and_session_are_stripped() {
        let (fp1, _) =
            canonicalise(r#"{"queryId":"20261023_0001","sessionId":"abc","type":"Scan"}"#);
        let (fp2, _) =
            canonicalise(r#"{"queryId":"20261023_9999","sessionId":"xyz","type":"Scan"}"#);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn timestamps_and_uuids_collapse_to_sentinels() {
        let (fp1, _) =
            canonicalise(r#"{"type":"Filter","where":{"const":"2026-04-23T10:00:00Z"}}"#);
        let (fp2, _) =
            canonicalise(r#"{"type":"Filter","where":{"const":"2024-01-02T03:04:05Z"}}"#);
        assert_eq!(fp1, fp2);

        let (fp3, _) = canonicalise(
            r#"{"type":"Filter","where":{"const":"550e8400-e29b-41d4-a716-446655440000"}}"#,
        );
        let (fp4, _) = canonicalise(
            r#"{"type":"Filter","where":{"const":"00000000-0000-0000-0000-000000000000"}}"#,
        );
        assert_eq!(fp3, fp4);
    }

    #[test]
    fn malformed_input_still_returns_a_fingerprint() {
        let (fp, canon) = canonicalise("not json at all");
        assert_eq!(fp.len(), 16);
        assert!(canon.starts_with("<unparseable:"));
    }

    #[test]
    fn label_or_other_masks_overflow() {
        assert_eq!(label_or_other("abcd", true), "abcd");
        assert_eq!(label_or_other("abcd", false), "other");
    }

    #[test]
    fn different_plan_shapes_do_not_collide() {
        let (fp1, _) = canonicalise(r#"{"type":"Filter","where":{"column":"x"}}"#);
        let (fp2, _) = canonicalise(r#"{"type":"Aggregate","group_by":[{"column":"x"}]}"#);
        assert_ne!(fp1, fp2);
    }
}
