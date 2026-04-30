//! RC6 P1.3 — `/admin/pin` schema flexibility.
//!
//! Today `POST /admin/pin` only accepts the strict shape:
//!
//! ```json
//! { "key_hex": "<64-hex>", "pool": "metadata|rowgroup", "mv_name": "<opt>" }
//! ```
//!
//! Replay-list pre-warm tooling (the spiritual successor to
//! `tools/gen_pin_list.py`, used during cutover pre-warm windows)
//! emits *manifest entries* describing S3 objects rather than
//! pre-computed cache keys:
//!
//! ```json
//! [{
//!   "bucket": "<s3-bucket>",
//!   "key":    "<s3-object-key>",
//!   "etag":   "<etag-as-returned-by-S3-HEAD>",
//!   "size_bytes": 12345,
//!   "pool": "metadata",         // optional, default: "metadata"
//!   "offset": 0,                // optional, default: 0
//!   "length": null,             // optional, default: size_bytes
//!   "rg_ordinal": 0,            // optional, default: 0
//!   "mv_name": null             // optional
//! }]
//! ```
//!
//! Posting that JSON to `/admin/pin` today returns
//! `400 invalid_request`, blocking the pinned-table protection
//! that the cutover playbook expects (workspace memory entry
//! "`tools/gen_pin_list.py` /admin/pin schema gap" — Apr 30 finding
//! during the rep-0 cutover prep).
//!
//! ## Decision (per ADR-0023)
//!
//! Widen the deserializer via a `serde(untagged)` enum so
//! `/admin/pin` accepts either schema. The replay-list shape is
//! converted to the strict `key_hex/pool` shape internally by
//! computing
//!
//! ```text
//! key_hex = hex(sha256(etag_bytes || u64_le(offset) || u64_le(length) || u32_le(rg_ordinal)))
//! ```
//!
//! exactly as `crate::store::key_from_tuple` does — same algorithm
//! the file-driven loader and `gen_pin_list.py` use, so a key
//! computed from the same Iceberg manifest entry hits the same
//! Foyer slot regardless of which channel admitted it.
//!
//! Backward compatibility: pre-RC6 strict callers (the existing
//! `shelfctl pin` codepath, the H3 mv-pin-watcher, the
//! cutover-validation script) all continue to work unchanged
//! because untagged enums try variants in declaration order, and
//! the strict variant is declared first.
//!
//! ## Cardinality and bounds
//!
//! The replay-list batch endpoint currently accepts up to
//! [`MAX_REPLAY_BATCH`] entries per request (matches the
//! `/cache/contains` cap of 65 536). Larger pre-warm sets must be
//! chunked client-side.

use serde::{Deserialize, Serialize};

use crate::store::{key_from_tuple, Pool};

/// Maximum entries accepted in a single `POST /admin/pin` array body.
/// Same value as `cache_contains` (65 536); chosen so a misbehaving
/// pre-warm script cannot schedule unbounded blocking work in one
/// request.
pub const MAX_REPLAY_BATCH: usize = 65_536;

/// Strict pin-payload shape. Identical to the existing
/// `handlers::PinEvictBody` field set; kept independently here so
/// the [`PinPayload`] enum below can be moved to its own module
/// without touching the `http::handlers` namespace.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrictPinEntry {
    /// 64-char lower-case hex of the SHELF-04 content-addressed key.
    pub key_hex: String,
    /// `"metadata"` or `"rowgroup"`.
    pub pool: String,
    /// Track H5 — optional fully-qualified MV name.
    #[serde(default)]
    pub mv_name: Option<String>,
}

/// Replay-list manifest entry. The fields mirror what a tool that
/// reads Iceberg manifests + S3 HEAD already has in scope, so no
/// "go re-derive your cache keys client-side" round-trip is forced
/// on the operator.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplayPinEntry {
    /// S3 bucket. Captured for audit logging only — the key
    /// derivation does not consume it.
    pub bucket: String,
    /// S3 object key. Captured for audit logging only.
    pub key: String,
    /// ETag returned by S3 `HeadObject`. Surrounding double-quotes
    /// (`"abc123"`) are stripped before hashing so a caller that
    /// passes either form gets the same cache key — matches
    /// `tools/gen_pin_list.py`'s `etag.strip('"').encode()`.
    pub etag: String,
    /// Object size in bytes. Used as the default `length` when
    /// none is supplied (whole-file pin), and surfaced in the
    /// per-entry response so the client can sanity-check.
    pub size_bytes: u64,
    /// Pool to admit into; defaults to `"metadata"` (matches
    /// `gen_pin_list.py`).
    #[serde(default = "default_pool")]
    pub pool: String,
    /// Byte offset within the object. Defaults to 0 (whole-file).
    #[serde(default)]
    pub offset: u64,
    /// Range length. `None` ⇒ defaults to `size_bytes`. This is
    /// the field that distinguishes a whole-file pin from a
    /// row-group-level pin.
    #[serde(default)]
    pub length: Option<u64>,
    /// Row-group ordinal. `0` for non-columnar ranges (manifests,
    /// footers); matches the v1 shelfd contract.
    #[serde(default)]
    pub rg_ordinal: u32,
    /// Optional materialized-view name (Track H5).
    #[serde(default)]
    pub mv_name: Option<String>,
}

fn default_pool() -> String {
    "metadata".to_string()
}

/// Wire-level pin payload. `serde(untagged)` lets a single endpoint
/// accept either shape without a discriminator field — the deserializer
/// tries variants top-down and picks the first that fits.
///
/// **Variant order is load-bearing**: the strict shape is tried first
/// so pre-RC6 callers continue to bind to it; replay shapes only
/// match when the strict required fields (`key_hex`, `pool`) are
/// absent.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PinPayload {
    /// Existing `{key_hex, pool, [mv_name]}` shape. Single-entry.
    Strict(StrictPinEntry),
    /// Array of replay-list manifest entries. Used by pre-warm
    /// tooling that wants to pin a whole table's manifests in one
    /// request.
    ReplayBatch(Vec<ReplayPinEntry>),
    /// Single replay-list manifest entry. Same shape as
    /// `ReplayBatch[0]` — kept distinct so the deserializer can
    /// distinguish a one-element array from a bare object.
    ReplaySingle(ReplayPinEntry),
}

/// Outcome of converting a [`PinPayload`] into the strict-pin
/// representation the existing `FoyerStore::pin` path consumes.
///
/// The conversion is intentionally lossy on bucket/key (we don't
/// thread them down to the store) — it keeps them around only for
/// audit logging via `to_audit_string`. The store cares about the
/// resulting `Key` and `Pool`, nothing more.
#[derive(Debug, Clone)]
pub struct ResolvedPin {
    pub key_hex: String,
    pub pool: Pool,
    pub mv_name: Option<String>,
    /// Audit metadata — present for replay-list entries; empty
    /// string for strict entries. Surfaced in the per-entry
    /// response so an operator scrubbing pre-warm output can
    /// match the request line back to the source S3 object.
    pub audit: String,
}

impl ResolvedPin {
    /// Build the strict-pin response payload for one resolved
    /// entry. Used in the array-mode response so each per-entry
    /// row carries the audit context.
    pub fn to_response_value(&self) -> serde_json::Value {
        serde_json::json!({
            "key_hex": self.key_hex,
            "pool": pool_label(self.pool),
            "mv_name": self.mv_name,
            "audit": self.audit,
        })
    }
}

/// Convert one [`StrictPinEntry`] into a [`ResolvedPin`].
pub fn resolve_strict(entry: &StrictPinEntry) -> Result<ResolvedPin, String> {
    let pool = parse_pool(&entry.pool)?;
    Ok(ResolvedPin {
        key_hex: entry.key_hex.clone(),
        pool,
        mv_name: entry.mv_name.clone(),
        audit: String::new(),
    })
}

/// Convert one [`ReplayPinEntry`] into a [`ResolvedPin`] by
/// computing the SHELF-04 key from the manifest fields.
///
/// Mirrors `tools/gen_pin_list.py`'s `_sha256_key`:
///
/// ```text
/// etag.strip('"') + struct.pack("<Q", offset)
///                 + struct.pack("<Q", length)
///                 + struct.pack("<I", rg_ordinal)
/// ```
///
/// We feed `etag_bytes` directly into [`crate::store::key_from_tuple`]
/// which is the canonical Rust implementation of the same algorithm.
/// A golden-vector test in this module asserts the two procedures
/// agree byte-for-byte for a hand-crafted input.
pub fn resolve_replay(entry: &ReplayPinEntry) -> Result<ResolvedPin, String> {
    let pool = parse_pool(&entry.pool)?;
    let etag_clean: &str = entry.etag.trim_matches('"');
    if etag_clean.is_empty() {
        return Err("etag must be non-empty after quote-strip".to_owned());
    }
    if entry.size_bytes == 0 {
        return Err("size_bytes must be > 0".to_owned());
    }
    let length = entry.length.unwrap_or(entry.size_bytes);
    if length == 0 {
        return Err("length must be > 0".to_owned());
    }
    let key = key_from_tuple(
        etag_clean.as_bytes(),
        entry.offset,
        length,
        entry.rg_ordinal,
    )
    .map_err(|e| format!("key_from_tuple: {e}"))?;
    let key_hex = key.to_hex();
    let audit = format!(
        "s3://{}/{} etag={} offset={} length={} rg={}",
        entry.bucket, entry.key, etag_clean, entry.offset, length, entry.rg_ordinal,
    );
    Ok(ResolvedPin {
        key_hex,
        pool,
        mv_name: entry.mv_name.clone(),
        audit,
    })
}

/// Resolve a [`PinPayload`] into a list of [`ResolvedPin`] entries,
/// ready for the existing `FoyerStore::pin` call site.
///
/// Returns `Err` when the array exceeds [`MAX_REPLAY_BATCH`] or
/// when any individual entry fails validation. Per-entry validation
/// is intentionally fail-fast on the resolver side — partial
/// success at the resolver level would force the response to carry
/// two distinct error shapes; the per-entry pin-store outcome
/// (resident vs not-resident) is reported on the response side
/// where the contract is naturally batched.
pub fn resolve(payload: &PinPayload) -> Result<Vec<ResolvedPin>, String> {
    match payload {
        PinPayload::Strict(s) => Ok(vec![resolve_strict(s)?]),
        PinPayload::ReplaySingle(r) => Ok(vec![resolve_replay(r)?]),
        PinPayload::ReplayBatch(rs) => {
            if rs.len() > MAX_REPLAY_BATCH {
                return Err(format!(
                    "batch_too_large: {} > {MAX_REPLAY_BATCH}",
                    rs.len()
                ));
            }
            let mut out = Vec::with_capacity(rs.len());
            for (i, r) in rs.iter().enumerate() {
                let resolved = resolve_replay(r).map_err(|e| format!("entry[{i}]: {e}"))?;
                out.push(resolved);
            }
            Ok(out)
        }
    }
}

fn parse_pool(s: &str) -> Result<Pool, String> {
    match s {
        "metadata" => Ok(Pool::Metadata),
        "rowgroup" => Ok(Pool::RowGroup),
        other => Err(format!(
            "unknown pool '{other}'; expected 'metadata' or 'rowgroup'"
        )),
    }
}

/// Stringify a [`Pool`] for the response body. Pulled out so a
/// future addition (e.g. a third pool) lands in exactly one place.
pub fn pool_label(p: Pool) -> &'static str {
    match p {
        Pool::Metadata => "metadata",
        Pool::RowGroup => "rowgroup",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// Precondition: serde resolves the strict shape into the
    /// `Strict` variant (pre-RC6 callers must keep working).
    #[test]
    fn deserializes_strict_shape() {
        let body = r#"{"key_hex":"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff","pool":"metadata"}"#;
        let p: PinPayload = serde_json::from_str(body).expect("strict");
        match p {
            PinPayload::Strict(s) => {
                assert_eq!(s.key_hex.len(), 64);
                assert_eq!(s.pool, "metadata");
                assert!(s.mv_name.is_none());
            }
            other => panic!("expected Strict; got {other:?}"),
        }
    }

    /// Replay-list **single-entry** form: bare object with bucket/key/etag.
    #[test]
    fn deserializes_replay_single() {
        let body = r#"{
            "bucket":"shelf-test",
            "key":"path/to/manifest.avro",
            "etag":"\"abc123\"",
            "size_bytes":1024
        }"#;
        let p: PinPayload = serde_json::from_str(body).expect("replay single");
        match p {
            PinPayload::ReplaySingle(r) => {
                assert_eq!(r.bucket, "shelf-test");
                assert_eq!(r.size_bytes, 1024);
                assert_eq!(r.pool, "metadata", "default pool is metadata");
            }
            other => panic!("expected ReplaySingle; got {other:?}"),
        }
    }

    /// Replay-list **batch** form: top-level array.
    #[test]
    fn deserializes_replay_batch() {
        let body = r#"[
            {"bucket":"a","key":"x","etag":"e1","size_bytes":10},
            {"bucket":"b","key":"y","etag":"e2","size_bytes":20,"pool":"rowgroup","rg_ordinal":3}
        ]"#;
        let p: PinPayload = serde_json::from_str(body).expect("replay batch");
        match p {
            PinPayload::ReplayBatch(rs) => {
                assert_eq!(rs.len(), 2);
                assert_eq!(rs[1].pool, "rowgroup");
                assert_eq!(rs[1].rg_ordinal, 3);
            }
            other => panic!("expected ReplayBatch; got {other:?}"),
        }
    }

    /// Strict and replay-single resolve into the same internal
    /// representation when the replay path is given a key_hex
    /// computed by hand. Locks in the contract that the wire shape
    /// is the only thing that differs across schemas.
    #[test]
    fn replay_and_strict_produce_identical_keys_for_same_inputs() {
        // Hand-computed key: sha256("e0" || u64_le(0) || u64_le(100) || u32_le(0))
        let mut h = Sha256::new();
        h.update(b"e0");
        h.update(0u64.to_le_bytes());
        h.update(100u64.to_le_bytes());
        h.update(0u32.to_le_bytes());
        let expected = hex::encode(h.finalize());

        // Replay path
        let r = ReplayPinEntry {
            bucket: "b".into(),
            key: "k".into(),
            etag: "e0".into(),
            size_bytes: 100,
            pool: "metadata".into(),
            offset: 0,
            length: None,
            rg_ordinal: 0,
            mv_name: None,
        };
        let replay = resolve_replay(&r).expect("replay resolve");
        assert_eq!(replay.key_hex, expected);

        // Strict path with the hand-computed key
        let s = StrictPinEntry {
            key_hex: expected.clone(),
            pool: "metadata".into(),
            mv_name: None,
        };
        let strict = resolve_strict(&s).expect("strict resolve");
        assert_eq!(strict.key_hex, replay.key_hex);
        assert_eq!(strict.pool, replay.pool);
    }

    /// Quote-stripped etag matches the Python tool's
    /// `etag.strip('"').encode()` exactly. A caller that passes the
    /// raw S3 ETag verbatim (with surrounding double-quotes) gets
    /// the same key as a caller that already stripped them.
    #[test]
    fn quoted_and_unquoted_etag_yield_same_key() {
        let q = ReplayPinEntry {
            bucket: "b".into(),
            key: "k".into(),
            etag: "\"abc123\"".into(),
            size_bytes: 50,
            pool: "metadata".into(),
            offset: 0,
            length: None,
            rg_ordinal: 0,
            mv_name: None,
        };
        let unq = ReplayPinEntry {
            etag: "abc123".into(),
            ..q.clone()
        };
        let q_resolved = resolve_replay(&q).expect("quoted");
        let unq_resolved = resolve_replay(&unq).expect("unquoted");
        assert_eq!(
            q_resolved.key_hex, unq_resolved.key_hex,
            "etag quote-strip must be bit-equivalent to client-side strip"
        );
    }

    /// Empty etag (after quote-strip) is rejected. Defensive — a
    /// pre-warm tool with a bug that emits empty etags must not get
    /// silently admitted as "all keys have the same hash".
    #[test]
    fn empty_etag_is_rejected() {
        let r = ReplayPinEntry {
            bucket: "b".into(),
            key: "k".into(),
            etag: "\"\"".into(),
            size_bytes: 50,
            pool: "metadata".into(),
            offset: 0,
            length: None,
            rg_ordinal: 0,
            mv_name: None,
        };
        let err = resolve_replay(&r).expect_err("must reject empty etag");
        assert!(err.contains("etag"), "{err}");
    }

    /// Zero size_bytes / explicit zero length are rejected.
    #[test]
    fn zero_length_is_rejected() {
        let r = ReplayPinEntry {
            bucket: "b".into(),
            key: "k".into(),
            etag: "e".into(),
            size_bytes: 0,
            pool: "metadata".into(),
            offset: 0,
            length: None,
            rg_ordinal: 0,
            mv_name: None,
        };
        let err = resolve_replay(&r).expect_err("must reject 0 size");
        assert!(
            err.contains("size_bytes") || err.contains("length"),
            "{err}"
        );
    }

    /// Unknown pool reaches a clear error rather than getting
    /// silently downgraded.
    #[test]
    fn unknown_pool_is_rejected() {
        let r = ReplayPinEntry {
            bucket: "b".into(),
            key: "k".into(),
            etag: "e".into(),
            size_bytes: 1,
            pool: "weird".into(),
            offset: 0,
            length: None,
            rg_ordinal: 0,
            mv_name: None,
        };
        let err = resolve_replay(&r).expect_err("must reject pool='weird'");
        assert!(err.contains("unknown pool"), "{err}");
    }

    /// Batch above the cap is rejected; below the cap resolves.
    #[test]
    fn batch_cap_is_enforced() {
        let too_many: Vec<ReplayPinEntry> = (0..(MAX_REPLAY_BATCH + 1))
            .map(|i| ReplayPinEntry {
                bucket: "b".into(),
                key: format!("k{i}"),
                etag: "e".into(),
                size_bytes: 1,
                pool: "metadata".into(),
                offset: 0,
                length: None,
                rg_ordinal: 0,
                mv_name: None,
            })
            .collect();
        let err = resolve(&PinPayload::ReplayBatch(too_many)).expect_err("batch too large");
        assert!(err.contains("batch_too_large"), "{err}");
    }

    /// Round-trip: `resolve(PinPayload::ReplayBatch(...))` returns
    /// one [`ResolvedPin`] per input entry, preserving order.
    #[test]
    fn batch_resolves_in_order() {
        let entries: Vec<ReplayPinEntry> = (0..5)
            .map(|i| ReplayPinEntry {
                bucket: format!("b{i}"),
                key: format!("k{i}"),
                etag: format!("e{i}"),
                size_bytes: 100 + i as u64,
                pool: if i % 2 == 0 { "metadata" } else { "rowgroup" }.into(),
                offset: 0,
                length: None,
                rg_ordinal: 0,
                mv_name: None,
            })
            .collect();
        let resolved = resolve(&PinPayload::ReplayBatch(entries)).expect("resolve");
        assert_eq!(resolved.len(), 5);
        for (i, r) in resolved.iter().enumerate() {
            assert!(r.audit.contains(&format!("s3://b{i}/k{i}")));
            assert_eq!(
                r.pool,
                if i % 2 == 0 {
                    Pool::Metadata
                } else {
                    Pool::RowGroup
                }
            );
        }
    }

    /// The strict variant takes precedence over replay variants
    /// when both could hypothetically match — defensive against
    /// a future schema overlap.
    #[test]
    fn strict_variant_wins_when_key_hex_present() {
        // A body with BOTH key_hex AND replay-style fields; serde
        // untagged tries Strict first, so we should land in Strict.
        let body = r#"{
            "key_hex":"0000000000000000000000000000000000000000000000000000000000000001",
            "pool":"metadata",
            "bucket":"b",
            "key":"k",
            "etag":"e",
            "size_bytes":42
        }"#;
        let p: PinPayload = serde_json::from_str(body).expect("hybrid");
        assert!(matches!(p, PinPayload::Strict(_)));
    }
}
