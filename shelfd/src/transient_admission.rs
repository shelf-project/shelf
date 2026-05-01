//! **B3 (rc.7)** — Intermediate-table opt-out admission gate.
//!
//! Refuses cache admission for tables flagged transient: dbt batch
//! tables and scratch tables that snapshot-expire in 1-3 days
//! churn through cache and are thrown out before the cache pays
//! itself back. Workspace memory: roughly 10-20% of NVMe occupancy
//! on rep-1 is intermediate-table churn.
//!
//! ## Hot-path interaction
//!
//! Consulted in [`crate::store::FoyerStore`]'s admit chain after
//! the A2 drain gate but before the SHELF-25 / SHELF-21e /
//! SHELF-29 / A6 chain. The decision is `O(1)` (one `RwLock` read
//! plus a `HashMap::get`) and short-circuits the more expensive
//! W-TinyLFU + LODC + rate-limiter work when the table is flagged.
//!
//! ## Decision sources, highest priority first
//!
//! 1. **Explicit override** — `overrides["schema.table"] =
//!    Admit | RefuseTransient`. Operator-blessed; wins over any
//!    metadata-derived value.
//! 2. **`shelf.cache-policy` table property** — the canonical
//!    custom property. `transient` ⇒ refuse; anything else ⇒
//!    admit. Mirrors the Iceberg convention of namespaced
//!    properties for engine-specific tuning.
//! 3. **Iceberg snapshot retention** — when
//!    `history.expire.max-snapshot-age-ms` is below
//!    `cfg.transient_threshold` (default 7 days), the table is
//!    flagged transient. `history.expire.min-snapshots-to-keep`
//!    on its own is not enough (Iceberg defaults to 1 even on
//!    long-lived tables); both must agree.
//!
//! ## Refresh model
//!
//! [`TransientGate::decide`] is synchronous and lock-free in the
//! steady state: a `parking_lot::RwLock<HashMap>` read, a
//! `HashMap::get`. When a table's cached decision is missing or
//! older than `cfg.decision_cache_ttl` (default 10 min), the gate
//! schedules a background refresh via [`tokio::spawn`] using a
//! single-flight HashSet to dedupe concurrent decides for the same
//! uncached table. The hot path always returns immediately —
//! `Admit` (fail-open) when no cached value is available, the
//! cached value otherwise.
//!
//! ## Composition with other admit gates
//!
//! The full admit chain (after this PR):
//!
//! 1. A2 drain gate — pod is terminating; refuse all admits.
//! 2. **B3 transient gate (this module)** — refuse admits for
//!    tables flagged transient.
//! 3. SHELF-25 size threshold + W-TinyLFU.
//! 4. SHELF-21e LODC level gate.
//! 5. SHELF-29 + A1 RSS-aware rate-limiter.
//! 6. A6 cooperative peer-admission probabilistic gate.
//!
//! See ADR-0038 for the operational rationale.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

/// Per-table cache decision: should we admit row groups for this table?
///
/// `Admit` is the fail-open default. `RefuseTransient` means the
/// table's snapshot retention (or explicit `shelf.cache-policy`)
/// indicates the data churns faster than the cache can pay itself
/// back; admitting would waste NVMe and write-amp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableAdmission {
    /// Admit normally (default).
    Admit,
    /// Refuse — table is intermediate / transient.
    RefuseTransient,
}

/// Operator-tunable knobs.
///
/// Default `enabled = false` is the safety hatch: a freshly
/// deployed shelfd that has not opted into B3 behaves identically
/// to pre-B3 (the gate is a strict no-op). Operators flip
/// `cache.transientAdmission.enabled = true` per cluster after the
/// `shelf_transient_refusals_total` dashboard panel has been
/// added.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransientAdmissionConfig {
    /// Master switch. Default `false` (opt-in per cluster).
    #[serde(default)]
    pub enabled: bool,

    /// Tables with `history.expire.max-snapshot-age-ms` below
    /// this threshold are flagged transient. Default 7 days.
    #[serde(default = "default_transient_threshold", with = "humantime_serde")]
    pub transient_threshold: Duration,

    /// Decision cache TTL — how long a refreshed decision stays
    /// authoritative before the next access triggers a background
    /// refresh. Default 10 min: table policy doesn't change often.
    #[serde(default = "default_decision_cache_ttl", with = "humantime_serde")]
    pub decision_cache_ttl: Duration,

    /// Explicit table-level overrides keyed on `schema.table`.
    /// Highest priority — overrides anything derived from
    /// `metadata.json`.
    #[serde(default)]
    pub overrides: HashMap<String, OverrideValue>,
}

/// Operator-supplied override value for a specific `schema.table`.
///
/// Serialised in `camelCase` (`admit`, `refuseTransient`) to match
/// the Helm-idiomatic spelling used in `values.yaml` overlays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OverrideValue {
    /// Force-admit this table even if metadata says it is transient.
    Admit,
    /// Force-refuse this table regardless of metadata.
    RefuseTransient,
}

impl OverrideValue {
    fn into_admission(self) -> TableAdmission {
        match self {
            OverrideValue::Admit => TableAdmission::Admit,
            OverrideValue::RefuseTransient => TableAdmission::RefuseTransient,
        }
    }
}

fn default_transient_threshold() -> Duration {
    Duration::from_secs(7 * 24 * 3600)
}

fn default_decision_cache_ttl() -> Duration {
    Duration::from_secs(10 * 60)
}

impl Default for TransientAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            transient_threshold: default_transient_threshold(),
            decision_cache_ttl: default_decision_cache_ttl(),
            overrides: HashMap::new(),
        }
    }
}

/// Per-table cached decision with the moment it was refreshed.
#[derive(Debug, Clone, Copy)]
struct CachedDecision {
    admission: TableAdmission,
    refreshed_at: Instant,
}

/// Pluggable read surface for the `metadata.json` refresh path.
///
/// Production wires an [`S3MetadataReader`] (below) backed by an
/// `aws_sdk_s3::Client` plus a per-`schema.table` `(bucket,
/// key_prefix)` lookup. Tests construct in-memory mocks that return
/// canned bytes (or simulate a 5xx error) without standing up a
/// MinIO container, mirroring the convention used by
/// `crate::rewarm_poller`'s `MockMetadataSource` test surface.
///
/// `Ok(bytes)` ⇒ raw `metadata.json` payload to be parsed.
/// `Err(_)` ⇒ refresh failed; the gate stays at fail-open
/// (`Admit`) and bumps `shelf_transient_refresh_errors_total`.
pub trait MetadataReader: Send + Sync + 'static {
    fn fetch_metadata_json<'a>(
        &'a self,
        table_label: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Vec<u8>>>;
}

/// The transient-admission gate.
///
/// Hot-path is `O(1)`: one `RwLock` read + a `HashMap::get`.
/// Refresh is single-flight via the in-flight `HashSet`, spawned
/// onto the ambient tokio runtime when [`TransientGate::decide`]
/// observes a missing or expired entry.
pub struct TransientGate {
    cfg: TransientAdmissionConfig,
    decisions: Arc<RwLock<HashMap<String, CachedDecision>>>,
    in_flight: Arc<Mutex<HashSet<String>>>,
    /// `Some` when a refresher is wired (production with operator
    /// opt-in, or a test with a mock). `None` is the v1 default
    /// for OSS deployments that ship without a refresher: the
    /// `overrides` map is the only signal source.
    reader: Option<Arc<dyn MetadataReader>>,
}

impl std::fmt::Debug for TransientGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransientGate")
            .field("enabled", &self.cfg.enabled)
            .field("transient_threshold", &self.cfg.transient_threshold)
            .field("decision_cache_ttl", &self.cfg.decision_cache_ttl)
            .field("overrides", &self.cfg.overrides.len())
            .field("decisions_cached", &self.decisions.read().len())
            .field("reader_wired", &self.reader.is_some())
            .finish()
    }
}

impl TransientGate {
    /// Construct a gate from operator config without a refresher.
    /// Decisions come from the `overrides` map only; metadata-based
    /// flags require [`TransientGate::with_reader`].
    pub fn new(cfg: TransientAdmissionConfig) -> Self {
        Self {
            cfg,
            decisions: Arc::new(RwLock::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            reader: None,
        }
    }

    /// Builder: attach a [`MetadataReader`] so the gate can
    /// refresh decisions from `metadata.json`. Without a reader
    /// the gate falls back on the `overrides` map.
    pub fn with_reader(mut self, reader: Arc<dyn MetadataReader>) -> Self {
        self.reader = Some(reader);
        self
    }

    /// `true` when the master switch is on.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Number of decisions currently held in the in-memory cache.
    /// Used by the `shelf_transient_decisions_cached` gauge.
    pub fn decisions_cached(&self) -> usize {
        self.decisions.read().len()
    }

    /// Returns the admission decision for the given table. Hot-path
    /// safe: lock-free in the cache-hit case (RwLock read), no
    /// `await`. Override > metadata > default. Always returns
    /// `Admit` when the gate is disabled or `table_label == "other"`.
    ///
    /// When the cache is missing or stale the gate spawns a
    /// background refresh (single-flight via the in-flight set) and
    /// returns `Admit` (fail-open) so a cold cache never blocks
    /// admits.
    pub fn decide(&self, table_label: &str) -> TableAdmission {
        if !self.cfg.enabled {
            return TableAdmission::Admit;
        }
        if table_label == "other" {
            return TableAdmission::Admit;
        }
        // Override wins over metadata. Look up FIRST so an explicit
        // "Admit" override does not waste a metadata fetch.
        if let Some(override_val) = self.cfg.overrides.get(table_label) {
            return override_val.into_admission();
        }

        let now = Instant::now();
        let cached = self.decisions.read().get(table_label).copied();

        let needs_refresh = match cached {
            Some(cd) => now.duration_since(cd.refreshed_at) >= self.cfg.decision_cache_ttl,
            None => true,
        };

        if needs_refresh {
            self.maybe_spawn_refresh(table_label.to_owned());
        }

        match cached {
            Some(cd) => cd.admission,
            None => TableAdmission::Admit, // fail-open
        }
    }

    /// Single-flight gate: only spawn a refresh task if no other
    /// caller has one in flight for the same table_label. Without a
    /// reader, this is a no-op.
    fn maybe_spawn_refresh(&self, table_label: String) {
        let Some(reader) = self.reader.clone() else {
            return;
        };
        // Single-flight: insert into the in-flight set; bail if
        // someone else already owns the slot.
        {
            let mut in_flight = self.in_flight.lock();
            if !in_flight.insert(table_label.clone()) {
                return;
            }
        }
        let decisions = self.decisions.clone();
        let in_flight = self.in_flight.clone();
        let cfg = self.cfg.clone();
        // The refresh task spawns onto whatever tokio runtime called
        // `decide`. Tests run inside `#[tokio::test]` so the runtime
        // is always present; production callers go through
        // `FoyerStore::get_or_fetch` which is itself async.
        tokio::spawn(async move {
            let outcome = refresh_decision_for(&table_label, reader.as_ref(), &cfg).await;
            // Always release the in-flight slot, even on error.
            in_flight.lock().remove(&table_label);
            match outcome {
                Ok(admission) => {
                    let mut g = decisions.write();
                    g.insert(
                        table_label.clone(),
                        CachedDecision {
                            admission,
                            refreshed_at: Instant::now(),
                        },
                    );
                    crate::metrics::TRANSIENT_DECISIONS_CACHED.set(g.len() as i64);
                }
                Err(e) => {
                    crate::metrics::TRANSIENT_REFRESH_ERRORS_TOTAL
                        .with_label_values(&[&table_label])
                        .inc();
                    tracing::debug!(
                        target: "shelfd::transient_admission",
                        table = %table_label,
                        error = %e,
                        "transient policy refresh failed; falling open to admit"
                    );
                }
            }
        });
    }
}

/// Read + parse the `metadata.json` for `table_label` and distil it
/// into a [`TableAdmission`]. Public so the production wiring in
/// `main.rs` can wrap a custom reader for batch warm-up.
///
/// `shelf.cache-policy = transient` short-circuits straight to
/// `RefuseTransient`. Otherwise the gate inspects
/// `history.expire.max-snapshot-age-ms` — if it is set and below
/// `cfg.transient_threshold`, the table is transient. Anything
/// else (missing properties, parse-only-readable shape, retention
/// above threshold) ⇒ `Admit`.
pub async fn refresh_decision_for(
    table_label: &str,
    reader: &dyn MetadataReader,
    cfg: &TransientAdmissionConfig,
) -> anyhow::Result<TableAdmission> {
    let bytes = reader.fetch_metadata_json(table_label).await?;
    let parsed: MetadataJson =
        serde_json::from_slice(&bytes).map_err(|e| anyhow::anyhow!("metadata.json parse: {e}"))?;
    Ok(parsed.distil(cfg))
}

/// Slice of `metadata.json` we actually care about. Iceberg's
/// metadata schema is forward-compatible and serde tolerates
/// unknown fields by default — we never parse what we don't read.
#[derive(Debug, Default, Deserialize)]
struct MetadataJson {
    #[serde(default)]
    properties: HashMap<String, String>,
}

impl MetadataJson {
    fn distil(&self, cfg: &TransientAdmissionConfig) -> TableAdmission {
        // Highest-priority property: explicit `shelf.cache-policy`.
        if let Some(policy) = self.properties.get("shelf.cache-policy") {
            if policy.eq_ignore_ascii_case("transient") {
                return TableAdmission::RefuseTransient;
            }
            // Any other value ⇒ explicit admit.
            return TableAdmission::Admit;
        }

        // Iceberg snapshot retention. `history.expire.max-snapshot-age-ms`
        // is the canonical knob; `min-snapshots-to-keep` on its own is
        // not enough (default 1 even on long-lived tables).
        if let Some(age_ms) = self
            .properties
            .get("history.expire.max-snapshot-age-ms")
            .and_then(|s| s.parse::<u64>().ok())
        {
            let max_age = Duration::from_millis(age_ms);
            if max_age < cfg.transient_threshold {
                return TableAdmission::RefuseTransient;
            }
        }

        TableAdmission::Admit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Shared mock used by the threshold / shelf-cache-policy /
    /// concurrent-refresh tests. Records call counts so the
    /// single-flight assertion has a quantitative anchor.
    type MockBody = Arc<dyn Fn(&str) -> anyhow::Result<Vec<u8>> + Send + Sync>;

    struct MockReader {
        body: MockBody,
        calls: Arc<AtomicU32>,
        delay: Duration,
    }

    impl MockReader {
        fn new<F>(body: F) -> (Arc<Self>, Arc<AtomicU32>)
        where
            F: Fn(&str) -> anyhow::Result<Vec<u8>> + Send + Sync + 'static,
        {
            let calls = Arc::new(AtomicU32::new(0));
            (
                Arc::new(Self {
                    body: Arc::new(body),
                    calls: calls.clone(),
                    delay: Duration::ZERO,
                }),
                calls,
            )
        }

        fn with_delay<F>(body: F, delay: Duration) -> (Arc<Self>, Arc<AtomicU32>)
        where
            F: Fn(&str) -> anyhow::Result<Vec<u8>> + Send + Sync + 'static,
        {
            let calls = Arc::new(AtomicU32::new(0));
            (
                Arc::new(Self {
                    body: Arc::new(body),
                    calls: calls.clone(),
                    delay,
                }),
                calls,
            )
        }
    }

    impl MetadataReader for MockReader {
        fn fetch_metadata_json<'a>(
            &'a self,
            table_label: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<Vec<u8>>> {
            let calls = self.calls.clone();
            let body = self.body.clone();
            let delay = self.delay;
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                calls.fetch_add(1, Ordering::SeqCst);
                body(table_label)
            })
        }
    }

    fn metadata_json_with_age_ms(age_ms: u64) -> Vec<u8> {
        format!(
            r#"{{
                "properties": {{
                    "history.expire.max-snapshot-age-ms": "{age_ms}"
                }}
            }}"#
        )
        .into_bytes()
    }

    fn enabled_cfg(threshold: Duration, ttl: Duration) -> TransientAdmissionConfig {
        TransientAdmissionConfig {
            enabled: true,
            transient_threshold: threshold,
            decision_cache_ttl: ttl,
            overrides: HashMap::new(),
        }
    }

    /// Tiny helper to drain spawned refresh tasks: yields + sleeps
    /// up to N times so the background `tokio::spawn` has a chance
    /// to land a write into the `decisions` map. Yields first
    /// (cheap) then sleeps 1ms (covers multi_thread workers and any
    /// in-test simulated I/O delays). Total bound: ~256 ms by default.
    async fn yield_until<F: FnMut() -> bool>(mut cond: F, max_iters: u32) -> bool {
        for _ in 0..max_iters {
            if cond() {
                return true;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        cond()
    }

    /// Lock in the YAML serialization of `OverrideValue`. The Helm
    /// `values.yaml` and `configmap-shelfd.yaml` template both
    /// emit `admit` / `refuseTransient`; a serde-rename drift here
    /// would silently turn an operator override into a parse
    /// failure on pod boot.
    #[test]
    fn override_value_yaml_serialization() {
        let yaml = r#"
foo.bar: admit
baz.qux: refuseTransient
"#;
        let parsed: HashMap<String, OverrideValue> =
            serde_yaml::from_str(yaml).expect("parse overrides");
        assert_eq!(parsed.get("foo.bar"), Some(&OverrideValue::Admit));
        assert_eq!(parsed.get("baz.qux"), Some(&OverrideValue::RefuseTransient));
    }

    /// `enabled = false` ⇒ every call admits regardless of
    /// configuration. The OSS default. No metadata is fetched.
    #[tokio::test]
    async fn disabled_admits_all() {
        let mut cfg = TransientAdmissionConfig::default();
        cfg.overrides
            .insert("foo.bar".to_owned(), OverrideValue::RefuseTransient);
        let gate = TransientGate::new(cfg);
        for _ in 0..1_000 {
            assert_eq!(gate.decide("foo.bar"), TableAdmission::Admit);
            assert_eq!(gate.decide("anything"), TableAdmission::Admit);
        }
    }

    /// `decide("other")` is the sentinel returned by
    /// `s3_shim::table_label` for any non-Iceberg path. The gate
    /// must admit unconditionally so non-cache traffic is never
    /// touched.
    #[tokio::test]
    async fn unknown_table_admits() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let gate = TransientGate::new(cfg);
        assert_eq!(gate.decide("other"), TableAdmission::Admit);
    }

    /// First decide on an unknown table (no cached value, no
    /// reader) ⇒ fail-open `Admit`. The single-flight refresh path
    /// MUST NOT block the call.
    #[tokio::test]
    async fn unknown_table_admits_when_no_metadata_fetched_yet() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let gate = TransientGate::new(cfg);
        // No reader, no override — fail-open.
        assert_eq!(gate.decide("schema.brand_new"), TableAdmission::Admit);
        assert_eq!(gate.decisions_cached(), 0);
    }

    /// Explicit `Admit` override beats a metadata signal that would
    /// otherwise refuse. The operator gets the final word.
    #[tokio::test]
    async fn explicit_override_admit_wins() {
        let mut cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        cfg.overrides
            .insert("foo.bar".to_owned(), OverrideValue::Admit);
        // Reader would otherwise return RefuseTransient, but we
        // never get to consult it — overrides short-circuit first.
        let (reader, calls) = MockReader::new(|_| Ok(metadata_json_with_age_ms(1)));
        let gate = TransientGate::new(cfg).with_reader(reader);
        assert_eq!(gate.decide("foo.bar"), TableAdmission::Admit);
        // No fetch was even scheduled — overrides hit before the
        // refresh-spawn site.
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// Symmetric: explicit `RefuseTransient` override wins even if
    /// metadata says the table has generous retention.
    #[tokio::test]
    async fn explicit_override_refuse_wins() {
        let mut cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        cfg.overrides
            .insert("foo.bar".to_owned(), OverrideValue::RefuseTransient);
        let (reader, calls) = MockReader::new(|_| {
            // 30 days retention — generous, would normally Admit.
            Ok(metadata_json_with_age_ms(30 * 86_400 * 1_000))
        });
        let gate = TransientGate::new(cfg).with_reader(reader);
        assert_eq!(gate.decide("foo.bar"), TableAdmission::RefuseTransient);
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// Threshold = 7 days, mock retention = 3 days ⇒
    /// `RefuseTransient` after the background refresh lands.
    #[tokio::test]
    async fn metadata_below_threshold_refuses() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let (reader, calls) =
            MockReader::new(|_| Ok(metadata_json_with_age_ms(3 * 86_400 * 1_000)));
        let gate = TransientGate::new(cfg).with_reader(reader);
        // First decide spawns the refresh; fail-open Admit.
        assert_eq!(gate.decide("dbt.scratch"), TableAdmission::Admit);
        // Wait for the refresh to land.
        let landed = yield_until(|| gate.decisions_cached() >= 1, 256).await;
        assert!(landed, "refresh did not populate cache");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Second decide observes the cached refusal.
        assert_eq!(gate.decide("dbt.scratch"), TableAdmission::RefuseTransient);
    }

    /// Threshold = 7 days, mock retention = 30 days ⇒ Admit.
    #[tokio::test]
    async fn metadata_above_threshold_admits() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let (reader, _calls) =
            MockReader::new(|_| Ok(metadata_json_with_age_ms(30 * 86_400 * 1_000)));
        let gate = TransientGate::new(cfg).with_reader(reader);
        assert_eq!(gate.decide("analytics.events"), TableAdmission::Admit);
        let landed = yield_until(|| gate.decisions_cached() >= 1, 256).await;
        assert!(landed);
        assert_eq!(gate.decide("analytics.events"), TableAdmission::Admit);
    }

    /// First decide caches the decision; advancing past the TTL
    /// triggers another refresh. Uses a 1ms TTL so the test runs
    /// in real time without `tokio::time::pause()` (which would
    /// also pause the spawned refresh's `sleep`s).
    #[tokio::test]
    async fn decision_cache_ttl_expiry_re_refreshes() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_millis(1));
        let (reader, calls) =
            MockReader::new(|_| Ok(metadata_json_with_age_ms(3 * 86_400 * 1_000)));
        let gate = TransientGate::new(cfg).with_reader(reader);
        gate.decide("dbt.scratch");
        let landed = yield_until(|| calls.load(Ordering::SeqCst) >= 1, 256).await;
        assert!(landed, "first refresh did not run");
        // Sleep past the TTL so the next decide observes an
        // expired cached entry.
        tokio::time::sleep(Duration::from_millis(5)).await;
        gate.decide("dbt.scratch");
        let landed = yield_until(|| calls.load(Ordering::SeqCst) >= 2, 256).await;
        assert!(landed, "TTL-expiry refresh did not run");
    }

    /// First decide spawns + caches; second decide within TTL just
    /// reads the cached value with no extra fetch.
    #[tokio::test]
    async fn decision_cache_hit_no_refresh() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let (reader, calls) =
            MockReader::new(|_| Ok(metadata_json_with_age_ms(30 * 86_400 * 1_000)));
        let gate = TransientGate::new(cfg).with_reader(reader);
        gate.decide("analytics.events");
        let landed = yield_until(|| gate.decisions_cached() >= 1, 256).await;
        assert!(landed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Several more decides within the TTL window.
        for _ in 0..100 {
            assert_eq!(gate.decide("analytics.events"), TableAdmission::Admit);
        }
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no extra fetches");
    }

    /// Mock returns an error on every fetch ⇒ gate stays at
    /// fail-open `Admit` AND the
    /// `shelf_transient_refresh_errors_total` counter ticks.
    #[tokio::test]
    async fn s3_error_during_refresh_admits_default() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let (reader, calls) =
            MockReader::new(|_| Err(anyhow::anyhow!("simulated S3 500 InternalError")));
        let gate = TransientGate::new(cfg).with_reader(reader);

        let before = crate::metrics::TRANSIENT_REFRESH_ERRORS_TOTAL
            .with_label_values(&["bad.table"])
            .get();

        assert_eq!(gate.decide("bad.table"), TableAdmission::Admit);
        // Wait for the refresh task to attempt + fail.
        let attempted = yield_until(|| calls.load(Ordering::SeqCst) >= 1, 256).await;
        assert!(attempted, "refresh task never ran");
        // Decision cache stays empty (no successful refresh).
        assert_eq!(gate.decisions_cached(), 0);
        // Subsequent decides keep returning Admit (fail-open).
        assert_eq!(gate.decide("bad.table"), TableAdmission::Admit);
        let after = crate::metrics::TRANSIENT_REFRESH_ERRORS_TOTAL
            .with_label_values(&["bad.table"])
            .get();
        assert!(
            after > before,
            "TRANSIENT_REFRESH_ERRORS_TOTAL did not increment (before={before}, after={after})"
        );
    }

    /// `shelf.cache-policy = transient` short-circuits straight to
    /// `RefuseTransient` even when retention is generous (30 days).
    /// Tests the "operator-blessed via property" path that does
    /// not require touching values.yaml.
    #[tokio::test]
    async fn shelf_cache_policy_property_respected() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        let (reader, _calls) = MockReader::new(|_| {
            Ok(br#"{
                "properties": {
                    "shelf.cache-policy": "transient",
                    "history.expire.max-snapshot-age-ms": "2592000000"
                }
            }"#
            .to_vec())
        });
        let gate = TransientGate::new(cfg).with_reader(reader);
        gate.decide("scratch.tmp_join");
        let landed = yield_until(|| gate.decisions_cached() >= 1, 256).await;
        assert!(landed);
        assert_eq!(
            gate.decide("scratch.tmp_join"),
            TableAdmission::RefuseTransient
        );
    }

    /// 100 concurrent decides for the same uncached table issue
    /// exactly ONE metadata.json fetch — the in-flight HashSet
    /// deduplicates the spawn. Any breakage of the single-flight
    /// pattern would manifest as `calls > 1`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_refresh_does_not_double_fetch() {
        let cfg = enabled_cfg(Duration::from_secs(7 * 86_400), Duration::from_secs(600));
        // Add a small delay so all 100 decides race the spawn site
        // before the first refresh task can release the in-flight
        // slot. Without the delay, the first refresh might complete
        // synchronously fast enough that some later decides would
        // legitimately spawn a second refresh.
        let (reader, calls) = MockReader::with_delay(
            |_| Ok(metadata_json_with_age_ms(3 * 86_400 * 1_000)),
            Duration::from_millis(20),
        );
        let gate = Arc::new(TransientGate::new(cfg).with_reader(reader));

        let mut handles = Vec::new();
        for _ in 0..100 {
            let g = gate.clone();
            handles.push(tokio::spawn(async move { g.decide("hot.contended_table") }));
        }
        for h in handles {
            let _ = h.await;
        }
        // Wait for the single in-flight fetch to land.
        let landed = yield_until(|| gate.decisions_cached() >= 1, 256).await;
        assert!(landed, "refresh did not populate cache");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "expected exactly one metadata.json fetch, got {}",
            calls.load(Ordering::SeqCst)
        );
    }
}
