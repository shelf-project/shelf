//! SHELF-24 — pin-list loader.
//!
//! The loader pulls `s3://<bucket>/<key>` (default
//! `shelf/pin_list.json`) on boot, reinstalls the pinned keys into
//! [`crate::store::FoyerStore`], and then refreshes on both a
//! configurable timer (default 15 min) and `SIGHUP`. The admin route
//! `POST /admin/reload` reuses the same refresh path through a
//! [`ReloadHandle`].
//!
//! ## Reload semantics — **replacing**
//!
//! Each refresh computes the diff between the previously-installed
//! set and the newly-fetched set:
//!
//! - keys present before AND after: left pinned;
//! - keys present only before: unpinned;
//! - keys present only after: pinned.
//!
//! This matches the way `pin_list.json` is maintained — as a
//! declarative list in the `shelf-config` repo. An additive pin-set
//! would leak pins that have been removed from the JSON. Trade-offs
//! are called out in
//! `shelfd/docs/design-notes/SHELF-23-24-admin-surface-and-pinlist.md`.

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::store::{FoyerStore, Key};

/// Per-reload summary returned by [`ReloadHandle::reload_now`] and the
/// `POST /admin/reload` HTTP handler. Alias to [`ReloadReport`] for
/// backwards compatibility with the HTTP handler wire shape —
/// pinned_bytes + pinned_count are the two fields the handler
/// projects back to JSON.
pub type ReloadStats = ReloadReport;

/// JSON row inside `pin_list.json`. Kept `pub(crate)` so the unit
/// tests can construct fixtures.
///
/// Pool is required so the loader knows which Foyer cache to query
/// on pin. Lower-case `"metadata"` / `"rowgroup"`; anything else is
/// logged WARN and skipped (non-fatal).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PinListEntry {
    pub(crate) key_hex: String,
    pub(crate) pool: String,
}

/// Wrapper document.
///
/// ```json
/// { "version": 1, "entries": [{"key_hex": "...", "pool": "metadata"}, ...] }
/// ```
///
/// We version the wrapper so a v2 can introduce breaking changes
/// (TTLs, priority, per-entry labels) without a silent misparse.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PinListDoc {
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) version: u32,
    pub(crate) entries: Vec<PinListEntry>,
}

/// Per-reload diff summary (ticket §4 SHELF-24). Added / removed are
/// counts relative to the previous in-memory pin-set; `skipped_missing`
/// tallies list entries that could not be pinned because the key was
/// not resident in the declared pool.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct ReloadReport {
    pub pinned_bytes: u64,
    pub pinned_count: usize,
    pub added: usize,
    pub removed: usize,
    pub skipped_missing: usize,
}

/// Handle to the running pin-list loader task. Clonable — every clone
/// talks to the same background task via a bounded `mpsc` channel.
#[derive(Debug, Clone)]
pub struct ReloadHandle {
    inner: Arc<ReloadInner>,
}

#[derive(Debug)]
struct ReloadInner {
    tx: mpsc::Sender<oneshot::Sender<crate::Result<ReloadStats>>>,
}

impl ReloadHandle {
    /// Trigger a reload NOW and await the refreshed stats. The
    /// outer `Err` means "loader task unreachable"; the inner
    /// `Err` comes from the reload itself (S3, JSON parse, …).
    pub async fn reload_now(&self) -> crate::Result<ReloadStats> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(tx)
            .await
            .map_err(|_| crate::Error::Config("pin-list loader task is not running".into()))?;
        rx.await
            .map_err(|_| crate::Error::Config("pin-list loader dropped response".into()))?
    }
}

/// Configuration + wiring for the background loader.
#[derive(Debug)]
pub struct PinListLoader {
    client: S3Client,
    bucket: String,
    key: String,
    period: Duration,
    store: Arc<FoyerStore>,
}

impl PinListLoader {
    pub fn new(
        client: S3Client,
        bucket: String,
        key: String,
        period: Duration,
        store: Arc<FoyerStore>,
    ) -> Self {
        Self {
            client,
            bucket,
            key,
            period,
            store,
        }
    }

    /// Fetch `pin_list.json` once and then spawn the refresh loop.
    /// Returns the `(ReloadHandle, JoinHandle)` pair.
    ///
    /// A failing initial fetch is logged but does not abort boot: the
    /// daemon stays functional with an empty pin-set while the loader
    /// keeps retrying on its timer.
    pub async fn boot_and_spawn(
        self,
        shutdown: CancellationToken,
    ) -> crate::Result<(ReloadHandle, JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel::<oneshot::Sender<crate::Result<ReloadStats>>>(8);
        let handle = ReloadHandle {
            inner: Arc::new(ReloadInner { tx }),
        };

        match self.reload_once().await {
            Ok(stats) => {
                tracing::info!(
                    bucket = %self.bucket,
                    key = %self.key,
                    pinned_bytes = stats.pinned_bytes,
                    pinned_count = stats.pinned_count,
                    "pin-list initial load",
                );
            }
            Err(e) => {
                tracing::warn!(
                    bucket = %self.bucket,
                    key = %self.key,
                    error = %e,
                    "pin-list initial load failed; continuing with empty pin-set",
                );
            }
        }

        let sighup = Arc::new(Notify::new());
        spawn_sighup_listener(sighup.clone(), shutdown.clone());
        let join = tokio::spawn(run_loader(self, rx, sighup, shutdown));
        Ok((handle, join))
    }

    /// Fetch `pin_list.json` and apply replace-semantics to the pin-set.
    ///
    /// "Replace" means the file is authoritative: any key the daemon
    /// was previously pinning but which no longer appears in the JSON
    /// gets **un-pinned**. The design note has the trade-off.
    pub async fn reload_once(&self) -> crate::Result<ReloadReport> {
        let entries = self.fetch_pin_list().await?;
        self.apply_entries(entries).await
    }

    /// Apply a pre-parsed entry list. Split out from `reload_once` so
    /// the unit tests can exercise replace-semantics without an S3
    /// client.
    async fn apply_entries(&self, entries: Vec<PinListEntry>) -> crate::Result<ReloadReport> {
        use crate::store::Pool;

        let mut desired: std::collections::HashMap<Key, Pool> =
            std::collections::HashMap::with_capacity(entries.len());
        for entry in &entries {
            let pool = match entry.pool.as_str() {
                "metadata" => Pool::Metadata,
                "rowgroup" => Pool::RowGroup,
                other => {
                    tracing::warn!(
                        key_hex = %entry.key_hex,
                        pool = other,
                        "pin-list entry has unknown pool — skipping",
                    );
                    continue;
                }
            };
            match Key::from_hex(&entry.key_hex) {
                Ok(key) => {
                    if let Some(prev) = desired.insert(key.clone(), pool) {
                        if prev != pool {
                            tracing::warn!(
                                key_hex = %entry.key_hex,
                                previous_pool = ?prev,
                                replacing_with = ?pool,
                                "pin-list has duplicate key_hex with conflicting pool — last entry wins",
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        key_hex = %entry.key_hex,
                        error = %e,
                        "pin-list entry has malformed key_hex — skipping",
                    );
                }
            }
        }

        // Diff against the current in-memory pin-set. The store's
        // `pinned_keys()` now returns `(Pool, Key)` pairs so we can
        // spot pool-drift (same key, different pool) as an unpin +
        // re-pin rather than a silent no-op.
        let current: std::collections::HashSet<(Pool, Key)> =
            self.store.pinned_keys().into_iter().collect();
        let desired_set: std::collections::HashSet<(Pool, Key)> = desired
            .iter()
            .map(|(key, pool)| (*pool, key.clone()))
            .collect();

        let mut removed = 0usize;
        for (pool, gone) in current.difference(&desired_set) {
            // `unpin` is pool-agnostic: a SHELF-04 key is unique per
            // pool, so there is at most one entry to drop.
            if self.store.unpin(gone) {
                removed += 1;
                // Track E8 — reload-driven unpin is a distinct
                // reason from admin-evict or capacity-evict.
                let pool_label = match pool {
                    Pool::Metadata => "metadata",
                    Pool::RowGroup => "rowgroup",
                };
                crate::metrics::EVICTIONS_TOTAL
                    .with_label_values(&[pool_label, "reload"])
                    .inc();
            }
        }
        let mut added = 0usize;
        let mut skipped_missing = 0usize;
        for (key, pool) in &desired {
            if current.contains(&(*pool, key.clone())) {
                continue;
            }
            if self.store.pin(*pool, key) {
                added += 1;
            } else {
                skipped_missing += 1;
                tracing::warn!(
                    key_hex = %key.to_hex(),
                    pool = ?pool,
                    "pin-list references a key that is not resident in the declared pool",
                );
            }
        }

        Ok(ReloadReport {
            pinned_bytes: self.store.pinned_bytes(),
            pinned_count: self.store.pinned_count(),
            added,
            removed,
            skipped_missing,
        })
    }

    async fn fetch_pin_list(&self) -> crate::Result<Vec<PinListEntry>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await
            .map_err(|e| {
                crate::Error::Origin(format!("GetObject s3://{}/{}: {e}", self.bucket, self.key))
            })?;
        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| crate::Error::Origin(format!("collect pin_list.json body: {e}")))?;
        let bytes = body.into_bytes();
        parse_pin_list(&bytes)
    }
}

/// Parse a `pin_list.json` payload. Extracted so unit tests can hit
/// it without touching S3.
///
/// The expected top-level shape is a JSON object
/// `{"entries": [...], "version": 1}`; a bare array is **rejected**
/// so a typo on the wrapper is caught at parse time rather than
/// silently loading zero entries.
pub(crate) fn parse_pin_list(bytes: &[u8]) -> crate::Result<Vec<PinListEntry>> {
    let doc: PinListDoc = serde_json::from_slice(bytes)
        .map_err(|e| crate::Error::Config(format!("pin_list.json parse: {e}")))?;
    Ok(doc.entries)
}

async fn run_loader(
    loader: PinListLoader,
    mut rx: mpsc::Receiver<oneshot::Sender<crate::Result<ReloadStats>>>,
    sighup: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(loader.period);
    // Skip the immediate tick — `boot_and_spawn` already did it.
    interval.reset();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("pin-list loader shutting down");
                return;
            }
            maybe_req = rx.recv() => {
                let Some(req) = maybe_req else { return; };
                let result = loader.reload_once().await;
                let _ = req.send(result);
            }
            _ = sighup.notified() => {
                tracing::info!("pin-list loader: SIGHUP");
                match loader.reload_once().await {
                    Ok(stats) => tracing::info!(
                        pinned_bytes = stats.pinned_bytes,
                        pinned_count = stats.pinned_count,
                        "pin-list reloaded on SIGHUP",
                    ),
                    Err(e) => tracing::warn!(error = %e, "pin-list SIGHUP reload failed"),
                }
            }
            _ = interval.tick() => {
                match loader.reload_once().await {
                    Ok(stats) => tracing::debug!(
                        pinned_bytes = stats.pinned_bytes,
                        pinned_count = stats.pinned_count,
                        "pin-list periodic reload",
                    ),
                    Err(e) => tracing::warn!(error = %e, "pin-list periodic reload failed"),
                }
            }
        }
    }
}

/// SIGHUP plumbing. `#[cfg(unix)]`-gated; on other platforms the
/// function is a no-op and the timer + admin handle remain live.
#[cfg(unix)]
fn spawn_sighup_listener(notify: Arc<Notify>, shutdown: CancellationToken) {
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sig = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "SIGHUP handler setup failed");
                return;
            }
        };
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                maybe = sig.recv() => {
                    if maybe.is_none() {
                        return;
                    }
                    notify.notify_one();
                }
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_sighup_listener(_notify: Arc<Notify>, _shutdown: CancellationToken) {
    // No SIGHUP on non-unix platforms.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig};
    use crate::store::{key_from_tuple, FoyerStore, Pool, Store};
    use bytes::Bytes;

    #[test]
    fn parses_pinlist_json_with_pool_hints() {
        let body = br#"{
            "version": 1,
            "entries": [
                {"key_hex":"0000000000000000000000000000000000000000000000000000000000000001","pool":"metadata"},
                {"key_hex":"0000000000000000000000000000000000000000000000000000000000000002","pool":"rowgroup"}
            ]
        }"#;
        let parsed = parse_pin_list(body).expect("ok");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].pool, "metadata");
        assert_eq!(parsed[1].pool, "rowgroup");
        assert_eq!(parsed[0].key_hex.len(), 64);
    }

    /// The v1 schema requires the `{entries: [...]}` wrapper — a bare
    /// JSON array must be rejected so a typo does not silently load
    /// zero entries.
    #[test]
    fn rejects_bare_array_root() {
        let body = br#"[{"key_hex":"00","pool":"metadata"}]"#;
        assert!(parse_pin_list(body).is_err());
    }

    #[test]
    fn rejects_non_array_root() {
        // Missing `entries` field → parse error.
        let body = br#"{"pins":[]}"#;
        assert!(parse_pin_list(body).is_err());
    }

    /// Per-entry garbage hex must produce a WARN log inside
    /// `reload_once` but must not panic or abort the reload. We
    /// check the parser's tolerance here and the diff side of
    /// `apply_entries` in `reload_is_replace_not_additive`.
    #[test]
    fn ignores_unknown_keys_at_warn_level() {
        let body = br#"{
            "version": 1,
            "entries": [
                {"key_hex":"not-hex","pool":"metadata"},
                {"key_hex":"0000000000000000000000000000000000000000000000000000000000000004","pool":"rowgroup"}
            ]
        }"#;
        let parsed = parse_pin_list(body).expect("top-level parse must succeed");
        assert_eq!(parsed.len(), 2);
        assert!(Key::from_hex(&parsed[0].key_hex).is_err());
        assert!(Key::from_hex(&parsed[1].key_hex).is_ok());
    }

    fn test_pools() -> PoolsConfig {
        PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir: std::path::PathBuf::from("/tmp/unused"),
                nvme_bytes: 0,
                eviction_policy: crate::config::EvictionPolicy::default(),
                disk_cache: crate::config::RowGroupDiskCacheConfig::default(),
            },
        }
    }

    /// A "stub origin" in this context means: skip S3 entirely and
    /// drive the diff path via [`PinListLoader::apply_entries`]. The
    /// function is crate-private specifically so the tests can reach
    /// in and validate replace-semantics without a real bucket.
    #[tokio::test]
    async fn reload_is_replace_not_additive() {
        // Seed two keys, both resident in RowGroup.
        let store = Arc::new(FoyerStore::open(&test_pools()).await.expect("open"));
        let k_a = key_from_tuple(b"a", 0, 1, 0).expect("k_a");
        let k_b = key_from_tuple(b"b", 0, 1, 0).expect("k_b");
        store
            .insert(Pool::RowGroup, k_a.clone(), Bytes::from_static(&[1u8; 8]))
            .await
            .expect("seed a");
        store
            .insert(Pool::RowGroup, k_b.clone(), Bytes::from_static(&[2u8; 16]))
            .await
            .expect("seed b");

        // Build a loader with placeholder S3 config — we never call
        // `reload_once` / `fetch_pin_list` in this test.
        let aws_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region("us-east-1")
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&aws_cfg);
        let loader = PinListLoader::new(
            client,
            "unused".into(),
            "unused".into(),
            Duration::from_secs(9999),
            store.clone(),
        );

        // First application: pin both keys.
        let payload_x = vec![
            PinListEntry {
                key_hex: k_a.to_hex(),
                pool: "rowgroup".into(),
            },
            PinListEntry {
                key_hex: k_b.to_hex(),
                pool: "rowgroup".into(),
            },
        ];
        let r1 = loader.apply_entries(payload_x).await.expect("r1");
        assert_eq!(r1.pinned_count, 2);
        assert_eq!(r1.added, 2);
        assert_eq!(r1.removed, 0);
        assert!(store.is_pinned(&k_a));
        assert!(store.is_pinned(&k_b));

        // Second application: payload Y drops k_a, keeps k_b. The
        // post-state pin-set must equal Y exactly (replace), not the
        // union of X and Y (which would still contain k_a).
        let payload_y = vec![PinListEntry {
            key_hex: k_b.to_hex(),
            pool: "rowgroup".into(),
        }];
        let r2 = loader.apply_entries(payload_y).await.expect("r2");
        assert_eq!(r2.pinned_count, 1);
        assert_eq!(r2.added, 0);
        assert_eq!(r2.removed, 1);
        assert!(!store.is_pinned(&k_a), "k_a must be unpinned on replace");
        assert!(store.is_pinned(&k_b), "k_b must remain pinned");

        // Third application: empty list — everything unpins.
        let r3 = loader.apply_entries(Vec::new()).await.expect("r3");
        assert_eq!(r3.pinned_count, 0);
        assert_eq!(r3.removed, 1);
        assert!(!store.is_pinned(&k_b));
    }

    /// SHELF-24: entries for keys that are not resident in the declared
    /// pool are logged WARN and counted in `skipped_missing`, not fatal.
    #[tokio::test]
    async fn missing_entries_count_as_skipped_not_fatal() {
        let store = Arc::new(FoyerStore::open(&test_pools()).await.expect("open"));
        let aws_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region("us-east-1")
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&aws_cfg);
        let loader = PinListLoader::new(
            client,
            "unused".into(),
            "unused".into(),
            Duration::from_secs(9999),
            store.clone(),
        );
        let ghost = key_from_tuple(b"ghost", 0, 1, 0).expect("ghost");
        let report = loader
            .apply_entries(vec![PinListEntry {
                key_hex: ghost.to_hex(),
                pool: "rowgroup".into(),
            }])
            .await
            .expect("ok");
        assert_eq!(report.skipped_missing, 1);
        assert_eq!(report.added, 0);
        assert_eq!(report.pinned_count, 0);
    }
}
