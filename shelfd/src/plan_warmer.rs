//! Plan-fingerprint-driven row-group pre-warm (§8.2 from TODO-fix-shelf-performance.md).
//!
//! Canonicalises Trino's `jsonPlan` (literals erased, commutative operands sorted)
//! into a 64-bit fingerprint. For each fingerprint, maintains a small in-memory
//! histogram of `(file_etag, row_group_ordinal)` accessed by historical instances.
//! On the next `QueryCreatedEvent` with the same fingerprint, **pre-warms the
//! historical row groups before the split source asks for them**.
//!
//! # Why novel for OSS OLAP
//!
//! Snowflake's [result cache](https://docs.snowflake.com/en/user-guide/querying-persisted-results)
//! requires exact text matching plus identical parameters, role, micro-partitions,
//! and unchanged data — too narrow for BI dashboards that template literals into
//! predicates (each dashboard refresh has a different `WHERE date_col = '...'`).
//!
//! The mechanism here lives a level below the result cache: it doesn't memoise
//! *results*, it memoises *which row groups the query needs*, so a same-fingerprint
//! different-literal query gets a warm cache and full Trino execution.
//!
//! # Related work
//!
//! - [Quickstep's Lookahead Information Passing (Zhu et al., VLDB 2017)](https://vldb.org/pvldb/vol10/p889-zhu.pdf)
//!   shares predicates across joins in the same query; this module shares row-group
//!   locality across queries with the same plan shape.
//!
//! - [Cooperative Scans (Zukowski et al., VLDB 2007)](http://bibtex.github.io/VLDB-2007-ZukowskiHNB.html)
//!   coordinated concurrent scans of the same table; this module coordinates
//!   *historical* scans of the same plan shape.
//!
//! # Composes with
//!
//! - [`fingerprint.rs`] — implements `canonicalise(jsonPlan) -> 64-bit hash`
//! - [`ShelfPrefetchListener.java`] — intercepts `QueryCreatedEvent`
//! - [`prefetch.rs`] — handles the actual pre-fetch I/O
//!
//! See `TODO-fix-shelf-performance.md` §8.2.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Maximum number of fingerprints to track in the LRU.
const MAX_FINGERPRINTS: usize = 10_000;

/// Maximum number of row-group entries per fingerprint histogram.
const MAX_ROW_GROUPS_PER_FINGERPRINT: usize = 1_000;

/// TTL for fingerprint entries — evict if not accessed for this duration.
const FINGERPRINT_TTL: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

/// Represents a row-group that was accessed by a query.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RowGroupAccess {
    /// S3 bucket.
    pub bucket: String,
    /// S3 key (file path).
    pub key: String,
    /// ETag of the file.
    pub etag: String,
    /// Row group ordinal within the file.
    pub row_group_ordinal: u32,
    /// Byte offset in the file.
    pub offset: u64,
    /// Length in bytes.
    pub length: u64,
}

/// Histogram of row-groups accessed for a given fingerprint.
#[derive(Debug, Clone)]
struct RowGroupHistogram {
    /// Access counts per row-group.
    counts: HashMap<RowGroupAccess, u32>,
    /// Last access time for TTL eviction.
    last_accessed: Instant,
    /// Total number of queries that contributed to this histogram.
    query_count: u64,
}

impl RowGroupHistogram {
    fn new() -> Self {
        Self {
            counts: HashMap::new(),
            last_accessed: Instant::now(),
            query_count: 0,
        }
    }

    fn record_access(&mut self, rg: RowGroupAccess) {
        *self.counts.entry(rg).or_insert(0) += 1;
    }

    fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.query_count += 1;
    }

    fn is_expired(&self) -> bool {
        self.last_accessed.elapsed() > FINGERPRINT_TTL
    }

    /// Returns the top-N row groups by access count.
    fn top_row_groups(&self, n: usize) -> Vec<RowGroupAccess> {
        let mut entries: Vec<_> = self.counts.iter().collect();
        entries.sort_by(|a, b| b.1.cmp(a.1));
        entries.into_iter().take(n).map(|(k, _)| k.clone()).collect()
    }

    /// Prune to max size, keeping highest-count entries.
    fn prune_if_needed(&mut self) {
        if self.counts.len() > MAX_ROW_GROUPS_PER_FINGERPRINT {
            let mut entries: Vec<_> = self.counts.drain().collect();
            entries.sort_by(|a, b| b.1.cmp(&a.1));
            entries.truncate(MAX_ROW_GROUPS_PER_FINGERPRINT / 2);
            self.counts = entries.into_iter().collect();
        }
    }
}

/// Commands for the plan warmer.
#[derive(Debug)]
pub enum PlanWarmerCommand {
    /// A query started with this fingerprint — trigger pre-warm.
    QueryStarted {
        fingerprint: u64,
        query_id: String,
    },
    /// A query completed — record which row groups it accessed.
    QueryCompleted {
        fingerprint: u64,
        query_id: String,
        row_groups: Vec<RowGroupAccess>,
    },
    /// Shutdown the warmer.
    Shutdown,
}

/// Pre-warm request to send to the prefetch subsystem.
#[derive(Debug, Clone)]
pub struct PrewarmRequest {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub offset: u64,
    pub length: u64,
}

/// The plan warmer maintains fingerprint → row-group histograms and triggers
/// pre-warm on query start.
pub struct PlanWarmer {
    /// LRU of fingerprint histograms.
    histograms: Arc<RwLock<HashMap<u64, RowGroupHistogram>>>,
    /// Channel for receiving commands.
    rx: mpsc::Receiver<PlanWarmerCommand>,
    /// Channel for sending pre-warm requests.
    prewarm_tx: Option<mpsc::Sender<PrewarmRequest>>,
    /// Number of row groups to pre-warm per query start.
    prewarm_count: usize,
}

impl PlanWarmer {
    /// Create a new plan warmer.
    pub fn new(
        rx: mpsc::Receiver<PlanWarmerCommand>,
        prewarm_tx: Option<mpsc::Sender<PrewarmRequest>>,
        prewarm_count: usize,
    ) -> Self {
        Self {
            histograms: Arc::new(RwLock::new(HashMap::new())),
            rx,
            prewarm_tx,
            prewarm_count,
        }
    }

    /// Run the plan warmer loop.
    pub async fn run(mut self) {
        info!("PlanWarmer started");

        // Spawn periodic cleanup task
        let histograms = Arc::clone(&self.histograms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // hourly
            loop {
                interval.tick().await;
                Self::cleanup_expired(&histograms);
            }
        });

        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                PlanWarmerCommand::QueryStarted {
                    fingerprint,
                    query_id,
                } => {
                    self.handle_query_started(fingerprint, &query_id).await;
                }
                PlanWarmerCommand::QueryCompleted {
                    fingerprint,
                    query_id,
                    row_groups,
                } => {
                    self.handle_query_completed(fingerprint, &query_id, row_groups);
                }
                PlanWarmerCommand::Shutdown => {
                    info!("PlanWarmer shutting down");
                    break;
                }
            }
        }
    }

    async fn handle_query_started(&self, fingerprint: u64, query_id: &str) {
        let row_groups_to_warm = {
            let mut histograms = self.histograms.write();

            // Enforce LRU size limit
            if histograms.len() > MAX_FINGERPRINTS {
                Self::evict_oldest(&mut histograms);
            }

            if let Some(hist) = histograms.get_mut(&fingerprint) {
                hist.touch();
                let top = hist.top_row_groups(self.prewarm_count);
                debug!(
                    fingerprint = fingerprint,
                    query_id = %query_id,
                    row_groups = top.len(),
                    "Fingerprint hit, triggering pre-warm"
                );
                FINGERPRINT_HITS_TOTAL.inc();
                top
            } else {
                debug!(
                    fingerprint = fingerprint,
                    query_id = %query_id,
                    "Fingerprint miss (first occurrence)"
                );
                FINGERPRINT_MISSES_TOTAL.inc();
                Vec::new()
            }
        };

        // Send pre-warm requests
        if let Some(tx) = &self.prewarm_tx {
            for rg in row_groups_to_warm {
                let req = PrewarmRequest {
                    bucket: rg.bucket,
                    key: rg.key,
                    etag: rg.etag,
                    offset: rg.offset,
                    length: rg.length,
                };
                if tx.try_send(req).is_err() {
                    warn!("Prewarm queue full, dropping request");
                    PREWARM_DROPPED_TOTAL.inc();
                    break;
                }
                PREWARM_QUEUED_TOTAL.inc();
            }
        }
    }

    fn handle_query_completed(
        &self,
        fingerprint: u64,
        query_id: &str,
        row_groups: Vec<RowGroupAccess>,
    ) {
        let rg_count = row_groups.len();
        {
            let mut histograms = self.histograms.write();
            let hist = histograms
                .entry(fingerprint)
                .or_insert_with(RowGroupHistogram::new);

            for rg in row_groups {
                hist.record_access(rg);
            }
            hist.prune_if_needed();
        }

        debug!(
            fingerprint = fingerprint,
            query_id = %query_id,
            row_groups = rg_count,
            "Recorded row-group accesses for fingerprint"
        );
        ROW_GROUPS_RECORDED_TOTAL.inc_by(rg_count as u64);
    }

    fn cleanup_expired(histograms: &RwLock<HashMap<u64, RowGroupHistogram>>) {
        let mut hists = histograms.write();
        let before = hists.len();
        hists.retain(|_, v| !v.is_expired());
        let evicted = before - hists.len();
        if evicted > 0 {
            info!(evicted = evicted, "Cleaned up expired fingerprint histograms");
            FINGERPRINTS_EVICTED_TOTAL.inc_by(evicted as u64);
        }
    }

    fn evict_oldest(histograms: &mut HashMap<u64, RowGroupHistogram>) {
        // Find and remove the least recently accessed entry
        if let Some(oldest_key) = histograms
            .iter()
            .min_by_key(|(_, v)| v.last_accessed)
            .map(|(k, _)| *k)
        {
            histograms.remove(&oldest_key);
            FINGERPRINTS_EVICTED_TOTAL.inc();
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

use once_cell::sync::Lazy;
use prometheus::{register_int_counter_with_registry, IntCounter};

static REGISTRY: Lazy<prometheus::Registry> = Lazy::new(|| crate::metrics::REGISTRY.clone());

pub static FINGERPRINT_HITS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_warmer_fingerprint_hits_total",
        "Number of query starts where the fingerprint was found in cache.",
        *REGISTRY
    )
    .expect("register fingerprint_hits_total")
});

pub static FINGERPRINT_MISSES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_warmer_fingerprint_misses_total",
        "Number of query starts where the fingerprint was not found.",
        *REGISTRY
    )
    .expect("register fingerprint_misses_total")
});

pub static PREWARM_QUEUED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_warmer_prewarm_queued_total",
        "Number of row-group pre-warm requests queued.",
        *REGISTRY
    )
    .expect("register prewarm_queued_total")
});

pub static PREWARM_DROPPED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_warmer_prewarm_dropped_total",
        "Number of pre-warm requests dropped due to queue full.",
        *REGISTRY
    )
    .expect("register prewarm_dropped_total")
});

pub static ROW_GROUPS_RECORDED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_warmer_row_groups_recorded_total",
        "Number of row-group accesses recorded from completed queries.",
        *REGISTRY
    )
    .expect("register row_groups_recorded_total")
});

pub static FINGERPRINTS_EVICTED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_warmer_fingerprints_evicted_total",
        "Number of fingerprints evicted due to TTL or LRU pressure.",
        *REGISTRY
    )
    .expect("register fingerprints_evicted_total")
});

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rg(key: &str, ordinal: u32) -> RowGroupAccess {
        RowGroupAccess {
            bucket: "test".into(),
            key: key.into(),
            etag: "etag123".into(),
            row_group_ordinal: ordinal,
            offset: ordinal as u64 * 1024 * 1024,
            length: 1024 * 1024,
        }
    }

    #[test]
    fn test_histogram_recording() {
        let mut hist = RowGroupHistogram::new();

        let rg1 = make_rg("file1.parquet", 0);
        let rg2 = make_rg("file1.parquet", 1);

        hist.record_access(rg1.clone());
        hist.record_access(rg1.clone());
        hist.record_access(rg2.clone());

        assert_eq!(hist.counts.len(), 2);
        assert_eq!(hist.counts.get(&rg1), Some(&2));
        assert_eq!(hist.counts.get(&rg2), Some(&1));
    }

    #[test]
    fn test_top_row_groups() {
        let mut hist = RowGroupHistogram::new();

        let rg1 = make_rg("file1.parquet", 0);
        let rg2 = make_rg("file1.parquet", 1);
        let rg3 = make_rg("file1.parquet", 2);

        hist.record_access(rg1.clone());
        hist.record_access(rg1.clone());
        hist.record_access(rg1.clone());
        hist.record_access(rg2.clone());
        hist.record_access(rg2.clone());
        hist.record_access(rg3.clone());

        let top = hist.top_row_groups(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0], rg1); // 3 accesses
        assert_eq!(top[1], rg2); // 2 accesses
    }

    #[test]
    fn test_prune_if_needed() {
        let mut hist = RowGroupHistogram::new();

        // Add more than MAX_ROW_GROUPS_PER_FINGERPRINT entries
        for i in 0..MAX_ROW_GROUPS_PER_FINGERPRINT + 100 {
            hist.record_access(make_rg(&format!("file{}.parquet", i), 0));
        }

        hist.prune_if_needed();

        // Should be pruned to half
        assert!(hist.counts.len() <= MAX_ROW_GROUPS_PER_FINGERPRINT / 2);
    }
}
