//! SHELF-40 — runtime glue between the `shelf-cost` crate and
//! `shelfd`'s Prometheus surface.
//!
//! What lives here:
//!
//! 1. [`CostState`] — a refcounted handle that the `s3_shim` and
//!    `peer_fetch` hot paths borrow to bump
//!    `shelf_s3_dollars_saved_total{region, outcome}` on every
//!    cache hit. The handle holds the shared `CostModel` (cheap to
//!    `Clone` because [`shelf_cost::CostModel`] is plain data).
//! 2. The rolling-rate updater task that fills
//!    `shelf_s3_dollars_saved_rate_cents_per_sec{region, outcome}`
//!    once per second so dashboards can render `cents/sec` without
//!    re-deriving `rate(...) * 0.01` everywhere.
//!
//! The wiring is **default-on** per the SHELF-40 acceptance gate;
//! operators flip `cache.cost.enabled: false` in YAML to disable
//! both the counter bumps and the rate-updater task.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use shelf_cost::{CostConfig, CostConfigError, CostModel, HitEvent};
use tokio_util::sync::CancellationToken;

/// Shared state the hot path reads on every served byte.
#[derive(Debug, Clone)]
pub struct CostState {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    enabled: bool,
    model: CostModel,
    /// Last sampled `shelf_s3_dollars_saved_total` value per
    /// `(region, outcome)` 60-sample sliding window. We keep an
    /// in-memory ring so the rate gauge can publish a smooth
    /// 60s rate without the dashboard having to re-derive
    /// `rate(... [60s])` in PromQL. Cardinality is bounded by
    /// `regions × 3 outcomes` ≤ ~ 6 series in practice.
    samples: parking_lot::RwLock<std::collections::HashMap<(String, &'static str), Window>>,
}

#[derive(Debug)]
struct Window {
    /// Ring of last-second cumulative cents (oldest at `head`).
    /// Length is 60 (one per second over the last minute).
    ring: [i64; 60],
    head: u8,
    filled: u8,
}

impl Window {
    fn push(&mut self, total: i64) -> i64 {
        let oldest = self.ring[self.head as usize];
        self.ring[self.head as usize] = total;
        self.head = (self.head + 1) % 60;
        if self.filled < 60 {
            self.filled += 1;
        }
        // Rate over the *filled* window (denominator scales early
        // in the lifetime of a pod so the gauge isn't artificially
        // small for the first minute).
        let denom = self.filled.max(1) as i64;
        // (newest - oldest_within_window) / window_seconds
        // The newest is `total`; the oldest is `oldest` only when
        // the ring is full; while filling, the oldest valid entry
        // is the value at index `(head - filled) % 60`.
        let oldest_idx = if self.filled < 60 {
            // Before the ring fills, the oldest is wherever we
            // started — index `(head - filled)` mod 60.
            ((self.head as i16 - self.filled as i16).rem_euclid(60)) as usize
        } else {
            self.head as usize
        };
        let oldest_val = if self.filled < 60 {
            self.ring[oldest_idx]
        } else {
            oldest
        };
        let delta = total.saturating_sub(oldest_val);
        delta / denom
    }
}

impl Window {
    fn new() -> Self {
        Self {
            ring: [0; 60],
            head: 0,
            filled: 0,
        }
    }
}

impl CostState {
    /// Build from a validated [`CostConfig`]. Returns the same
    /// [`CostConfigError`] the loader produced — the only place
    /// we ever materialise a `CostModel` is here, so errors
    /// surface at the top of `main` and refuse to register the
    /// counter (matching the SHELF-40 anti-overclaim gate).
    pub fn from_config(cfg: &CostConfig) -> Result<Self, CostConfigError> {
        let model = CostModel::from_config(cfg)?;
        Ok(Self {
            inner: Arc::new(Inner {
                enabled: cfg.enabled,
                model,
                samples: parking_lot::RwLock::new(std::collections::HashMap::new()),
            }),
        })
    }

    /// "Off" sentinel — cost wiring inert, every observe is a
    /// no-op. Useful in unit tests / lightweight integration tests
    /// that don't want a real cost model in scope.
    pub fn disabled() -> Self {
        // Safe even though we ignore failures: the us-east-1 preset
        // is deterministic and never returns Err for this region.
        let model = CostModel::for_region("us-east-1").expect("preset");
        Self {
            inner: Arc::new(Inner {
                enabled: false,
                model,
                samples: parking_lot::RwLock::new(std::collections::HashMap::new()),
            }),
        }
    }

    /// Whether the runtime is bumping the counter today. Hot path
    /// reads this once per request; an inert state short-circuits
    /// to zero atomic adds.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.enabled
    }

    /// Region label used for both Prometheus dimensions; stable
    /// for the lifetime of the process.
    #[inline]
    pub fn region(&self) -> &str {
        &self.inner.model.region_id
    }

    /// Compute and bump the counter for `event`. Returns the
    /// `Cents` contribution so the caller (or a debug log) can see
    /// what was added without re-running the formula.
    ///
    /// Hot path: roughly two integer multiplies and an atomic
    /// add — see `crates/shelf-cost/benches/`. When `enabled` is
    /// false this short-circuits to a single bool load + return.
    #[inline]
    pub fn observe(&self, event: HitEvent) -> shelf_cost::Cents {
        if !self.inner.enabled {
            return shelf_cost::Cents::ZERO;
        }
        let saved = self.inner.model.dollars_saved(event);
        let inc = saved.as_cents_u64();
        if inc > 0 {
            crate::metrics::S3_DOLLARS_SAVED_TOTAL
                .with_label_values(&[self.region(), event.outcome_label()])
                .inc_by(inc);
        }
        saved
    }

    /// Spawn the rolling-rate updater task. Ticks once per second,
    /// reads each `(region, outcome)` series of
    /// `shelf_s3_dollars_saved_total`, pushes the new sample into
    /// the ring, and writes the resulting cents-per-second number
    /// to `shelf_s3_dollars_saved_rate_cents_per_sec`.
    ///
    /// The task observes `cancel`; when the parent shuts down the
    /// loop returns instead of leaving the runtime alive.
    pub fn spawn_rate_updater(&self, cancel: CancellationToken) {
        if !self.inner.enabled {
            return;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            // 1 Hz tick; SHELF-40 acceptance is "60s rolling", so
            // any tick rate ≥ 1/60 Hz produces a stable signal.
            // 1 Hz is the natural fit for a 60-sample ring.
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("SHELF-40 rate updater shutting down");
                        return;
                    }
                    _ = ticker.tick() => {
                        update_rate(&inner);
                    }
                }
            }
        });
    }
}

fn update_rate(inner: &Inner) {
    // Walk the **observed** label values via a snapshot of the
    // counter family. We can't enumerate `IntCounterVec`'s children
    // directly, so we rely on `MetricVec` returning the populated
    // children via `collect()`. For a typical 1-region cluster this
    // produces 0..3 rows once any traffic flows.
    let region = inner.model.region_id.as_str();
    for outcome in ["hit_memory", "hit_disk", "peer"] {
        // `with_label_values` lazily registers a child; we then
        // read its value. This is the standard pattern in the
        // prometheus crate for recurring scrape-style reads.
        let total = crate::metrics::S3_DOLLARS_SAVED_TOTAL
            .with_label_values(&[region, outcome])
            .get();
        let total_i64 = total as i64;
        let mut samples = inner.samples.write();
        let entry = samples
            .entry((region.to_owned(), outcome))
            .or_insert_with(Window::new);
        let rate = entry.push(total_i64);
        crate::metrics::S3_DOLLARS_SAVED_RATE_CENTS_PER_SEC
            .with_label_values(&[region, outcome])
            .set(rate);
    }
}

/// Tiny helper that maps `(HitTier, peer_az)` to the right
/// [`HitEvent`] variant for the local cache-hit path. Centralising
/// the `match` saves us from sprinkling identical four-way switches
/// across `s3_shim` and any future direct-cache caller.
#[inline]
pub fn hit_event_local(
    tier: crate::store::HitTier,
    bytes: u64,
    peer_az: shelf_cost::PeerAz,
) -> HitEvent {
    match tier {
        crate::store::HitTier::Memory => HitEvent::Memory {
            bytes_returned: bytes,
            peer_az,
        },
        crate::store::HitTier::Disk => HitEvent::Disk {
            bytes_returned: bytes,
            peer_az,
        },
    }
}

/// SHELF-40 — pessimistic default. Without explicit AZ-aware
/// membership data (SHELF-23 + SHELF-20 surfaces it later), every
/// hit is **modelled** as same-AZ so the counter never inflates by
/// claiming cross-AZ savings that didn't actually happen. Operators
/// who confirm a cross-AZ topology (e.g. multi-AZ Trino-per-shelfd
/// pairing) can flip the contract via a future `peer_az` per-pod
/// override on `CostState`. The OSS-default contract today is
/// "same-AZ unless proven otherwise".
pub const DEFAULT_PEER_AZ: shelf_cost::PeerAz = shelf_cost::PeerAz::SameAz;

/// SHELF-40 internal use only — kept as a 64-bit atomic so the
/// caller can probe rolling totals from a non-async context (the
/// integration test harness uses this to assert the counter
/// advanced after N hits without waiting on a Prometheus scrape).
#[derive(Debug, Default)]
pub struct DebugProbe(AtomicI64);

impl DebugProbe {
    pub fn add(&self, delta: i64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
    pub fn read(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_state_short_circuits_observe() {
        let cs = CostState::disabled();
        assert!(!cs.is_enabled());
        let saved = cs.observe(HitEvent::Memory {
            bytes_returned: 1 << 30,
            peer_az: shelf_cost::PeerAz::CrossAz,
        });
        assert_eq!(saved, shelf_cost::Cents::ZERO);
    }

    #[test]
    fn enabled_state_observes_and_returns_cents() {
        let cfg = CostConfig {
            enabled: true,
            region: "ap-south-1".to_owned(),
            ..CostConfig::default()
        };
        let cs = CostState::from_config(&cfg).unwrap();
        assert!(cs.is_enabled());
        // 1 GiB cross-AZ Disk hit = 1 cent (the data-transfer term
        // contributes exactly $0.01/GiB; the GET term is sub-cent).
        let saved = cs.observe(HitEvent::Disk {
            bytes_returned: 1 << 30,
            peer_az: shelf_cost::PeerAz::CrossAz,
        });
        assert_eq!(saved.as_cents_i64(), 1, "got {saved}");
    }

    #[test]
    fn window_push_yields_stable_rate_after_filling() {
        // 60 samples of 100-cents-per-sec → rate should be 100.
        let mut w = Window::new();
        for sec in 0..60 {
            let total = sec * 100;
            let _ = w.push(total);
        }
        // The 61st push at +100 over the new 'oldest' should still
        // report ~100 cents/sec.
        let rate = w.push(60 * 100);
        assert!((90..=110).contains(&rate), "rate={rate}");
    }

    #[test]
    fn hit_event_local_picks_right_variant() {
        let m = hit_event_local(
            crate::store::HitTier::Memory,
            1234,
            shelf_cost::PeerAz::SameAz,
        );
        assert!(matches!(m, HitEvent::Memory { .. }));
        let d = hit_event_local(
            crate::store::HitTier::Disk,
            5678,
            shelf_cost::PeerAz::CrossAz,
        );
        assert!(matches!(d, HitEvent::Disk { .. }));
    }
}
