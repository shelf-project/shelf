//! Rolling hit-ratio sampler + cold-start warm-up SLI.
//!
//! Track G-11 (perf-research-2026-04-27, Phase A). The two metrics
//! that the post-cutover canary gate actually scores against —
//! "hit ratio ≥ 80% after 12h warm" and "time from pod ready until
//! ≥ 50% hit ratio" — are not directly readable from the monotonic
//! `shelf_hits_total` / `shelf_misses_total` counters because those
//! mix the giant cold-start tail with the steady-state behaviour.
//!
//! This module spawns a single background sampler that:
//!
//! 1. Ticks every [`SAMPLE_INTERVAL`] seconds.
//! 2. For each pool it reads `(hits, misses)` from the Prometheus
//!    counters, diffs against the previous tick, and computes a
//!    rolling hit ratio over the last [`WINDOW_SECS`] seconds.
//! 3. Publishes the ratio to
//!    [`crate::metrics::ROLLING_HIT_RATIO_BPS`] in basis points
//!    (0–10_000) — integer gauge dodges the YAML scientific-notation
//!    landmine the Helm chart hit on big floats; clients divide by
//!    100 for a percentage.
//! 4. The first time the rolling ratio crosses
//!    `warm_threshold_bps` for a pool, sets
//!    [`crate::metrics::WARM_THRESHOLD_CROSSED_SECONDS`] to the
//!    elapsed wall-clock seconds since the sampler started
//!    (≈ pod ready). Subsequent crossings are no-ops, so dashboards
//!    can use `max_over_time(...)` to surface a gap when a pod
//!    rotated before warming.
//!
//! The sampler is intentionally thin — no shared state with the
//! hot path, only Prometheus reads. That keeps the cost ~µs/tick
//! and avoids pulling locks into a path that would otherwise be
//! contention-free.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// How often the sampler ticks. Five seconds is chosen so the
/// rolling-window queue stays small (≤ 12 entries for a 60 s window)
/// and dashboards refreshing at 30 s see ≥ 6 fresh samples per panel.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// Length of the rolling window over which the hit ratio is
/// averaged. Long enough to absorb a single bursty Trino split
/// (which can issue O(100) GETs in a second) but short enough that
/// "currently warm" lights up within a minute of cold-start.
pub const WINDOW_SECS: u64 = 60;

/// Default warm threshold in basis points. 5_000 bps = 50%; matches
/// the rollout playbook's "≥ 50 % hit ratio = pod considered warm"
/// definition (cf. `shelf/docs/launch/playbook.md`).
pub const DEFAULT_WARM_THRESHOLD_BPS: i64 = 5_000;

/// One pool's rolling-window state. Held inside the sampler task,
/// not exported; tests touch it via [`tick_once`].
struct PoolState {
    label: &'static str,
    samples: VecDeque<(u64, u64)>,
    last_hits: u64,
    last_misses: u64,
    crossed: bool,
}

impl PoolState {
    fn new(label: &'static str, hits: u64, misses: u64) -> Self {
        Self {
            label,
            samples: VecDeque::with_capacity(window_capacity()),
            last_hits: hits,
            last_misses: misses,
            crossed: false,
        }
    }
}

fn window_capacity() -> usize {
    (WINDOW_SECS / SAMPLE_INTERVAL.as_secs()) as usize
}

/// Spawn the warm-up sampler. The returned `JoinHandle` is detached
/// in `main`; the task exits when `shutdown` fires.
pub fn spawn(threshold_bps: i64, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run(threshold_bps, shutdown).await;
    })
}

async fn run(threshold_bps: i64, shutdown: CancellationToken) {
    let started = Instant::now();
    let mut pools: Vec<PoolState> = ["metadata", "rowgroup"]
        .iter()
        .map(|label| {
            let (h, m) = read_counters(label);
            PoolState::new(label, h, m)
        })
        .collect();

    let mut ticker = interval(SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Skip the immediate tick so the first sample reflects real
    // traffic, not the zero baseline taken at task start.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {
                let elapsed = started.elapsed().as_secs() as i64;
                for state in pools.iter_mut() {
                    tick_once(state, elapsed, threshold_bps);
                }
            }
        }
    }
}

/// Single-tick body, factored so tests can drive the math
/// deterministically without spinning a tokio runtime.
fn tick_once(state: &mut PoolState, elapsed: i64, threshold_bps: i64) {
    let (hits, misses) = read_counters(state.label);
    let dh = hits.saturating_sub(state.last_hits);
    let dm = misses.saturating_sub(state.last_misses);
    state.last_hits = hits;
    state.last_misses = misses;
    state.samples.push_back((dh, dm));
    while state.samples.len() > window_capacity() {
        state.samples.pop_front();
    }
    let (sum_h, sum_m): (u64, u64) = state
        .samples
        .iter()
        .fold((0, 0), |(a, b), (h, m)| (a + h, b + m));
    let bps = if sum_h + sum_m == 0 {
        0
    } else {
        (sum_h as i64 * 10_000) / (sum_h as i64 + sum_m as i64)
    };
    crate::metrics::ROLLING_HIT_RATIO_BPS
        .with_label_values(&[state.label])
        .set(bps);
    if !state.crossed && bps >= threshold_bps {
        crate::metrics::WARM_THRESHOLD_CROSSED_SECONDS
            .with_label_values(&[state.label])
            .set(elapsed);
        state.crossed = true;
        tracing::info!(
            pool = state.label,
            elapsed_seconds = elapsed,
            threshold_bps,
            "warm threshold crossed",
        );
    }
}

/// Read the current `(hits, misses)` for a pool. Direct counter
/// reads — no `gather()` round-trip — so a 5 s tick costs ~µs.
fn read_counters(label: &str) -> (u64, u64) {
    let h = crate::metrics::HITS_TOTAL.with_label_values(&[label]).get();
    let m = crate::metrics::MISSES_TOTAL
        .with_label_values(&[label])
        .get();
    (h, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive `tick_once` by hand against the global Prometheus
    /// registry; verifies the rolling-window math and the one-shot
    /// warm-threshold latch.
    #[test]
    fn rolling_window_and_warm_latch() {
        // Use a unique pool label so this test does not collide with
        // others in the same binary that touch `HITS_TOTAL`.
        const LABEL: &str = "test_warm_pool";
        crate::metrics::HITS_TOTAL
            .with_label_values(&[LABEL])
            .inc_by(0);
        crate::metrics::MISSES_TOTAL
            .with_label_values(&[LABEL])
            .inc_by(0);
        let (h0, m0) = read_counters(LABEL);
        let mut state = PoolState {
            label: LABEL,
            samples: VecDeque::with_capacity(window_capacity()),
            last_hits: h0,
            last_misses: m0,
            crossed: false,
        };

        // First tick: 0 hits, 5 misses → 0 bps, no cross.
        crate::metrics::MISSES_TOTAL
            .with_label_values(&[LABEL])
            .inc_by(5);
        tick_once(&mut state, 5, 5_000);
        assert!(!state.crossed);
        let bps = crate::metrics::ROLLING_HIT_RATIO_BPS
            .with_label_values(&[LABEL])
            .get();
        assert_eq!(bps, 0);

        // Second tick: 100 hits, 0 misses on top → ratio over the
        // window is now 100 / 105 ≈ 9523 bps; 5_000 threshold trips.
        crate::metrics::HITS_TOTAL
            .with_label_values(&[LABEL])
            .inc_by(100);
        tick_once(&mut state, 10, 5_000);
        assert!(state.crossed, "warm threshold should latch");
        let elapsed = crate::metrics::WARM_THRESHOLD_CROSSED_SECONDS
            .with_label_values(&[LABEL])
            .get();
        assert_eq!(elapsed, 10);

        // Third tick: another 50 misses; gauge drifts but the
        // crossed-seconds gauge stays put (one-shot latch).
        crate::metrics::MISSES_TOTAL
            .with_label_values(&[LABEL])
            .inc_by(50);
        tick_once(&mut state, 15, 5_000);
        let still = crate::metrics::WARM_THRESHOLD_CROSSED_SECONDS
            .with_label_values(&[LABEL])
            .get();
        assert_eq!(still, 10);
    }
}
