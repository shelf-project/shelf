//! Long-running runtime — `watch` mode + Prometheus exposition.
//!
//! The `watch` subcommand re-runs the recommender pipeline every
//! N minutes, writes a fresh report to disk, and exposes a tiny
//! Prometheus exposition endpoint at `:9100/metrics` (configurable)
//! so an external Prom can scrape `shelf_advisor_recommendations_total`
//! without a Grafana / shelfd dependency.
//!
//! ## Why a hand-rolled HTTP responder
//!
//! For one read-only endpoint that emits ~400 bytes of text we
//! deliberately do **not** pull in `axum` / `hyper` framework
//! middleware. The advisor's "no new heavy deps" rule (per the
//! SHELF-53 user override) applies; the responder below is ~50
//! lines of pure tokio + a single `format!`. Replacing it with
//! a framework is a follow-up if we ever grow more endpoints.
//!
//! The exposition format is the standard Prom text format
//! ([documented here](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format)).
//! Test coverage lives in `tests/it_recommend.rs::watch_metrics_smoke`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::output::Recommendation;

/// Aggregated counters surfaced at `:9100/metrics`. Each
/// `(category, severity)` pair has its own atomic counter so the
/// `watch` loop can update them lock-free between scrapes.
///
/// "Severity" today is a coarse derivation from confidence —
/// `>=0.8` is `critical`, `0.6..0.8` is `warn`, the rest is
/// `info`. This matches the canonical SHELF-53 design note's
/// `info|warn|critical` triple without forcing every recommender
/// to author its own severity field today.
#[derive(Default)]
pub struct RuntimeMetrics {
    runs_total: AtomicU64,
    runs_failed_total: AtomicU64,
    /// Flat `(category, severity, count)` table — looked up
    /// linearly. We only care about ~10 keys total so the tiny
    /// linear scan is cheaper than wiring up a hashmap behind a
    /// mutex.
    counters: parking_lot_safe::Mutex<Vec<(String, String, u64)>>,
}

/// Tiny mutex shim. We don't pull in `parking_lot` (not in
/// workspace.dependencies for advisor's lean subset) and stdlib
/// `Mutex` is plenty fast for an exposition counter that updates
/// once per `watch` interval.
mod parking_lot_safe {
    pub struct Mutex<T>(std::sync::Mutex<T>);
    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Self(std::sync::Mutex::new(T::default()))
        }
    }
    impl<T> Mutex<T> {
        pub fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(|p| p.into_inner())
        }
    }
}

impl RuntimeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Mark a run completed (successfully) and refresh the
    /// `(category, severity, count)` table from the produced
    /// recommendations. Called once per `watch` iteration.
    pub fn record_run(&self, recs: &[Recommendation]) {
        self.runs_total.fetch_add(1, Ordering::Relaxed);
        let mut tab: Vec<(String, String, u64)> = Vec::new();
        for r in recs {
            let sev = severity(r.confidence).to_string();
            if let Some(slot) = tab
                .iter_mut()
                .find(|(c, s, _)| *c == r.recommendation_type && *s == sev)
            {
                slot.2 += 1;
            } else {
                tab.push((r.recommendation_type.clone(), sev, 1));
            }
        }
        tab.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
        *self.counters.lock() = tab;
    }

    /// Mark a run as failed (advisor caught an error mid-pipeline).
    pub fn record_failure(&self) {
        self.runs_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Render the Prometheus text exposition body. Stable order
    /// — the `record_run` table is always sorted before being
    /// stored.
    pub fn render_prom(&self) -> String {
        let mut s = String::new();
        s.push_str(
            "# HELP shelf_advisor_runs_total Total advisor runs (success).\n\
             # TYPE shelf_advisor_runs_total counter\n",
        );
        s.push_str(&format!(
            "shelf_advisor_runs_total {}\n",
            self.runs_total.load(Ordering::Relaxed)
        ));
        s.push_str(
            "# HELP shelf_advisor_runs_failed_total Advisor runs that failed mid-pipeline.\n\
             # TYPE shelf_advisor_runs_failed_total counter\n",
        );
        s.push_str(&format!(
            "shelf_advisor_runs_failed_total {}\n",
            self.runs_failed_total.load(Ordering::Relaxed)
        ));
        s.push_str(
            "# HELP shelf_advisor_recommendations_total Recommendations emitted by the most recent run, labelled by category + severity.\n\
             # TYPE shelf_advisor_recommendations_total gauge\n",
        );
        let tab = self.counters.lock().clone();
        for (cat, sev, n) in tab {
            s.push_str(&format!(
                "shelf_advisor_recommendations_total{{category={cat:?},severity={sev:?}}} {n}\n"
            ));
        }
        s
    }
}

/// Coarse severity derivation. Mirrors the canonical SHELF-53
/// design note's `info|warn|critical` triple.
pub fn severity(confidence: f32) -> &'static str {
    if confidence >= 0.8 {
        "critical"
    } else if confidence >= 0.6 {
        "warn"
    } else {
        "info"
    }
}

/// Spawn the Prometheus exposition listener on `addr`. Returns
/// the bound `SocketAddr` so callers can log it. Background
/// task lives until the runtime shuts down.
pub async fn spawn_prom_listener(
    addr: std::net::SocketAddr,
    metrics: Arc<RuntimeMetrics>,
) -> crate::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((sock, _peer)) => {
                    let m = Arc::clone(&metrics);
                    tokio::spawn(async move {
                        if let Err(e) = handle_prom_request(sock, m).await {
                            tracing::warn!(error = %e, "prom request handler failed");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "prom accept failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    });
    Ok(bound)
}

async fn handle_prom_request(
    mut sock: tokio::net::TcpStream,
    metrics: Arc<RuntimeMetrics>,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = [0u8; 1024];
    // Best-effort read: we only need to consume the request line
    // (the response is always the same body).
    let _ = sock.read(&mut buf).await?;
    let body = metrics.render_prom();
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body,
    );
    sock.write_all(resp.as_bytes()).await?;
    sock.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(ty: &str, conf: f32) -> Recommendation {
        Recommendation {
            recommendation_type: ty.to_string(),
            table: "t".to_string(),
            confidence: conf,
            rationale: json!({}),
            suggested_change: json!({}),
        }
    }

    #[test]
    fn metrics_text_is_stable_order() {
        let m = RuntimeMetrics::new();
        m.record_run(&[
            rec("optimize_targets", 0.95),
            rec("pin_list_candidates", 0.7),
            rec("optimize_targets", 0.55),
        ]);
        let body = m.render_prom();
        // category=optimize_targets sorts before pin_list; within, critical < info per ASCII.
        let optimize_critical_pos = body
            .find(r#"category="optimize_targets",severity="critical""#)
            .unwrap();
        let optimize_info_pos = body
            .find(r#"category="optimize_targets",severity="info""#)
            .unwrap();
        let pin_pos = body
            .find(r#"category="pin_list_candidates",severity="warn""#)
            .unwrap();
        assert!(optimize_critical_pos < optimize_info_pos);
        assert!(optimize_info_pos < pin_pos);
        assert!(body.contains("shelf_advisor_runs_total 1"));
    }

    #[test]
    fn severity_buckets() {
        assert_eq!(severity(0.95), "critical");
        assert_eq!(severity(0.8), "critical");
        assert_eq!(severity(0.7), "warn");
        assert_eq!(severity(0.6), "warn");
        assert_eq!(severity(0.59), "info");
        assert_eq!(severity(0.0), "info");
    }
}
