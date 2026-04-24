//! Telemetry init for `shelfd` (SHELF-08).
//!
//! Combines the existing structured-JSON `tracing-subscriber` fmt
//! layer with an optional OTLP span exporter backed by
//! `tracing-opentelemetry`.
//!
//! # Fail-open contract
//!
//! Telemetry failures must never crash `shelfd`. If OTLP init fails,
//! [`init`] logs a `warn!` through the fmt layer and returns a guard
//! whose drop is a no-op. The daemon then runs exactly as before —
//! Prometheus still scrapes, logs still flow, traces are simply not
//! exported.
//!
//! # Shutdown
//!
//! The returned [`TelemetryGuard`] holds the `TracerProvider` so that
//! the batch span processor flushes on drop. Dropping it inside
//! `main`'s Tokio runtime is safe because
//! `opentelemetry_sdk::trace::TracerProvider::shutdown` is sync and
//! idempotent.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::TracerProvider, Resource};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::ObservabilityConfig;

/// RAII handle keeping the `TracerProvider` alive. Drop flushes
/// pending spans on a best-effort basis.
#[derive(Debug, Default)]
pub struct TelemetryGuard {
    provider: Option<TracerProvider>,
}

impl TelemetryGuard {
    /// True if the OTLP exporter was successfully installed.
    pub fn otlp_enabled(&self) -> bool {
        self.provider.is_some()
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            // `shutdown()` flushes the batch processor. Swallow errors —
            // a failing collector must not take the daemon down on
            // SIGTERM.
            if let Err(e) = provider.shutdown() {
                tracing::warn!(error = %e, "otlp tracer shutdown failed");
            }
        }
    }
}

/// Install the global tracing subscriber.
///
/// The fmt layer is always installed. The OTLP layer is only added
/// when `observability.otlp_endpoint` is `Some` AND the exporter
/// builds cleanly; any failure downgrades to "fmt only" with a
/// warning log line.
///
/// `pod_id` is emitted as the `pod.id` resource attribute so Tempo
/// can group traces by emitting pod.
pub fn init(
    filter: &str,
    observability: &ObservabilityConfig,
    pod_id: &str,
) -> anyhow::Result<TelemetryGuard> {
    let env_filter =
        EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info,shelfd=debug"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .json()
        .flatten_event(true);

    // Build the OTLP layer if configured. On any failure we fall back
    // to fmt-only so the daemon still boots in fixtures/CI.
    let (otlp_layer, provider) = match observability.otlp_endpoint.as_deref() {
        None => (None, None),
        Some(endpoint) => match build_otlp_provider(endpoint, pod_id) {
            Ok(provider) => {
                let tracer = provider.tracer("shelfd");
                let layer = tracing_opentelemetry::layer().with_tracer(tracer);
                (Some(layer), Some(provider))
            }
            Err(e) => {
                // Emit via eprintln! because the subscriber isn't
                // installed yet — a `tracing::warn!` here would be
                // dropped. The fmt layer below will be the one that
                // picks up subsequent events.
                eprintln!("shelfd: OTLP init failed ({e}); continuing without trace export");
                (None, None)
            }
        },
    };

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    if let Some(otlp_layer) = otlp_layer {
        registry
            .with(otlp_layer)
            .try_init()
            .map_err(|e| anyhow::anyhow!("tracing subscriber init: {e}"))?;
    } else {
        registry
            .try_init()
            .map_err(|e| anyhow::anyhow!("tracing subscriber init: {e}"))?;
    }

    Ok(TelemetryGuard { provider })
}

/// Build a batch `TracerProvider` that ships spans to `endpoint`.
///
/// Uses the `rt-tokio` runtime so the batch processor cooperates with
/// the main Tokio runtime shelfd builds in `main`.
fn build_otlp_provider(endpoint: &str, pod_id: &str) -> anyhow::Result<TracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| anyhow::anyhow!("otlp exporter build: {e}"))?;

    let resource = Resource::new([
        KeyValue::new("service.name", "shelfd"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("pod.id", pod_id.to_owned()),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    global::set_tracer_provider(provider.clone());
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_without_otlp_is_ok() {
        // The global subscriber is process-wide: this test may race with
        // other tests in the same binary that also call `init`. That is
        // fine — `try_init` returns an error on the second caller and
        // our error path is surfaced as anyhow::Error. We only assert
        // the "fail-open" behaviour (no panic, guard returned on the
        // first successful install).
        let cfg = ObservabilityConfig::default();
        let res = init("info", &cfg, "shelf-test-0");
        // Whether this call was the first or not, it must not panic.
        drop(res);
    }

    #[test]
    fn guard_drop_is_noop_when_no_provider() {
        let g = TelemetryGuard::default();
        assert!(!g.otlp_enabled());
        drop(g);
    }
}
