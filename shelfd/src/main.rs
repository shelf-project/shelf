//! `shelfd` binary entry point.
//!
//! Ticket ownership:
//! - SHELF-02 — Axum HTTP server, `/healthz`, `/readyz`, `/metrics`,
//!   graceful shutdown, structured logging.
//! - SHELF-06 — full read-through path (see `http::handlers::get_cache`).
//!
//! `main` composes `Config`, `metrics::Registry`, `origin::S3Origin`,
//! `store::FoyerStore`, `router::Router`, `admission::SizeThresholdPolicy`
//! into an `http::ServerState`, then drives `http::serve` under a
//! SIGTERM/SIGINT-triggered `CancellationToken`.

use std::sync::Arc;

use clap::Parser;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::Config,
    head_lru::HeadLru,
    http::{self, ServerState},
    metrics,
    origin::S3Origin,
    router::Router,
    store::FoyerStore,
    telemetry::{self, TelemetryGuard},
};
use tokio_util::sync::CancellationToken;

/// Command-line arguments for `shelfd`. Kept intentionally small; all
/// tunables live in `Config` (see `shelfd::config::Config` +
/// `contracts/config-keys.md`).
#[derive(Debug, Parser)]
#[command(
    name = "shelfd",
    version,
    about = "Shelf cache daemon",
    long_about = "Row-group-granular, Iceberg-native read cache for Trino. \
                  See shelf/BLUEPRINT.md §6.1 and shelf/agents/out/03-plan.md."
)]
struct Args {
    /// Path to the shelfd YAML config file.
    #[arg(long, env = "SHELFD_CONFIG", default_value = "/etc/shelfd/config.yaml")]
    config: std::path::PathBuf,

    /// Override the log level (`RUST_LOG`-compatible filter).
    #[arg(long, env = "SHELFD_LOG", default_value = "info,shelfd=debug")]
    log: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async { run(args).await })
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config =
        Config::from_path(&args.config).map_err(|e| anyhow::anyhow!("config load failed: {e}"))?;

    // SHELF-08: install the subscriber AFTER config is parsed so the
    // OTLP endpoint + pod id can flow into the tracer resource. The
    // guard flushes pending spans on drop at the end of `run`.
    let _telemetry: TelemetryGuard =
        telemetry::init(&args.log, &config.observability, &config.node.id)?;
    if _telemetry.otlp_enabled() {
        tracing::info!(
            endpoint = ?config.observability.otlp_endpoint,
            "otlp tracing exporter enabled",
        );
    }
    tracing::info!(node = %config.node.id, "shelfd starting");

    let metrics = Arc::new(metrics::Registry::init()?);
    let origin = Arc::new(S3Origin::new(&config.origin).await?);
    let store = Arc::new(FoyerStore::open(&config.pools).await?);
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&config.admission));
    let head_lru = Arc::new(HeadLru::new(config.head_lru_entries));

    let shutdown = CancellationToken::new();
    spawn_signal_handler(shutdown.clone());

    // SHELF-24: construct the pin-list loader BEFORE building
    // `ServerState` so the resulting `ReloadHandle` can be threaded
    // through `with_reload_handle`. The loader runs regardless of
    // whether the admin surface is reachable — the handle just lets
    // `POST /admin/reload` short-circuit the timer.
    let reload_handle = match config.pin_list.as_ref() {
        Some(cfg) if cfg.enabled => {
            use shelfd::pinlist::PinListLoader;
            let loader = PinListLoader::new(
                origin.client().clone(),
                cfg.bucket.clone(),
                cfg.key.clone(),
                cfg.refresh_period,
                store.clone(),
            );
            match loader.boot_and_spawn(shutdown.clone()).await {
                Ok((handle, _join)) => {
                    tracing::info!(
                        bucket = %cfg.bucket,
                        key = %cfg.key,
                        refresh_period = ?cfg.refresh_period,
                        "pin-list loader online",
                    );
                    Some(handle)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "pin-list loader failed to start; continuing without");
                    None
                }
            }
        }
        Some(_) => {
            tracing::info!("pin-list loader disabled by config (pin_list.enabled=false)");
            None
        }
        None => {
            tracing::debug!("no pin_list stanza; pin-list loader not spawned");
            None
        }
    };

    let mut state = ServerState::with_head_lru_and_pod_id(
        store.clone(),
        origin.clone(),
        router,
        admission,
        metrics,
        head_lru,
        config.node.id.clone(),
    );
    if let Some(handle) = reload_handle {
        state = state.with_reload_handle(handle);
    }
    let state = Arc::new(state);
    // Phase-0: mark ready as soon as Foyer + S3 client built. The
    // origin head-bucket probe would go here once SHELF-07 lands.
    state.mark_ready();

    let listen = config.http.listen;
    let request_timeout = config.http.request_timeout;
    tracing::info!(%listen, ?request_timeout, "binding data plane");

    if config.s3_shim.enabled {
        // SHELF-22: dedicated port keeps generic-S3 clients off
        // the native read path so a hot boto3 loop cannot starve
        // Trino splits sharing this daemon's event loop.
        let shim_addr: std::net::SocketAddr = config.s3_shim.bind_address.parse().map_err(|e| {
            anyhow::anyhow!(
                "s3_shim.bind_address='{}' is not a valid SocketAddr: {e}",
                config.s3_shim.bind_address,
            )
        })?;
        state.s3_shim_max_full_object_bytes.store(
            config.s3_shim.max_full_object_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );
        tracing::info!(%shim_addr, "binding s3-compat shim");
        let data_fut = http::serve(listen, state.clone(), request_timeout, shutdown.clone());
        let shim_fut =
            http::serve_s3_shim(shim_addr, state.clone(), request_timeout, shutdown.clone());
        tokio::select! {
            r = data_fut => r?,
            r = shim_fut => r?,
        }
    } else {
        http::serve(listen, state, request_timeout, shutdown).await?;
    }

    tracing::info!("shelfd shutdown complete");
    Ok(())
}

/// Cancel `token` on SIGTERM or SIGINT. On non-unix we only listen for
/// Ctrl-C; shelfd is a linux-only binary in production but this keeps
/// `cargo run` on macOS dev-machines sane.
fn spawn_signal_handler(token: CancellationToken) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "SIGTERM handler setup failed");
                    return;
                }
            };
            let mut int = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "SIGINT handler setup failed");
                    return;
                }
            };
            tokio::select! {
                _ = term.recv() => tracing::info!("SIGTERM received"),
                _ = int.recv()  => tracing::info!("SIGINT received"),
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %e, "ctrl_c handler failed");
                return;
            }
            tracing::info!("Ctrl-C received");
        }
        token.cancel();
    });
}
