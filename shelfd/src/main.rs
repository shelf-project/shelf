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
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

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
    init_tracing(&args.log)?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async { run(args).await })
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config =
        Config::from_path(&args.config).map_err(|e| anyhow::anyhow!("config load failed: {e}"))?;
    tracing::info!(node = %config.node.id, "shelfd starting");

    let metrics = Arc::new(metrics::Registry::init()?);
    let origin = Arc::new(S3Origin::new(&config.origin).await?);
    let store = Arc::new(FoyerStore::open(&config.pools).await?);
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&config.admission));
    let head_lru = Arc::new(HeadLru::new(config.head_lru_entries));

    let state = Arc::new(ServerState::with_head_lru_and_pod_id(
        store.clone(),
        origin.clone(),
        router,
        admission,
        metrics,
        head_lru,
        config.node.id.clone(),
    ));
    // Phase-0: mark ready as soon as Foyer + S3 client built. The
    // origin head-bucket probe would go here once SHELF-07 lands.
    state.mark_ready();

    let shutdown = CancellationToken::new();
    spawn_signal_handler(shutdown.clone());

    let listen = config.http.listen;
    let request_timeout = config.http.request_timeout;
    tracing::info!(%listen, ?request_timeout, "binding data plane");
    http::serve(listen, state, request_timeout, shutdown).await?;
    tracing::info!("shelfd shutdown complete");
    Ok(())
}

fn init_tracing(filter: &str) -> anyhow::Result<()> {
    let env_filter =
        EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info,shelfd=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .json()
        .flatten_event(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init failed: {e}"))?;
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
