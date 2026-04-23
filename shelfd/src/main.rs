//! `shelfd` binary entry point.
//!
//! Ticket ownership:
//! - SHELF-02 — Axum HTTP server skeleton, `/healthz`, `/readyz`,
//!   `/metrics`, graceful shutdown, structured logging.
//! - SHELF-08 — Prometheus registry + OTel traces are wired in here
//!   so that every subsystem emits through the same pipeline.
//!
//! The real `main` will compose `config::Config`, `metrics::Registry`,
//! `origin::Origin`, `store::Store`, `router::Router`,
//! `admission::AdmissionPolicy`, `membership::Resolver`, and
//! `http::serve` into a graceful-shutdown loop. This scaffold only
//! parses args + wires tracing so that `cargo run --bin shelfd
//! --help` prints something sensible.

use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Command-line arguments for `shelfd`. Kept intentionally small; all
/// tunables live in `Config` (see `config::Config` and
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

    tracing::info!(
        config_path = %args.config.display(),
        "shelfd startup (scaffold — SHELF-02 not yet implemented)"
    );

    // SHELF-02 / SHELF-06 / SHELF-17 / SHELF-18 / SHELF-20:
    // The real entry point will:
    //   1. load config (config::Config::from_path)
    //   2. init metrics registry (metrics::init)
    //   3. build origin client (origin::Origin::new)
    //   4. build store (store::Store::open)
    //   5. build router + membership
    //   6. spawn http::serve + control::serve on a tokio runtime
    //   7. wait on a SIGTERM-driven CancellationToken
    //
    // Until those tickets land, `shelfd` exits cleanly so that
    // `docker run shelfd:0.1 --help` and CI smoke tests are green.
    Ok(())
}

/// Initialise `tracing` with an `EnvFilter` + JSON formatter.
///
/// The JSON layer is structured so that rep-2's Loki / OTel collectors
/// can scrape without custom parsers (SHELF-08).
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
