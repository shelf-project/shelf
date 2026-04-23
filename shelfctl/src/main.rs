//! `shelfctl` — operator CLI for the `shelfd` cache daemon.
//!
//! Ticket ownership:
//! - SHELF-23 — subcommands `stats`, `pin`, `unpin`, `evict`, `ring`,
//!   `reload`. Each talks to `shelfd`'s control plane (HTTP in v1;
//!   the gRPC scaffold in `shelfd/src/control.rs` will grow into a
//!   real surface as SHELF-23 lands).
//! - SHELF-24 — `reload pin-list` triggers SIGHUP on the target pod.
//!
//! Every subcommand body is `todo!()` until SHELF-23 merges, but the
//! clap `derive` layout is final: operators see stable `--help` text
//! from day one, which is the spec other agents (plugin, SRE) consume.

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

/// Shelf cache daemon operator CLI.
#[derive(Debug, Parser)]
#[command(name = "shelfctl", version, about, long_about = None)]
struct Cli {
    /// Base URL of the shelfd control endpoint, e.g.
    /// `http://shelf-0.shelf.shelf.svc.cluster.local:9091`.
    #[arg(
        long,
        env = "SHELFCTL_ENDPOINT",
        default_value = "http://127.0.0.1:9091"
    )]
    endpoint: String,

    /// Log level override (`RUST_LOG`-compatible filter).
    #[arg(long, env = "SHELFCTL_LOG", default_value = "warn,shelfctl=info")]
    log: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Dump live cache statistics from the target pod.
    Stats {
        /// Optional granularity filter (`row-group`, `footer`, `manifest`).
        #[arg(long)]
        granularity: Option<String>,
    },
    /// Pin a table (optionally scoped to partitions) in the pin list.
    Pin {
        /// Fully-qualified table name, e.g. `cdp.icesheet.silver_offline_event_data_2026`.
        table: String,
        /// Optional `key=value` partition predicates; may repeat.
        #[arg(long)]
        partition: Vec<String>,
    },
    /// Remove a table from the pin list.
    Unpin { table: String },
    /// Forcibly evict a single key from the cache.
    Evict {
        /// Hex-encoded content-addressed key.
        key: String,
    },
    /// Dump the current HRW ring view (membership + capacity weights).
    Ring,
    /// Trigger an out-of-band reload of a live config surface.
    Reload {
        /// What to reload.
        #[arg(value_parser = ["pin-list", "admission-model"])]
        target: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let env_filter =
        EnvFilter::try_new(&cli.log).unwrap_or_else(|_| EnvFilter::new("warn,shelfctl=info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init()
        .ok();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(dispatch(&cli))
}

async fn dispatch(cli: &Cli) -> anyhow::Result<()> {
    match &cli.command {
        Command::Stats { granularity } => cmd_stats(&cli.endpoint, granularity.as_deref()).await,
        Command::Pin { table, partition } => cmd_pin(&cli.endpoint, table, partition).await,
        Command::Unpin { table } => cmd_unpin(&cli.endpoint, table).await,
        Command::Evict { key } => cmd_evict(&cli.endpoint, key).await,
        Command::Ring => cmd_ring(&cli.endpoint).await,
        Command::Reload { target } => cmd_reload(&cli.endpoint, target).await,
    }
}

async fn cmd_stats(_endpoint: &str, _granularity: Option<&str>) -> anyhow::Result<()> {
    todo!(
        "SHELF-23: shelfctl: GET {{endpoint}}/stats?granularity=… and pretty-print; \
         see 03-plan.md §4 SHELF-23"
    )
}

async fn cmd_pin(_endpoint: &str, _table: &str, _partitions: &[String]) -> anyhow::Result<()> {
    todo!(
        "SHELF-23: shelfctl: POST {{endpoint}}/pin {{table, partitions}}; see \
         03-plan.md §4 SHELF-23 + SHELF-24"
    )
}

async fn cmd_unpin(_endpoint: &str, _table: &str) -> anyhow::Result<()> {
    todo!("SHELF-23: shelfctl: DELETE {{endpoint}}/pin/{{table}}; see 03-plan.md §4 SHELF-23")
}

async fn cmd_evict(_endpoint: &str, _key: &str) -> anyhow::Result<()> {
    todo!("SHELF-23: shelfctl: POST {{endpoint}}/evict {{key}}; see 03-plan.md §4 SHELF-23")
}

async fn cmd_ring(_endpoint: &str) -> anyhow::Result<()> {
    todo!(
        "SHELF-23: shelfctl: GET {{endpoint}}/ring and render membership + \
         weights in a sorted table; see 03-plan.md §4 SHELF-23"
    )
}

async fn cmd_reload(_endpoint: &str, _target: &str) -> anyhow::Result<()> {
    todo!(
        "SHELF-23: shelfctl: POST {{endpoint}}/reload/{{target}}; see 03-plan.md §4 \
         SHELF-23 + SHELF-24"
    )
}
