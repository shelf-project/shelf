//! `shelfctl` — operator CLI for the `shelfd` cache daemon.
//!
//! Ticket ownership:
//! - SHELF-23 — subcommands `stats`, `ring`, `pin <key>`, `unpin
//!   <key>`, `evict <key>`, `reload`. Each talks to `shelfd`'s admin
//!   HTTP surface under `/admin/*`.
//! - SHELF-24 — `reload` is the in-band pin-list reload button. The
//!   same refresh fires on `SIGHUP` and on the 15-minute timer
//!   inside `shelfd` — this subcommand is for operators who want to
//!   bypass the timer without shelling into the pod.
//!
//! The CLI is intentionally thin: every subcommand maps 1:1 to a
//! single HTTP call against `--endpoint`. We deliberately never call
//! into `shelfd`'s internals directly — the admin surface is the
//! contract and exercising it from the CLI is the cheapest way to
//! keep that contract honest.

use anyhow::{anyhow, Context};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

mod bundle;
mod chaos;
mod install;

/// Shelf cache daemon operator CLI.
#[derive(Debug, Parser)]
#[command(name = "shelfctl", version, about, long_about = None)]
struct Cli {
    /// Base URL of the shelfd data-plane / admin endpoint. Defaults
    /// match the in-cluster StatefulSet: `http://shelf-0.shelf.shelf.
    /// svc.cluster.local:8080`. Dev loops use `http://127.0.0.1:8080`.
    #[arg(
        long,
        env = "SHELFCTL_ENDPOINT",
        default_value = "http://127.0.0.1:8080"
    )]
    endpoint: String,

    /// Log level override (`RUST_LOG`-compatible filter).
    #[arg(long, env = "SHELFCTL_LOG", default_value = "warn,shelfctl=info")]
    log: String,

    #[command(subcommand)]
    command: Command,
}

/// Pool selector mirroring `shelfd::store::Pool`. Kept as a local
/// `ValueEnum` (rather than re-exporting from `shelfd`) so `shelfctl`
/// stays a pure HTTP client — no link dependency on the daemon's
/// internals.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum PoolArg {
    Metadata,
    Rowgroup,
}

impl PoolArg {
    fn as_wire(self) -> &'static str {
        match self {
            PoolArg::Metadata => "metadata",
            PoolArg::Rowgroup => "rowgroup",
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Dump `/stats` from the target pod as pretty-printed JSON.
    Stats,
    /// Dump the HRW ring view — pod_id / weight / healthy per row.
    Ring,
    /// Pin a key (content-addressed hex) so it bypasses the size
    /// threshold on admission.
    Pin {
        /// Hex-encoded content-addressed key (64 chars).
        key: String,
        /// Which Foyer pool the key lives in.
        #[arg(long, value_enum, default_value = "rowgroup")]
        pool: PoolArg,
    },
    /// Unpin a key. Pool-agnostic — content-addressed keys are
    /// unique across pools by construction.
    Unpin {
        /// Hex-encoded content-addressed key (64 chars).
        key: String,
    },
    /// Evict a key from the given pool. The pin-set is preserved.
    Evict {
        /// Hex-encoded content-addressed key (64 chars).
        key: String,
        /// Which Foyer pool the key lives in.
        #[arg(long, value_enum, default_value = "rowgroup")]
        pool: PoolArg,
    },
    /// Trigger an out-of-band pin-list reload. Equivalent to
    /// sending the `shelfd` process a `SIGHUP`.
    Reload,
    /// SHELF-31 — kill a fraction of shelfd pods to demonstrate
    /// fail-open behaviour under pod churn.
    Chaos(chaos::ChaosArgs),
    /// SHELF-32 — gather a redacted diagnostic bundle (logs, stats,
    /// metrics, ring view, optional helm values) into a single
    /// tar.gz.
    Bundle(bundle::BundleArgs),
    /// SHELF-33 — auto-detect Trino catalogs, generate values.yaml,
    /// and `helm upgrade --install` the Shelf chart.
    Install(install::InstallArgs),
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

    // The kube-touching subcommands (chaos/bundle/install) drive
    // multiple async kube clients in parallel; spin them on the
    // multi-thread runtime. The legacy admin subcommands are happy
    // here too — single in-flight request each.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(dispatch(cli))
}

async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    // Subcommands that need the kube apiserver own their own client
    // setup (kubeconfig discovery, TLS, etc.) — wire them first so
    // we don't bother spinning up a reqwest client they wouldn't use.
    match cli.command {
        Command::Chaos(args) => return chaos::run(args).await,
        Command::Bundle(args) => return bundle::run(args).await,
        Command::Install(args) => return install::run(args).await,
        _ => {}
    }

    let client = reqwest::Client::builder()
        .user_agent(concat!("shelfctl/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building HTTP client")?;

    match cli.command {
        Command::Stats => cmd_stats(&client, &cli.endpoint).await,
        Command::Ring => cmd_ring(&client, &cli.endpoint).await,
        Command::Pin { key, pool } => cmd_pin(&client, &cli.endpoint, &key, pool).await,
        Command::Unpin { key } => cmd_unpin(&client, &cli.endpoint, &key).await,
        Command::Evict { key, pool } => cmd_evict(&client, &cli.endpoint, &key, pool).await,
        Command::Reload => cmd_reload(&client, &cli.endpoint).await,
        Command::Chaos(_) | Command::Bundle(_) | Command::Install(_) => unreachable!(),
    }
}

fn url(endpoint: &str, path: &str) -> String {
    // Join without depending on `url` crate — endpoint never has a
    // trailing slash in practice, and we control the path literals.
    let endpoint = endpoint.trim_end_matches('/');
    format!("{endpoint}{path}")
}

/// Normalise an HTTP response into `anyhow::Result<Response>` so
/// non-2xx bodies get written to stderr and the process exits 1.
async fn ok_or_bail(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
    eprintln!("{status}: {body}");
    Err(anyhow!("request failed: {status}"))
}

async fn cmd_stats(client: &reqwest::Client, endpoint: &str) -> anyhow::Result<()> {
    let resp = client
        .get(url(endpoint, "/stats"))
        .send()
        .await
        .context("GET /stats")?;
    let resp = ok_or_bail(resp).await?;
    let json: serde_json::Value = resp.json().await.context("parse /stats JSON")?;
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct RingRow {
    pod_id: String,
    weight: f64,
    healthy: bool,
}

async fn cmd_ring(client: &reqwest::Client, endpoint: &str) -> anyhow::Result<()> {
    let resp = client
        .get(url(endpoint, "/admin/ring"))
        .send()
        .await
        .context("GET /admin/ring")?;
    let resp = ok_or_bail(resp).await?;
    let rows: Vec<RingRow> = resp.json().await.context("parse /admin/ring JSON")?;
    // Fixed-width table; operators grep this in noisy output.
    println!("{:<40} {:>8} {:>8}", "pod_id", "weight", "healthy");
    for r in rows {
        println!("{:<40} {:>8.3} {:>8}", r.pod_id, r.weight, r.healthy);
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct PinEvictBody<'a> {
    key_hex: &'a str,
    pool: &'a str,
}

#[derive(Debug, Serialize)]
struct UnpinBody<'a> {
    key_hex: &'a str,
}

async fn cmd_pin(
    client: &reqwest::Client,
    endpoint: &str,
    key: &str,
    pool: PoolArg,
) -> anyhow::Result<()> {
    let resp = client
        .post(url(endpoint, "/admin/pin"))
        .json(&PinEvictBody {
            key_hex: key,
            pool: pool.as_wire(),
        })
        .send()
        .await
        .context("POST /admin/pin")?;
    let resp = ok_or_bail(resp).await?;
    let body: serde_json::Value = resp.json().await.context("parse /admin/pin JSON")?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

async fn cmd_unpin(client: &reqwest::Client, endpoint: &str, key: &str) -> anyhow::Result<()> {
    let resp = client
        .post(url(endpoint, "/admin/unpin"))
        .json(&UnpinBody { key_hex: key })
        .send()
        .await
        .context("POST /admin/unpin")?;
    let resp = ok_or_bail(resp).await?;
    let body: serde_json::Value = resp.json().await.context("parse /admin/unpin JSON")?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

async fn cmd_evict(
    client: &reqwest::Client,
    endpoint: &str,
    key: &str,
    pool: PoolArg,
) -> anyhow::Result<()> {
    let resp = client
        .post(url(endpoint, "/admin/evict"))
        .json(&PinEvictBody {
            key_hex: key,
            pool: pool.as_wire(),
        })
        .send()
        .await
        .context("POST /admin/evict")?;
    let resp = ok_or_bail(resp).await?;
    let body: serde_json::Value = resp.json().await.context("parse /admin/evict JSON")?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

async fn cmd_reload(client: &reqwest::Client, endpoint: &str) -> anyhow::Result<()> {
    let resp = client
        .post(url(endpoint, "/admin/reload"))
        .send()
        .await
        .context("POST /admin/reload")?;
    let resp = ok_or_bail(resp).await?;
    let body: serde_json::Value = resp.json().await.context("parse /admin/reload JSON")?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}
