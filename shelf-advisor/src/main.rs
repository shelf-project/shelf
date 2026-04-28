//! `shelf-advisor` — Phase-1 CLI shim.
//!
//! The binary owns argv parsing, logging setup, and JSON emission.
//! The real work lives behind the `Recommender` trait family in
//! `lib.rs`; main.rs is intentionally boring so future recommenders
//! land without touching the entrypoint.
//!
//! Phase-1 wiring uses an in-binary stub for both readers — the CLI
//! is end-to-end testable without a live Iceberg catalog. SHELF-53
//! will swap these for real `iceberg-rust`-backed implementations.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use shelf_advisor::{
    default_recommenders, run_pipeline, write_recommendations_json, AdvisorConfig, DataFile,
    IcebergEventLogReader, IcebergManifestReader, QueryRecord,
};

/// Standalone advisor that mines Trino event-listener data + Iceberg
/// manifests and emits JSON recommendations.
#[derive(Debug, Parser)]
#[command(name = "shelf-advisor", version, about, long_about = None)]
struct Cli {
    /// Log filter (`RUST_LOG`-compatible). Defaults match `shelfctl`
    /// so operators see the same default verbosity across the
    /// Shelf binary suite.
    #[arg(long, env = "SHELF_ADVISOR_LOG", default_value = "warn,shelf_advisor=info")]
    log: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run all recommenders over the configured lookback window and
    /// write a JSON array of recommendations to `--output`.
    Analyze {
        /// Lookback window for the event-log scan. Accepts
        /// humantime-style strings: `1d`, `7d`, `24h`, `30m`.
        #[arg(long, default_value = "7d")]
        window: String,

        /// Destination path for the JSON output. Parent directory
        /// must exist; the file is overwritten if it already does.
        #[arg(long)]
        output: PathBuf,

        /// Override for the event-log table location. Defaults to
        /// the Phase-1 placeholder; real deployments will set this
        /// to the catalog/schema/table where their Iceberg-sink
        /// event-listener jar writes.
        #[arg(long, default_value = AdvisorConfig::DEFAULT_EVENT_LOG_TABLE)]
        event_log_table: String,

        /// Cap on recommendations per `(table, recommendation_type)`
        /// pair. Mirrors the false-positive guardrail in
        /// `feature-ideas-ranked.md` Tier S #4.
        #[arg(long, default_value_t = AdvisorConfig::DEFAULT_TOP_N_PER_TABLE)]
        top_n_per_table: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&cli.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    match cli.command {
        Command::Analyze {
            window,
            output,
            event_log_table,
            top_n_per_table,
        } => {
            let window = parse_window(&window)
                .with_context(|| format!("failed to parse --window {window:?}"))?;

            let config = AdvisorConfig {
                event_log_table,
                output_path: output.clone(),
                window,
                top_n_per_table,
            };

            tracing::info!(
                event_log_table = %config.event_log_table,
                window_secs = config.window.as_secs(),
                output = %output.display(),
                "shelf-advisor analyze starting"
            );

            // Phase-1 stub readers. These return empty data so the
            // pipeline runs end-to-end — the recommenders are
            // already stubbed to `Ok(vec![])`, so the *output*
            // would be `[]` regardless. Wiring them in here keeps
            // the trait surface honest and gives SHELF-53 a single
            // diff site to drop the real readers in.
            let event_log = StubEventLogReader;
            let manifests = StubManifestReader;
            let recs = run_pipeline(&config, &event_log, &manifests, &default_recommenders())?;

            write_recommendations_json(&output, &recs).with_context(|| {
                format!("failed to write recommendations to {}", output.display())
            })?;

            tracing::info!(count = recs.len(), "shelf-advisor analyze done");
        }
    }

    Ok(())
}

/// Tiny humantime-ish parser: accepts `<n><unit>` where unit is one
/// of `s`, `m`, `h`, `d`. We deliberately don't pull in a full
/// `humantime` dep for Phase-1 — the workspace is keeping its dep
/// list lean (`feature-ideas-ranked.md` Tier S #4 explicitly calls
/// out "no heavy deps for now").
fn parse_window(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty window");
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u64 = num
        .parse()
        .with_context(|| format!("expected <number><unit>, got {s:?}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 60 * 60,
        "d" => n * 60 * 60 * 24,
        other => anyhow::bail!("unknown window unit {other:?} (expected s|m|h|d)"),
    };
    Ok(Duration::from_secs(secs))
}

/// Phase-1 placeholder reader. Replaced by an Iceberg-backed reader
/// in SHELF-53.
struct StubEventLogReader;

impl IcebergEventLogReader for StubEventLogReader {
    fn read_window(&self, _window: Duration) -> shelf_advisor::Result<Vec<QueryRecord>> {
        Ok(Vec::new())
    }
}

/// Phase-1 placeholder reader. Replaced by an Iceberg-backed reader
/// in SHELF-53.
struct StubManifestReader;

impl IcebergManifestReader for StubManifestReader {
    fn list_files(&self, _table: &str) -> shelf_advisor::Result<Vec<DataFile>> {
        Ok(Vec::new())
    }
}
