//! `shelf-advisor` CLI shim.
//!
//! Four subcommands:
//!
//! * `recommend [all|optimize|pin-list|bloom|mv]` — primary
//!   command per the canonical SHELF-53 design note. Writes a
//!   versioned [`Envelope`](shelf_advisor::Envelope) to a single
//!   file or one envelope per kind under an output directory.
//! * `analyze` — backward-compatible alias of `recommend all`
//!   that emits a bare-array JSON file at the path passed via
//!   `--output`. Kept so the SHELF-34 scaffold's smoke test and
//!   any downstream pipeline that pinned to the older shape keep
//!   working.
//! * `watch` — periodic `recommend all` loop with a minimal
//!   Prometheus exposition endpoint at `:9100/metrics`.
//! * `dry-run` — replay a fixture (event log + manifests +
//!   `/stats`) bundled into a single JSON document; primarily
//!   used by CI tests but operators run it for local sanity-checks.
//!
//! In all four modes, the recommender pipeline is identical —
//! only the I/O surface differs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use tracing_subscriber::EnvFilter;

use shelf_advisor::{
    default_recommenders, render_rfc3339_utc, run_pipeline, run_pipeline_bare, write_envelope_json,
    write_per_kind_dir, write_recommendations_json, AdvisorConfig, AnalysisContext,
    BloomWriteConfig, DataFile, FixtureEventLogReader, FixtureManifestReader,
    FixtureShelfdStatsReader, HttpShelfdStatsReader, IcebergEventLogReader, IcebergManifestReader,
    PodStats, QueryRecord, ShelfdStatsReader,
};

/// CLI form of the recommendation-type discriminator. clap's
/// `ValueEnum` derive renders these in kebab-case (`all`,
/// `optimize`, `pin-list`, `bloom`, `mv`); `to_filter` maps them
/// to the canonical snake_case kind string used in the
/// recommendation envelope's `recommendation_type` field.
///
/// Lives in `main.rs` (not `lib.rs`) so the library surface stays
/// clap-free for downstream embedders.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum KindArg {
    All,
    Optimize,
    PinList,
    Bloom,
    Mv,
}

impl KindArg {
    fn to_filter(self) -> Option<&'static str> {
        match self {
            KindArg::All => None,
            KindArg::Optimize => Some("optimize_targets"),
            KindArg::PinList => Some("pin_list_candidates"),
            KindArg::Bloom => Some("bloom_filter_columns"),
            KindArg::Mv => Some("mv_candidates"),
        }
    }
}

/// Standalone advisor that mines Trino event-listener data,
/// Iceberg manifests, and shelfd `/stats` and emits JSON
/// recommendations for `OPTIMIZE` targets, pin-list candidates,
/// and (via SHELF-52 / SHELF-65) bloom-write columns + MV
/// pinning.
#[derive(Debug, Parser)]
#[command(name = "shelf-advisor", version, about, long_about = None)]
struct Cli {
    /// Log filter (`RUST_LOG`-compatible). Defaults match
    /// shelfctl so operators see the same default verbosity
    /// across the Shelf binary suite.
    #[arg(
        long,
        env = "SHELF_ADVISOR_LOG",
        default_value = "warn,shelf_advisor=info"
    )]
    log: String,

    /// Path to the YAML config file. Defaults to
    /// `~/.shelf-advisor/config.yaml`; missing-file is non-fatal
    /// (advisor falls back to compiled defaults + flags).
    #[arg(long, env = "SHELF_ADVISOR_CONFIG", global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the recommender pipeline once and write a versioned
    /// envelope. The `kind` argument selects which recommendation
    /// type to emit; pass `all` for every kind in one go.
    Recommend {
        /// `all` | `optimize` | `pin-list` | `bloom` | `mv`.
        #[arg(value_enum)]
        kind: KindArg,

        /// Lookback window for the event-log scan.
        #[arg(long, default_value = "7d")]
        window: String,

        /// Single-file output. Mutually exclusive with
        /// `--output-dir`. Always writes the envelope (versioned)
        /// shape, regardless of how many kinds are present.
        #[arg(long, conflicts_with = "output_dir")]
        output: Option<PathBuf>,

        /// Per-kind directory output: writes
        /// `<dir>/<YYYY-MM-DD>/<kind>.json`. Mirrors the canonical
        /// SHELF-53 design note's output layout.
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// RFC3339 wall-clock pin for the run. Tests pass a fixed
        /// value to keep snapshots byte-stable; operators leave
        /// unset and the advisor uses `now`.
        #[arg(long)]
        as_of: Option<String>,

        /// Override the event-log table from config / default.
        #[arg(long)]
        event_log_table: Option<String>,

        /// Override `top_n_per_table` from config / default.
        #[arg(long)]
        top_n_per_table: Option<usize>,

        /// Optional dry-run-style fixture; when set, both event
        /// log and manifests are replayed from this file instead
        /// of the live readers. Useful for "run the same
        /// recommend command CI runs against fixture X".
        #[arg(long)]
        fixture: Option<PathBuf>,
    },

    /// Backward-compat alias: same pipeline as `recommend all` but
    /// writes a bare JSON array (no envelope) to `--output`.
    /// Preserves the SHELF-34 phase-1 scaffold's CLI contract +
    /// integration smoke-test.
    Analyze {
        /// Lookback window for the event-log scan.
        #[arg(long, default_value = "7d")]
        window: String,

        /// Destination path for the JSON output.
        #[arg(long)]
        output: PathBuf,

        /// Override the event-log table from config / default.
        #[arg(long, default_value = AdvisorConfig::DEFAULT_EVENT_LOG_TABLE)]
        event_log_table: String,

        /// Override `top_n_per_table` from config / default.
        #[arg(long, default_value_t = AdvisorConfig::DEFAULT_TOP_N_PER_TABLE)]
        top_n_per_table: usize,
    },

    /// Re-run the pipeline every `--interval` and write a fresh
    /// report; expose run / per-category counters at
    /// `--prom-listen`.
    Watch {
        /// Wall-clock interval between runs. Same humantime grammar
        /// as `--window`.
        #[arg(long, default_value = "15m")]
        interval: String,

        /// Lookback window passed into each run.
        #[arg(long, default_value = "24h")]
        window: String,

        /// Where to write the latest envelope on each tick.
        #[arg(long, default_value = "/var/lib/shelf-advisor/report.json")]
        output: PathBuf,

        /// Listen address for the Prometheus exposition endpoint.
        /// Set to empty string to disable.
        #[arg(long, default_value = "0.0.0.0:9100")]
        prom_listen: String,
    },

    /// Replay a fixture and emit the resulting envelope. Used by
    /// CI tests to drive the full pipeline against a known input.
    DryRun {
        /// Input fixture (JSON document with `event_log`,
        /// `manifests`, `shelfd_stats` keys; see
        /// `tests/fixtures/dry_run_input.json` for the schema).
        #[arg(long)]
        fixture: PathBuf,

        /// Output path. Always writes envelope shape.
        #[arg(long)]
        output: PathBuf,

        /// RFC3339 `as_of` pin; defaults to a frozen value so the
        /// dry-run output is byte-stable for snapshot tests.
        #[arg(long, default_value = "2026-04-30T00:00:00Z")]
        as_of: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&cli.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let base_config = load_base_config(cli.config.as_deref())?;

    match cli.command {
        Command::Recommend {
            kind,
            window,
            output,
            output_dir,
            as_of,
            event_log_table,
            top_n_per_table,
            fixture,
        } => {
            let window = parse_window(&window)
                .with_context(|| format!("failed to parse --window {window:?}"))?;
            let mut cfg = base_config.clone();
            cfg.window = window;
            if let Some(t) = event_log_table {
                cfg.event_log_table = t;
            }
            if let Some(n) = top_n_per_table {
                cfg.top_n_per_table = n;
            }
            cfg.output_path = output.clone().unwrap_or_else(|| PathBuf::from("/dev/null"));

            let env = if let Some(p) = fixture.as_ref() {
                run_against_fixture(&cfg, p, as_of.clone())?
            } else {
                run_against_live(&cfg, as_of.clone()).await?
            };

            let env = if let Some(filter) = kind.to_filter() {
                env.for_kind(filter)
            } else {
                env
            };

            if let Some(out) = output {
                write_envelope_json(&out, &env)
                    .with_context(|| format!("failed to write envelope to {}", out.display()))?;
                tracing::info!(
                    count = env.recommendations.len(),
                    output = %out.display(),
                    "shelf-advisor recommend done (single-file envelope)"
                );
            } else if let Some(dir) = output_dir {
                let written = write_per_kind_dir(&dir, &env).with_context(|| {
                    format!("failed to write per-kind envelopes under {}", dir.display())
                })?;
                tracing::info!(
                    count = env.recommendations.len(),
                    files = written.len(),
                    dir = %dir.display(),
                    "shelf-advisor recommend done (per-kind directory)"
                );
            } else {
                anyhow::bail!("provide either --output FILE or --output-dir DIR");
            }
        }
        Command::Analyze {
            window,
            output,
            event_log_table,
            top_n_per_table,
        } => {
            let window = parse_window(&window)
                .with_context(|| format!("failed to parse --window {window:?}"))?;
            let mut cfg = base_config.clone();
            cfg.window = window;
            cfg.event_log_table = event_log_table;
            cfg.top_n_per_table = top_n_per_table;
            cfg.output_path = output.clone();

            // Backward-compat path: empty live readers (no fixture
            // wired) + bare-array writer. The SHELF-34 smoke test
            // depends on this writing `[]` for an empty input.
            let recs = run_against_live_bare(&cfg).await?;

            write_recommendations_json(&output, &recs).with_context(|| {
                format!("failed to write recommendations to {}", output.display())
            })?;
            tracing::info!(count = recs.len(), "shelf-advisor analyze done");
        }
        Command::Watch {
            interval,
            window,
            output,
            prom_listen,
        } => {
            let interval = parse_window(&interval)
                .with_context(|| format!("failed to parse --interval {interval:?}"))?;
            let window = parse_window(&window)
                .with_context(|| format!("failed to parse --window {window:?}"))?;
            let mut cfg = base_config.clone();
            cfg.window = window;
            cfg.output_path = output.clone();

            let metrics = shelf_advisor::runtime::RuntimeMetrics::new();
            if !prom_listen.is_empty() {
                let addr: std::net::SocketAddr = prom_listen
                    .parse()
                    .with_context(|| format!("invalid --prom-listen {prom_listen:?}"))?;
                let bound =
                    shelf_advisor::runtime::spawn_prom_listener(addr, metrics.clone()).await?;
                tracing::info!(listen = %bound, "prometheus exposition listening");
            }

            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                let now = render_rfc3339_utc(SystemTime::now());
                match run_against_live(&cfg, Some(now)).await {
                    Ok(env) => {
                        if let Err(e) = write_envelope_json(&output, &env) {
                            tracing::warn!(error = %e, "watch: envelope write failed");
                            metrics.record_failure();
                            continue;
                        }
                        metrics.record_run(&env.recommendations);
                        tracing::info!(
                            count = env.recommendations.len(),
                            output = %output.display(),
                            "watch: tick complete"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "watch: pipeline failed");
                        metrics.record_failure();
                    }
                }
            }
        }
        Command::DryRun {
            fixture,
            output,
            as_of,
        } => {
            let cfg = base_config.clone();
            let env = run_against_fixture(&cfg, &fixture, Some(as_of))?;
            write_envelope_json(&output, &env)?;
            tracing::info!(
                count = env.recommendations.len(),
                output = %output.display(),
                "shelf-advisor dry-run done"
            );
        }
    }
    Ok(())
}

/// Layered config loader. Operator's YAML file (if present) wins
/// over compiled defaults; per-flag overrides win over both.
fn load_base_config(explicit: Option<&std::path::Path>) -> Result<AdvisorConfig> {
    let path = match explicit {
        Some(p) => Some(p.to_path_buf()),
        None => default_config_path(),
    };
    if let Some(p) = path {
        if p.exists() {
            return AdvisorConfig::from_yaml_file(&p)
                .with_context(|| format!("loading config from {}", p.display()));
        }
    }
    Ok(AdvisorConfig::defaults(
        PathBuf::from("/dev/null"),
        Duration::from_secs(7 * 86_400),
    ))
}

fn default_config_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let mut p = PathBuf::from(home);
    p.push(".shelf-advisor");
    p.push("config.yaml");
    Some(p)
}

/// Tiny humantime parser. Accepts `<n><unit>` with `s|m|h|d`. We
/// keep this in-tree rather than depending on the full `humantime`
/// crate because the CLI grammar is bounded — adding a heavy
/// dep for `<u64><char>` parsing is bad value.
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

/// Build readers + tables list and run the pipeline against the
/// live cluster.
async fn run_against_live(
    cfg: &AdvisorConfig,
    as_of: Option<String>,
) -> Result<shelf_advisor::Envelope> {
    let event_log = LiveEventLogReader::new();
    let manifests = LiveManifestReader::new();
    let stats = build_live_stats_reader(cfg)?;
    let tables = derive_tables(&event_log, &manifests).await?;
    let ctx = AnalysisContext {
        config: cfg,
        event_log: &event_log,
        manifests: &manifests,
        shelfd_stats: stats.as_ref(),
        tables: &tables,
    };
    let pin = as_of.unwrap_or_else(|| render_rfc3339_utc(SystemTime::now()));
    run_pipeline(&ctx, &default_recommenders(), pin)
}

async fn run_against_live_bare(cfg: &AdvisorConfig) -> Result<Vec<shelf_advisor::Recommendation>> {
    let event_log = LiveEventLogReader::new();
    let manifests = LiveManifestReader::new();
    let stats = build_live_stats_reader(cfg)?;
    let tables = derive_tables(&event_log, &manifests).await?;
    let ctx = AnalysisContext {
        config: cfg,
        event_log: &event_log,
        manifests: &manifests,
        shelfd_stats: stats.as_ref(),
        tables: &tables,
    };
    run_pipeline_bare(&ctx, &default_recommenders())
}

fn build_live_stats_reader(cfg: &AdvisorConfig) -> Result<Box<dyn ShelfdStatsReader>> {
    if cfg.shelfd_stats_urls.is_empty() {
        Ok(Box::new(FixtureShelfdStatsReader::empty()))
    } else {
        Ok(Box::new(HttpShelfdStatsReader::new(
            cfg.shelfd_stats_urls.clone(),
            None,
        )?))
    }
}

async fn derive_tables(
    _event_log: &dyn IcebergEventLogReader,
    _manifests: &dyn IcebergManifestReader,
) -> Result<Vec<String>> {
    // Live tables list comes from the operator-supplied config in
    // a follow-up; today the live readers are empty stubs (the
    // production Trino client is intentionally deferred per the
    // SHELF-53 user override) so there is nothing to discover.
    Ok(Vec::new())
}

/// Live event-log reader stub. Returns `Ok(vec![])` so the
/// `analyze` legacy command keeps emitting `[]` until SHELF-65 /
/// SHELF-52 land their JDBC bridge.
//
// TODO(SHELF-53-followup): pick a Trino-Rust client (`prusto`,
// shellout to `trino-cli`, or a sidecar) and wire it here. The
// trait + fixture path lets every recommender land in tree
// without blocking on the choice.
struct LiveEventLogReader;

impl LiveEventLogReader {
    fn new() -> Self {
        Self
    }
}

impl IcebergEventLogReader for LiveEventLogReader {
    fn read_window(&self, _window: Duration) -> shelf_advisor::Result<Vec<QueryRecord>> {
        Ok(Vec::new())
    }
}

/// Live manifest reader stub. See `LiveEventLogReader` for the
/// follow-up note.
struct LiveManifestReader;

impl LiveManifestReader {
    fn new() -> Self {
        Self
    }
}

impl IcebergManifestReader for LiveManifestReader {
    fn list_files(&self, _table: &str) -> shelf_advisor::Result<Vec<DataFile>> {
        Ok(Vec::new())
    }
}

/// Run the pipeline against the dry-run fixture file. Bundles the
/// three reader inputs into one document to keep CI fixtures
/// self-contained.
fn run_against_fixture(
    cfg: &AdvisorConfig,
    path: &std::path::Path,
    as_of: Option<String>,
) -> Result<shelf_advisor::Envelope> {
    let bytes = std::fs::read(path).with_context(|| format!("read fixture {}", path.display()))?;
    let doc: FixtureDoc = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse fixture {}", path.display()))?;

    let event_log = FixtureEventLogReader::new(doc.event_log);
    let manifests = FixtureManifestReader::new(doc.manifests);
    let stats = FixtureShelfdStatsReader::new(doc.shelfd_stats);

    let mut tables: std::collections::BTreeSet<String> =
        manifests.iter_tables().map(|(t, _)| t.clone()).collect();
    for r in event_log.read_window(cfg.window)? {
        tables.insert(r.table);
    }
    let tables: Vec<String> = tables.into_iter().collect();

    let ctx = AnalysisContext {
        config: cfg,
        event_log: &event_log,
        manifests: &manifests,
        shelfd_stats: &stats,
        tables: &tables,
    };
    let pin = as_of.unwrap_or_else(|| render_rfc3339_utc(SystemTime::now()));
    run_pipeline(&ctx, &default_recommenders(), pin)
}

#[derive(Debug, serde::Deserialize)]
struct FixtureDoc {
    #[serde(default)]
    event_log: Vec<QueryRecord>,
    #[serde(default)]
    manifests: HashMap<String, Vec<DataFile>>,
    #[serde(default)]
    shelfd_stats: Vec<PodStats>,
}
