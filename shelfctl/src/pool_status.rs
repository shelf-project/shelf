//! RC7 D1 — `shelfctl pool-status`.
//!
//! Aggregate `/stats` (and optionally `/metrics`) across every
//! `shelfd` pod in a namespace and render a single table. Eliminates
//! the per-pod `kubectl port-forward 19090:9090` loop that operators
//! were running by hand and that has bitten this team twice in one
//! morning with local-port collisions (a stale subprocess on
//! `127.0.0.1:19090` silently steers all probes into the wrong pod
//! and produces "every pod looks identical" reports).
//!
//! Discovery uses `kube::api::Api::list` against a label selector
//! (default `app.kubernetes.io/name=shelfd`, the standard label
//! emitted by `charts/shelf/templates/_helpers.tpl`). HTTP fetching
//! goes through a per-pod `kubectl port-forward pod/<name> :<port>`
//! subprocess where the empty local port asks the OS for an ephemeral
//! port — guaranteeing each pod's probe lands on a unique
//! `127.0.0.1:<random>` socket and that no probe in this command run
//! (or in any other tool the operator left running on `19090`) can
//! collide. The chosen port is parsed back out of the
//! `Forwarding from 127.0.0.1:NNNN -> ...` line `kubectl` prints to
//! stdout when the listener is up, so we never have to guess timing.
//!
//! The wire shape we deserialize is a permissive subset of
//! [`shelfd::control::Stats`] (with `rowgroup_pool` made optional so
//! older daemons that predate SHELF-18 still parse). This is the
//! same compatibility posture `shelfctl stats` uses — `shelfctl` is
//! intentionally tolerant of older `/stats` payloads so an operator
//! can drive a mixed-version cluster during a rolling upgrade
//! without replacing the CLI.

use anyhow::{anyhow, Context, Result};
use clap::Args;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::Client;
use serde::{Deserialize, Serialize};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// CLI arguments for `shelfctl pool-status`.
///
/// Defaults match the stock chart shape (`alluxio` namespace from
/// `charts/shelf/values.yaml`, `app.kubernetes.io/name=shelf` from
/// `_helpers.tpl`, data port `9090` from `values.yaml`). Every
/// default is configurable; on a vanilla
/// `helm install shelf shelf-project/shelf` the discovery selector
/// matches the StatefulSet's pod labels out of the box.
#[derive(Debug, Args)]
pub struct PoolStatusArgs {
    /// Kubernetes namespace shelfd is installed in. Short form `-n`
    /// is provided to match `kubectl` muscle memory.
    #[arg(short = 'n', long, default_value = "alluxio")]
    pub namespace: String,

    /// Label selector identifying shelfd pods. The default matches
    /// the chart's `app.kubernetes.io/name` label, which
    /// `_helpers.tpl` emits as the chart name (`shelf`).
    #[arg(short = 'l', long, default_value = "app.kubernetes.io/name=shelf")]
    pub selector: String,

    /// Optional explicit pod-name list (skips kube discovery). Useful
    /// when you have RBAC to `pods/portforward` but not `pods/list`,
    /// or when you want to scope a probe to a single pod during a
    /// rolling restart. Comma-separated.
    #[arg(long, value_delimiter = ',')]
    pub pods: Option<Vec<String>>,

    /// Output format. `table` is the human-friendly default, `json`
    /// is for piping into `jq`, `tsv` is for piping into `awk` /
    /// `cut` (the column count and order are stable across versions
    /// — that's the contract).
    #[arg(long, default_value = "table", value_parser = ["table", "json", "tsv"])]
    pub format: String,

    /// Also fetch `/metrics` from each pod and aggregate hit-ratio
    /// counters (`shelf_hits_total` / `shelf_misses_total`). Slower
    /// — adds roughly a second per pod because the metrics body is
    /// O(KB) text. Off by default so the standard "is everyone
    /// alive?" probe stays sub-second.
    #[arg(long)]
    pub metrics: bool,

    /// Data port (`/stats`, `/metrics`, `/healthz`, `/readyz`,
    /// `/admin/*`). Configurable so the command keeps working if a
    /// future chart version moves it.
    #[arg(long, default_value_t = 9090)]
    pub data_port: u16,

    /// Per-pod probe timeout in seconds. Includes the
    /// port-forward warmup (≤ 3s in practice) plus the HTTP GET
    /// itself.
    #[arg(long, default_value_t = 10)]
    pub timeout_secs: u64,
}

/// Wire-compatible mirror of `shelfd::control::PoolStats`.
///
/// `disk_*` fields default to `0` so we still parse a payload
/// produced by a daemon older than SHELF-18 (when the disk-tier
/// fields were added). Field names are exact-match against the
/// daemon's `serde::Serialize` derive — `serde` is case-sensitive
/// and we want a parse failure if the contract drifts, not silent
/// `0`s.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ShelfPoolStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    #[serde(default)]
    pub disk_used_bytes: u64,
    #[serde(default)]
    pub disk_capacity_bytes: u64,
}

/// Wire-compatible mirror of `shelfd::control::Stats`.
///
/// `rowgroup_pool` is `Option` even though current daemons always
/// populate it: the spec asks for graceful handling of older builds
/// that may have shipped without the field, and the cost is just a
/// `None` branch in the renderer. `pinned_bytes` / `pinned_count` /
/// `draining` / `rss_bytes` all `#[serde(default)]` for the same
/// mixed-version compat reason `shelfd::control::Stats` declared
/// them with `#[serde(default)]` in the first place.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ShelfStats {
    pub pod_id: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub metadata_pool: ShelfPoolStats,
    #[serde(default)]
    pub rowgroup_pool: Option<ShelfPoolStats>,
    #[serde(default)]
    pub pinned_bytes: u64,
    #[serde(default)]
    pub pinned_count: u64,
    #[serde(default)]
    pub draining: bool,
    #[serde(default)]
    pub rss_bytes: u64,
}

/// Parsed `/metrics` aggregates we surface in the table when
/// `--metrics` is on. Counter parsing is intentionally string-based:
/// pulling in `prometheus-parse` would double the dep weight for two
/// integer extractions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PromAggregate {
    pub hits: u64,
    pub misses: u64,
}

impl PromAggregate {
    pub fn hit_ratio_pct(&self) -> Option<f64> {
        let total = self.hits.saturating_add(self.misses);
        if total == 0 {
            None
        } else {
            Some(100.0 * (self.hits as f64) / (total as f64))
        }
    }
}

/// One row in the aggregated table — one shelfd pod's view of
/// itself, plus k8s-side facts (restart count) we get for free
/// from the Pod we already listed.
#[derive(Debug, Clone, Serialize)]
pub struct PodStatus {
    pub pod_name: String,
    pub restart_count: i32,
    /// `Some(stats)` if the probe succeeded, `None` if anything in
    /// the discover-then-fetch path errored. The renderer prints a
    /// `?` for the affected columns and the whole `error` field
    /// flows through to JSON output for callers that want to alert.
    pub stats: Option<ShelfStats>,
    pub metrics: Option<PromAggregate>,
    pub error: Option<String>,
}

pub async fn run(args: PoolStatusArgs) -> Result<()> {
    let client = Client::try_default()
        .await
        .context("building kube client (kubeconfig or in-cluster config)")?;
    let pods_api: Api<Pod> = Api::namespaced(client, &args.namespace);

    let pod_objs = if let Some(names) = &args.pods {
        // Operator-supplied list: one Get per name. We still load the
        // full Pod object so `restart_count` works even on an
        // explicit list.
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            match pods_api.get(n).await {
                Ok(p) => out.push(p),
                Err(e) => {
                    eprintln!("WARN: get pod/{n}: {e} (skipping)");
                }
            }
        }
        out
    } else {
        let lp = ListParams::default().labels(&args.selector);
        pods_api
            .list(&lp)
            .await
            .with_context(|| {
                format!(
                    "listing pods in ns={} selector={}",
                    args.namespace, args.selector
                )
            })?
            .items
    };

    if pod_objs.is_empty() {
        return Err(anyhow!(
            "no pods matched ns={} selector={} (use --pods to override discovery)",
            args.namespace,
            args.selector
        ));
    }

    // Probe pods concurrently. `kubectl port-forward` opens a
    // separate listener per child so there's no shared resource to
    // serialize on; the kube apiserver also handles parallel
    // port-forward streams just fine. Cap at the pod count — no
    // need to invent a worker pool.
    let probe_timeout = Duration::from_secs(args.timeout_secs);
    let namespace = args.namespace.clone();
    let mut handles = Vec::with_capacity(pod_objs.len());
    for pod in pod_objs {
        let ns = namespace.clone();
        let port = args.data_port;
        let want_metrics = args.metrics;
        handles.push(tokio::spawn(async move {
            probe_pod(ns, pod, port, want_metrics, probe_timeout).await
        }));
    }
    let mut rows: Vec<PodStatus> = Vec::with_capacity(handles.len());
    for h in handles {
        match h.await {
            Ok(row) => rows.push(row),
            Err(join) => rows.push(PodStatus {
                pod_name: "<panic>".into(),
                restart_count: 0,
                stats: None,
                metrics: None,
                error: Some(format!("probe task panicked: {join}")),
            }),
        }
    }
    rows.sort_by(|a, b| a.pod_name.cmp(&b.pod_name));

    match args.format.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&rows)?),
        "tsv" => print!("{}", render_tsv(&rows, args.metrics)),
        _ => print!("{}", render_table(&rows, args.metrics)),
    }
    Ok(())
}

async fn probe_pod(
    namespace: String,
    pod: Pod,
    port: u16,
    want_metrics: bool,
    probe_timeout: Duration,
) -> PodStatus {
    let pod_name = pod.metadata.name.clone().unwrap_or_default();
    let restart_count = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .map(|cs| cs.iter().map(|c| c.restart_count).sum::<i32>())
        .unwrap_or(0);

    match timeout(
        probe_timeout,
        fetch_one(&namespace, &pod_name, port, want_metrics),
    )
    .await
    {
        Ok(Ok((stats, metrics))) => PodStatus {
            pod_name,
            restart_count,
            stats: Some(stats),
            metrics,
            error: None,
        },
        Ok(Err(e)) => PodStatus {
            pod_name,
            restart_count,
            stats: None,
            metrics: None,
            error: Some(format!("{e:#}")),
        },
        Err(_) => PodStatus {
            pod_name,
            restart_count,
            stats: None,
            metrics: None,
            error: Some(format!("probe timed out after {:?}", probe_timeout)),
        },
    }
}

async fn fetch_one(
    namespace: &str,
    pod_name: &str,
    remote_port: u16,
    want_metrics: bool,
) -> Result<(ShelfStats, Option<PromAggregate>)> {
    let mut pf = PortForward::open(namespace, pod_name, remote_port).await?;
    let local_port = pf.local_port;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .context("building reqwest client")?;
    let stats: ShelfStats = http
        .get(format!("http://127.0.0.1:{local_port}/stats"))
        .send()
        .await
        .with_context(|| format!("GET /stats via port-forward to pod/{pod_name}"))?
        .error_for_status()
        .context("/stats returned non-2xx")?
        .json()
        .await
        .context("parsing /stats JSON")?;
    let metrics = if want_metrics {
        let body = http
            .get(format!("http://127.0.0.1:{local_port}/metrics"))
            .send()
            .await
            .with_context(|| format!("GET /metrics via port-forward to pod/{pod_name}"))?
            .error_for_status()
            .context("/metrics returned non-2xx")?
            .text()
            .await
            .context("reading /metrics body")?;
        Some(parse_prom_aggregate(&body))
    } else {
        None
    };
    pf.shutdown().await;
    Ok((stats, metrics))
}

/// Owns the lifecycle of one `kubectl port-forward` subprocess and
/// its OS-allocated local port. Drop closes the port (we also call
/// [`shutdown`] explicitly so port reuse is deterministic across
/// concurrent probes, instead of relying on Drop running soon).
///
/// `_drain` keeps a background task alive that reads kubectl's
/// stdout for the lifetime of the subprocess. Without it, the
/// stdout pipe fills up after a few "Handling connection for ..."
/// messages or, worse, the read end of the pipe closes on Drop and
/// kubectl exits with SIGPIPE on its next write — silently breaking
/// the next HTTP probe with `Connection reset by peer`.
struct PortForward {
    child: Child,
    local_port: u16,
    _drain: JoinHandle<()>,
}

impl PortForward {
    /// Spawn `kubectl port-forward pod/<name> -n <ns> :<remote_port>`.
    /// The leading colon is the kubectl idiom for "OS-allocated local
    /// port" — we read kubectl's stdout line by line until we hit the
    /// `Forwarding from 127.0.0.1:<N> -> ...` line and return that
    /// `<N>`. The IPv6 line (`Forwarding from [::1]:<N> -> ...`) is
    /// always emitted too and is harmless to skip.
    async fn open(namespace: &str, pod_name: &str, remote_port: u16) -> Result<Self> {
        let mut child = TokioCommand::new("kubectl")
            .args([
                "port-forward",
                "-n",
                namespace,
                &format!("pod/{pod_name}"),
                // Empty local port + remote -> kubectl asks the OS to pick.
                &format!(":{remote_port}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("spawn kubectl port-forward")?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("kubectl port-forward stdout closed before first read"))?;
        let mut reader = BufReader::new(stdout).lines();
        // 4s warmup cap — the typical local-cluster value is ≤ 1s;
        // allow some slack for a busy apiserver / a multi-hop VPN.
        let warmup = Duration::from_secs(4);
        let local_port = loop {
            let next = timeout(warmup, reader.next_line())
                .await
                .map_err(|_| anyhow!("kubectl port-forward stdout silent for >{:?}", warmup))?
                .context("reading kubectl port-forward stdout")?
                .ok_or_else(|| anyhow!("kubectl port-forward exited before reporting a port"))?;
            if let Some(p) = parse_kubectl_listen_line(&next) {
                break p;
            }
            // Skip IPv6 / unrelated chatter and keep reading; kubectl
            // emits an IPv4 line too within the same warmup window.
        };

        // Hand the lines reader off to a background task that
        // drains forever. This serves two purposes: it keeps the
        // pipe FD open (so kubectl doesn't SIGPIPE on its next
        // "Handling connection for NNNN" log line and tear down
        // the port-forward mid-probe) and it prevents the kernel
        // pipe buffer from filling up under sustained probing.
        let drain =
            tokio::spawn(async move { while let Ok(Some(_)) = reader.next_line().await {} });

        Ok(Self {
            child,
            local_port,
            _drain: drain,
        })
    }

    async fn shutdown(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        // The drain task lives behind `_` — when this struct drops
        // it'll be cancelled by the runtime.
    }
}

/// Parse a `kubectl port-forward` first-line "Forwarding from
/// 127.0.0.1:<port> -> <remote_port>" into the local port.
///
/// Pulled out so the unit suite can lock the line shapes kubectl
/// has shipped over its history without spinning up a real
/// subprocess.
fn parse_kubectl_listen_line(line: &str) -> Option<u16> {
    let prefix = "Forwarding from 127.0.0.1:";
    let rest = line.strip_prefix(prefix)?;
    let port_str = rest.split_whitespace().next()?;
    let port_str = port_str.split('-').next()?;
    port_str.parse::<u16>().ok()
}

/// Sum `shelf_hits_total` and `shelf_misses_total` across every
/// label combination. We don't need per-label aggregation here; the
/// table view is just a cluster-wide hit ratio per pod, and Foyer's
/// `hits_by_table` slice is too noisy for a one-line summary.
pub fn parse_prom_aggregate(body: &str) -> PromAggregate {
    let mut agg = PromAggregate::default();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (metric, value_str) = match line.rsplit_once(' ') {
            Some(parts) => parts,
            None => continue,
        };
        let value = match value_str.parse::<f64>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Strip the {label="..."} suffix if present so `metric_name`
        // is just the bare counter name.
        let metric_name = metric.split('{').next().unwrap_or(metric);
        let value = value as u64;
        match metric_name {
            "shelf_hits_total" => agg.hits = agg.hits.saturating_add(value),
            "shelf_misses_total" => agg.misses = agg.misses.saturating_add(value),
            _ => {}
        }
    }
    agg
}

fn fmt_bytes(b: u64) -> String {
    const TIB: u64 = 1 << 40;
    const GIB: u64 = 1 << 30;
    const MIB: u64 = 1 << 20;
    if b >= TIB {
        format!("{:.1}T", b as f64 / TIB as f64)
    } else if b >= GIB {
        format!("{:.1}G", b as f64 / GIB as f64)
    } else if b >= MIB {
        format!("{:.0}M", b as f64 / MIB as f64)
    } else {
        format!("{b}B")
    }
}

fn fmt_pool(p: Option<&ShelfPoolStats>) -> String {
    match p {
        None => "n/a".into(),
        Some(p) if p.capacity_bytes == 0 => format!("{}/0(?%)", fmt_bytes(p.used_bytes)),
        Some(p) => {
            let pct = 100.0 * (p.used_bytes as f64) / (p.capacity_bytes as f64);
            format!(
                "{}/{}({:.0}%)",
                fmt_bytes(p.used_bytes),
                fmt_bytes(p.capacity_bytes),
                pct
            )
        }
    }
}

fn fmt_used_cap(s: &ShelfStats) -> String {
    if s.capacity_bytes == 0 {
        format!("{}/0(?%)", fmt_bytes(s.used_bytes))
    } else {
        let pct = 100.0 * (s.used_bytes as f64) / (s.capacity_bytes as f64);
        format!(
            "{}/{}({:.0}%)",
            fmt_bytes(s.used_bytes),
            fmt_bytes(s.capacity_bytes),
            pct
        )
    }
}

fn fmt_hit_ratio(m: Option<&PromAggregate>) -> String {
    match m.and_then(|a| a.hit_ratio_pct()) {
        Some(p) => format!("{p:.0}%"),
        None => "n/a".into(),
    }
}

/// Render the human-friendly fixed-width table. The column order is
/// stable: pod | rss | used/cap | metadata | rowgroup | drain |
/// restarts ( + hit_ratio when --metrics ). Operators grep this in
/// noisy terminal output, so the column count must not jitter
/// between calls.
pub fn render_table(rows: &[PodStatus], with_metrics: bool) -> String {
    let mut out = String::new();
    if with_metrics {
        out.push_str(&format!(
            "{:<10} {:>6} {:>14} {:>14} {:>16} {:>5} {:>8} {:>9}\n",
            "POD", "RSS", "USED/CAP", "METADATA", "ROWGROUP", "DRAIN", "RESTART", "HIT_RATIO",
        ));
    } else {
        out.push_str(&format!(
            "{:<10} {:>6} {:>14} {:>14} {:>16} {:>5} {:>8}\n",
            "POD", "RSS", "USED/CAP", "METADATA", "ROWGROUP", "DRAIN", "RESTART",
        ));
    }
    for r in rows {
        match &r.stats {
            None => {
                let err = r.error.as_deref().unwrap_or("?");
                if with_metrics {
                    out.push_str(&format!(
                        "{:<10} {:>6} {:>14} {:>14} {:>16} {:>5} {:>8} {:>9}  ! {}\n",
                        r.pod_name, "?", "?", "?", "?", "?", r.restart_count, "?", err,
                    ));
                } else {
                    out.push_str(&format!(
                        "{:<10} {:>6} {:>14} {:>14} {:>16} {:>5} {:>8}  ! {}\n",
                        r.pod_name, "?", "?", "?", "?", "?", r.restart_count, err,
                    ));
                }
            }
            Some(s) => {
                let drain = if s.draining { "yes" } else { "no" };
                if with_metrics {
                    out.push_str(&format!(
                        "{:<10} {:>6} {:>14} {:>14} {:>16} {:>5} {:>8} {:>9}\n",
                        r.pod_name,
                        fmt_bytes(s.rss_bytes),
                        fmt_used_cap(s),
                        fmt_pool(Some(&s.metadata_pool)),
                        fmt_pool(s.rowgroup_pool.as_ref()),
                        drain,
                        r.restart_count,
                        fmt_hit_ratio(r.metrics.as_ref()),
                    ));
                } else {
                    out.push_str(&format!(
                        "{:<10} {:>6} {:>14} {:>14} {:>16} {:>5} {:>8}\n",
                        r.pod_name,
                        fmt_bytes(s.rss_bytes),
                        fmt_used_cap(s),
                        fmt_pool(Some(&s.metadata_pool)),
                        fmt_pool(s.rowgroup_pool.as_ref()),
                        drain,
                        r.restart_count,
                    ));
                }
            }
        }
    }
    out
}

/// Tab-separated render with a header line. Picked to be safe for
/// `cut -f` and `awk` — none of the per-cell strings contain tabs.
pub fn render_tsv(rows: &[PodStatus], with_metrics: bool) -> String {
    let mut out = String::new();
    let header = if with_metrics {
        "pod\trss_bytes\tcapacity_bytes\tused_bytes\tmetadata_used\tmetadata_capacity\trowgroup_used\trowgroup_capacity\tdraining\trestart_count\thits\tmisses\thit_ratio_pct\terror\n"
    } else {
        "pod\trss_bytes\tcapacity_bytes\tused_bytes\tmetadata_used\tmetadata_capacity\trowgroup_used\trowgroup_capacity\tdraining\trestart_count\terror\n"
    };
    out.push_str(header);
    for r in rows {
        let err = r.error.clone().unwrap_or_default();
        match &r.stats {
            None => {
                if with_metrics {
                    out.push_str(&format!(
                        "{}\t\t\t\t\t\t\t\t\t{}\t\t\t\t{}\n",
                        r.pod_name, r.restart_count, err
                    ));
                } else {
                    out.push_str(&format!(
                        "{}\t\t\t\t\t\t\t\t\t{}\t{}\n",
                        r.pod_name, r.restart_count, err
                    ));
                }
            }
            Some(s) => {
                let rg_used = s.rowgroup_pool.as_ref().map(|p| p.used_bytes).unwrap_or(0);
                let rg_cap = s
                    .rowgroup_pool
                    .as_ref()
                    .map(|p| p.capacity_bytes)
                    .unwrap_or(0);
                let drain = if s.draining { "true" } else { "false" };
                if with_metrics {
                    let m = r.metrics.clone().unwrap_or_default();
                    let hr = m
                        .hit_ratio_pct()
                        .map(|p| format!("{p:.2}"))
                        .unwrap_or_else(|| "".into());
                    out.push_str(&format!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        r.pod_name,
                        s.rss_bytes,
                        s.capacity_bytes,
                        s.used_bytes,
                        s.metadata_pool.used_bytes,
                        s.metadata_pool.capacity_bytes,
                        rg_used,
                        rg_cap,
                        drain,
                        r.restart_count,
                        m.hits,
                        m.misses,
                        hr,
                        err,
                    ));
                } else {
                    out.push_str(&format!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        r.pod_name,
                        s.rss_bytes,
                        s.capacity_bytes,
                        s.used_bytes,
                        s.metadata_pool.used_bytes,
                        s.metadata_pool.capacity_bytes,
                        rg_used,
                        rg_cap,
                        drain,
                        r.restart_count,
                        err,
                    ));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Locks the wire shape against the known-good `shelfd` v1.0
    /// `/stats` payload. If this test fails on a future bump, either
    /// the daemon broke its wire contract or this struct drifted —
    /// either way we want CI to surface it before an operator does.
    #[test]
    fn parse_shelfstats_v1_0_format() {
        let json = r#"{
          "pod_id": "shelf-2",
          "capacity_bytes": 257698037760,
          "used_bytes": 200000000000,
          "metadata_pool": {
            "capacity_bytes": 8589934592,
            "used_bytes": 5368709120,
            "disk_used_bytes": 0,
            "disk_capacity_bytes": 0
          },
          "rowgroup_pool": {
            "capacity_bytes": 257698037760,
            "used_bytes": 200000000000,
            "disk_used_bytes": 200000000000,
            "disk_capacity_bytes": 257698037760
          },
          "pinned_bytes": 1234,
          "pinned_count": 7,
          "draining": false,
          "rss_bytes": 18253611008
        }"#;
        let s: ShelfStats = serde_json::from_str(json).unwrap();
        assert_eq!(s.pod_id, "shelf-2");
        assert_eq!(s.capacity_bytes, 257_698_037_760);
        assert_eq!(s.metadata_pool.capacity_bytes, 8_589_934_592);
        let rg = s.rowgroup_pool.expect("rowgroup_pool present in v1.0");
        assert_eq!(rg.disk_used_bytes, 200_000_000_000);
        assert_eq!(s.pinned_bytes, 1234);
        assert_eq!(s.pinned_count, 7);
        assert!(!s.draining);
    }

    /// Older daemons (pre-SHELF-18) may have shipped without the
    /// rowgroup pool entirely. Make sure we still parse them so a
    /// mixed-version probe doesn't fail the whole table.
    #[test]
    fn parse_shelfstats_missing_rowgroup_pool_handles_gracefully() {
        let json = r#"{
          "pod_id": "shelf-old",
          "capacity_bytes": 8589934592,
          "used_bytes": 1024,
          "metadata_pool": {
            "capacity_bytes": 8589934592,
            "used_bytes": 1024
          }
        }"#;
        let s: ShelfStats = serde_json::from_str(json).expect("must parse without rowgroup_pool");
        assert!(s.rowgroup_pool.is_none(), "rowgroup_pool should be None");
        assert_eq!(s.metadata_pool.disk_used_bytes, 0);
        assert_eq!(s.pinned_bytes, 0);
        assert!(!s.draining);
    }

    fn fixture_three_pods() -> Vec<PodStatus> {
        let mk = |name: &str,
                  used: u64,
                  cap: u64,
                  meta_used: u64,
                  rg_used: u64,
                  draining: bool,
                  restarts: i32,
                  rss: u64| PodStatus {
            pod_name: name.into(),
            restart_count: restarts,
            stats: Some(ShelfStats {
                pod_id: name.into(),
                capacity_bytes: cap,
                used_bytes: used,
                metadata_pool: ShelfPoolStats {
                    capacity_bytes: 8 * (1 << 30),
                    used_bytes: meta_used,
                    disk_used_bytes: 0,
                    disk_capacity_bytes: 0,
                },
                rowgroup_pool: Some(ShelfPoolStats {
                    capacity_bytes: 240 * (1 << 30),
                    used_bytes: rg_used,
                    disk_used_bytes: rg_used,
                    disk_capacity_bytes: 240 * (1 << 30),
                }),
                pinned_bytes: 0,
                pinned_count: 0,
                draining,
                rss_bytes: rss,
            }),
            metrics: None,
            error: None,
        };
        vec![
            mk(
                "shelf-0",
                257_698_037_760,
                257_698_037_760,
                5 * (1 << 30),
                234 * (1 << 30),
                false,
                0,
                17 * (1u64 << 30),
            ),
            mk(
                "shelf-1",
                257_698_037_760,
                257_698_037_760,
                6 * (1 << 30),
                234 * (1 << 30),
                false,
                0,
                18 * (1u64 << 30),
            ),
            mk(
                "shelf-2",
                102_000_000_000,
                257_698_037_760,
                2 * (1 << 30),
                93 * (1 << 30),
                false,
                1,
                16 * (1u64 << 30),
            ),
        ]
    }

    /// Lock the table layout — column order, header row,
    /// per-pod-row width. Operators grep this in noisy terminal
    /// output so column drift between releases is a real-world
    /// issue, not a stylistic one.
    #[test]
    fn aggregate_table_output_format_correct() {
        let out = render_table(&fixture_three_pods(), false);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 4, "header + 3 rows; got: {out}");
        assert!(
            lines[0].starts_with("POD"),
            "header line should start with POD: {:?}",
            lines[0]
        );
        for (h, row) in [
            ("RSS", lines[0]),
            ("USED/CAP", lines[0]),
            ("METADATA", lines[0]),
            ("ROWGROUP", lines[0]),
            ("DRAIN", lines[0]),
            ("RESTART", lines[0]),
        ] {
            assert!(row.contains(h), "header missing column {h}: {row:?}");
        }
        for (i, name) in ["shelf-0", "shelf-1", "shelf-2"].iter().enumerate() {
            assert!(
                lines[i + 1].starts_with(name),
                "row {i} should start with {name}: {:?}",
                lines[i + 1]
            );
        }
        assert!(
            lines[1].contains("17.0G"),
            "row 0 RSS column should render 17.0G: {:?}",
            lines[1]
        );
    }

    /// JSON output is a serialisation contract. Lock it: array of
    /// `PodStatus`, exact field names. Anything that breaks this
    /// breaks downstream `jq` pipelines operators have set up.
    #[test]
    fn aggregate_json_output_format_correct() {
        let rows = fixture_three_pods();
        let out = serde_json::to_string(&rows).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(parsed.is_array(), "top-level must be array: {parsed:?}");
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        let r0 = &arr[0];
        assert_eq!(r0["pod_name"].as_str(), Some("shelf-0"));
        assert!(
            r0["stats"]["metadata_pool"]["capacity_bytes"].is_u64(),
            "stats.metadata_pool.capacity_bytes should be a u64"
        );
        assert_eq!(r0["restart_count"].as_i64(), Some(0));
    }

    /// TSV output is a contract too — `cut -f` users will pin
    /// column N. Header stays stable.
    #[test]
    fn aggregate_tsv_output_format_correct() {
        let out = render_tsv(&fixture_three_pods(), false);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 4, "header + 3 rows; got: {out}");
        let header_cols: Vec<&str> = lines[0].split('\t').collect();
        assert_eq!(
            header_cols,
            vec![
                "pod",
                "rss_bytes",
                "capacity_bytes",
                "used_bytes",
                "metadata_used",
                "metadata_capacity",
                "rowgroup_used",
                "rowgroup_capacity",
                "draining",
                "restart_count",
                "error",
            ],
            "TSV header column order is a contract; if you must change it, bump shelfctl major"
        );
        for (i, name) in ["shelf-0", "shelf-1", "shelf-2"].iter().enumerate() {
            let cols: Vec<&str> = lines[i + 1].split('\t').collect();
            assert_eq!(cols.len(), header_cols.len(), "row {i} column count");
            assert_eq!(cols[0], *name);
        }
    }

    #[test]
    fn parse_kubectl_listen_line_typical() {
        // Stable across kubectl 1.20-1.30. The "->" arrow surrounds
        // the remote port and is what we strip on for the local one.
        assert_eq!(
            parse_kubectl_listen_line("Forwarding from 127.0.0.1:54321 -> 9090"),
            Some(54321)
        );
        // IPv6 line (also emitted) — we deliberately bind only on
        // 127.0.0.1, so this returns None and the next line in stdout
        // is read. The caller's `next_line` loop handles that.
        assert_eq!(
            parse_kubectl_listen_line("Forwarding from [::1]:54321 -> 9090"),
            None
        );
        // Empty / unrelated lines → None.
        assert_eq!(parse_kubectl_listen_line(""), None);
        assert_eq!(parse_kubectl_listen_line("Handling connection..."), None);
    }

    #[test]
    fn parse_prom_aggregate_extracts_hits_and_misses() {
        let body = r#"
# HELP shelf_hits_total cumulative hits
# TYPE shelf_hits_total counter
shelf_hits_total{pool="metadata"} 100
shelf_hits_total{pool="rowgroup"} 400
shelf_misses_total{pool="metadata"} 50
shelf_misses_total{pool="rowgroup"} 50
shelf_some_other_metric 999
"#;
        let agg = parse_prom_aggregate(body);
        assert_eq!(agg.hits, 500);
        assert_eq!(agg.misses, 100);
        assert!(
            (agg.hit_ratio_pct().unwrap() - (100.0 * 500.0 / 600.0)).abs() < 1e-6,
            "hit ratio should be 500/600"
        );
    }

    #[test]
    fn fmt_bytes_units() {
        assert_eq!(fmt_bytes(0), "0B");
        assert_eq!(fmt_bytes(1023), "1023B");
        assert_eq!(fmt_bytes(2 * (1 << 20)), "2M");
        assert_eq!(fmt_bytes(17 * (1u64 << 30)), "17.0G");
        assert_eq!(fmt_bytes(2 * (1u64 << 40)), "2.0T");
    }
}
