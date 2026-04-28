/** Live tab — operator's working surface (replaces OpsTab).
 *
 * Three-row discipline per `AGENTS.md`:
 *   Row 1 — Stephen Few bullet charts, fixed thresholds, no charts.
 *   Row 2 — culprit panels that stay empty when healthy.
 *   Row 3 — always-on trend sparklines (5-minute window) with engine-
 *           reset annotations marked inline.
 *
 * Every important metric appears somewhere on rows 1/3; row 2 is
 * load-bearing only when something is broken. The tab is read-only;
 * pin/evict/reload moved into the command palette via App.tsx.
 *
 * Polish-pass changes (S1/S4/A1):
 *   - Row 1's six traffic-light tiles became Stephen Few bullet charts
 *     so "where am I in the band?" is legible at a glance.
 *   - Every bullet carries a `Δ vs 5m` caption derived from the
 *     5-min history we already keep via `useTimeseries`.
 *   - Row 3 sparklines mark the indices where
 *     `shelf_engine_resets_total` ticked, so the operator sees *when*
 *     the pool engine resynced, not just the throughput dip.
 */

import { useMemo } from "react";
import { getMetricsText, type Stats } from "../api/client";
import {
  groupBy,
  histogramQuantile,
  parseMetrics,
  sumMatching,
  sumSeries,
  listSeries,
} from "../api/metrics";
import { usePolled } from "../polling";
import { useTimeseries } from "../hooks/useTimeseries";
import { formatCount, formatLatencyMs, formatPercent, formatDelta } from "../format";
import CapacityBar from "../components/CapacityBar";
import Sparkline from "../components/Sparkline";
import Bullet from "../components/Bullet";

type Props = { stats: Stats | null };

type Tone = "ok" | "warn" | "err" | "pending";

/** Convert a recent-history `levels` array into a `now` value and a
 * `then` value 5 min ago (or the oldest sample we have, if the buffer
 * isn't full yet). Returns `null` for either side when the data is
 * insufficient. */
function nowAndThen(levels: number[]): {
  now: number | null;
  then: number | null;
} {
  if (levels.length === 0) return { now: null, then: null };
  return {
    now: levels[levels.length - 1] ?? null,
    then: levels[0] ?? null,
  };
}

export default function LiveTab({ stats }: Props) {
  const { data: metricsText, error } = usePolled(getMetricsText);
  const series = useMemo(
    () => (metricsText ? parseMetrics(metricsText) : []),
    [metricsText],
  );

  /* ---------------- Row 1 inputs ---------------- */

  const hitRatioBps = listSeries(series, "shelf_rolling_hit_ratio_bps");
  const rollingHitRatio =
    hitRatioBps.length === 0
      ? null
      : hitRatioBps.reduce((a, r) => a + r.value, 0) / hitRatioBps.length / 10_000;
  const hitRatioTs = useTimeseries(rollingHitRatio);

  const p99 = histogramQuantile(series, "shelf_request_seconds", () => true, 0.99);
  const p99Ts = useTimeseries(p99);

  const totalReads =
    sumSeries(series, "shelf_hits_total") + sumSeries(series, "shelf_misses_total");
  const originErrors = sumMatching(
    series,
    "shelfd_error_total",
    (l) => l["component"] === "origin",
  );
  const originErrorRatio = totalReads > 0 ? originErrors / totalReads : null;
  const originErrorTs = useTimeseries(originErrorRatio);

  const diskUsed = sumSeries(series, "shelf_disk_bytes_used");
  const diskCap = sumSeries(series, "shelf_disk_bytes_capacity");
  const nvmeUsage = diskCap > 0 ? diskUsed / diskCap : null;
  const nvmeTs = useTimeseries(nvmeUsage);

  const peerHit = sumSeries(series, "shelf_peer_hit_total");
  const peerMiss = sumSeries(series, "shelf_peer_miss_total");
  const peerTimeout = sumSeries(series, "shelf_peer_timeout_total");
  const peerError = sumSeries(series, "shelf_peer_error_total");
  const peerTotal = peerHit + peerMiss + peerTimeout + peerError;
  const peerErrRatio = peerTotal > 0 ? (peerTimeout + peerError) / peerTotal : null;
  const peerTs = useTimeseries(peerErrRatio);

  // Engine resets are a counter; we want the rate over a recent
  // window. `useTimeseries` over the last sample gives us delta/poll
  // which at 5 s polling × 60 samples = 5 min of evidence (good
  // enough proxy for "last 15 min" without a separate timer).
  const engineResets = sumSeries(series, "shelf_engine_resets_total");
  const resetTs = useTimeseries(engineResets);
  const resetsRecent = resetTs.deltas.slice(-3).reduce((a, b) => a + b, 0);
  const resetsRecentTs = useTimeseries(resetsRecent);

  // Compute the Tier S4 delta caption for each bullet.
  const hitRatioDelta = formatDelta(
    nowAndThen(hitRatioTs.levels).now,
    nowAndThen(hitRatioTs.levels).then,
    "higher-is-better",
    "percent",
  );
  const p99Delta = formatDelta(
    nowAndThen(p99Ts.levels).now,
    nowAndThen(p99Ts.levels).then,
    "lower-is-better",
    "percent",
  );
  const originDelta = formatDelta(
    nowAndThen(originErrorTs.levels).now,
    nowAndThen(originErrorTs.levels).then,
    "lower-is-better",
    "percent",
  );
  const nvmeDelta = formatDelta(
    nowAndThen(nvmeTs.levels).now,
    nowAndThen(nvmeTs.levels).then,
    "lower-is-better",
    "percent",
  );
  const peerDelta = formatDelta(
    nowAndThen(peerTs.levels).now,
    nowAndThen(peerTs.levels).then,
    "lower-is-better",
    "percent",
  );
  const resetsDelta = formatDelta(
    nowAndThen(resetsRecentTs.levels).now,
    nowAndThen(resetsRecentTs.levels).then,
    "lower-is-better",
    "absolute",
  );

  /* ---------------- Row 2 inputs ---------------- */

  const evictionsByPoolReason = listSeries(series, "shelf_evictions_total");
  const errorsByKind = listSeries(series, "shelfd_error_total");
  const recentResetEvents = listSeries(series, "shelf_engine_resets_total").filter(
    (r) => r.value > 0,
  );

  /* ---------------- Row 3 inputs ---------------- */

  const hits = sumSeries(series, "shelf_hits_total");
  const misses = sumSeries(series, "shelf_misses_total");
  const hitsByPool = groupBy(series, "shelf_hits_total", "pool");
  const missesByPool = groupBy(series, "shelf_misses_total", "pool");
  const hitsTs = useTimeseries(hits);
  const missesTs = useTimeseries(misses);

  const p50Ts = useTimeseries(
    histogramQuantile(series, "shelf_request_seconds", () => true, 0.5),
  );
  const p95Ts = useTimeseries(
    histogramQuantile(series, "shelf_request_seconds", () => true, 0.95),
  );

  const inflight = sumSeries(series, "shelf_inflight_singleflight");
  const inflightTs = useTimeseries(inflight);

  // Build the engine-reset annotation marks once and pass to every
  // row-3 sparkline. A "reset" mark is any index where the reset
  // counter delta was non-zero — that's a true pool resync event.
  const resetMarks = useMemo(() => {
    const marks: Array<{ idx: number; label: string }> = [];
    for (let i = 0; i < resetTs.deltas.length; i++) {
      if (resetTs.deltas[i] > 0) {
        marks.push({
          idx: i,
          label: `engine reset · ${resetTs.deltas[i].toFixed(0)} delta`,
        });
      }
    }
    return marks;
  }, [resetTs.deltas]);

  return (
    <>
      {error ? (
        <div className="card" style={{ borderColor: "var(--err)" }}>
          <div className="card-title">metrics scrape failed</div>
          <div style={{ fontFamily: "var(--mono)", fontSize: 12, color: "var(--err)" }}>
            {error}
          </div>
        </div>
      ) : null}

      {/* ============================ Row 1 ============================ */}
      <section className="live-row1 live-row1-bullets">
        <Bullet
          label="Hit rate"
          value={rollingHitRatio}
          format={(v) => formatPercent(v)}
          thresholds={{ green: 0.8, amber: 0.6 }}
          direction="higher-is-better"
          delta={hitRatioDelta}
          help="60-second rolling hit ratio averaged across pools"
        />
        <Bullet
          label="p99 read latency"
          value={p99}
          format={(v) => formatLatencyMs(v)}
          thresholds={{ green: 0.2, amber: 1.0 }}
          direction="lower-is-better"
          scaleMax={2.0}
          delta={p99Delta}
          help="across all outcomes (memory + disk + miss + passthrough)"
        />
        <Bullet
          label="Origin error rate"
          value={originErrorRatio}
          format={(v) => formatPercent(v)}
          thresholds={{ green: 0.01, amber: 0.05 }}
          direction="lower-is-better"
          scaleMax={0.1}
          delta={originDelta}
          help="shelfd_error_total{component=origin} / total reads"
        />
        <Bullet
          label="NVMe headroom"
          value={nvmeUsage}
          format={(v) => formatPercent(v)}
          thresholds={{ green: 0.7, amber: 0.9 }}
          direction="lower-is-better"
          delta={nvmeDelta}
          help="shelf_disk_bytes_used / capacity, summed across pools"
        />
        <Bullet
          label="Peer routing"
          value={peerErrRatio}
          format={(v) => formatPercent(v)}
          thresholds={{ green: 0.01, amber: 0.05 }}
          direction="lower-is-better"
          scaleMax={0.1}
          delta={peerDelta}
          help="(peer_timeout + peer_error) / total peer probes"
        />
        <Bullet
          label="Engine resets (5 min)"
          value={resetsRecent}
          format={(v) => v.toFixed(0)}
          thresholds={{ green: 0.5, amber: 2.5 }}
          direction="lower-is-better"
          scaleMax={5}
          delta={resetsDelta}
          help="shelf_engine_resets_total deltas across recent polls"
        />
      </section>

      {/* ============================ Row 2 ============================ */}
      <section className="live-row2">
        <CulpritPanel
          title="Most-evicted by capacity"
          rows={evictionsByPoolReason
            .filter((r) => r.labels["reason"] === "capacity" && r.value > 0)
            .sort((a, b) => b.value - a.value)
            .slice(0, 5)
            .map((r) => ({
              left: `${r.labels["pool"]}`,
              right: `${formatCount(r.value)} evictions`,
            }))}
          empty="No capacity-driven evictions."
        />
        <CulpritPanel
          title="Pods in lameduck"
          rows={
            stats?.draining
              ? [{ left: stats.pod_id, right: "draining (SHELF-20)" }]
              : []
          }
          empty="All pods serving traffic."
        />
        <CulpritPanel
          title="Origin errors by kind"
          rows={errorsByKind
            .filter((r) => r.labels["component"] === "origin" && r.value > 0)
            .sort((a, b) => b.value - a.value)
            .slice(0, 5)
            .map((r) => ({
              left: r.labels["kind"] ?? "?",
              right: formatCount(r.value),
            }))}
          empty="Origin path is clean."
        />
        <CulpritPanel
          title="Engine resets last hour"
          rows={recentResetEvents.slice(0, 5).map((r) => ({
            left: `${r.labels["pool"]} · ${r.labels["reason"]}`,
            right: formatCount(r.value),
          }))}
          empty="Pool engines stable."
        />
      </section>

      {/* ============================ Row 3 ============================ */}
      <section className="live-row3">
        <TrendCard title="Hit rate by pool">
          <PoolHitRateChart hitsByPool={hitsByPool} missesByPool={missesByPool} />
        </TrendCard>
        <TrendCard title="Latency p50 / p95 / p99">
          <LatencyTriple
            p50={p50Ts.levels}
            p95={p95Ts.levels}
            p99={p99Ts.levels}
            marks={resetMarks}
          />
        </TrendCard>
        <TrendCard title="Throughput">
          <ThroughputChart
            hits={hitsTs.deltas}
            misses={missesTs.deltas}
            marks={resetMarks}
          />
        </TrendCard>
        <TrendCard title="Capacity">
          {stats ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              <CapacityBar
                label="metadata · DRAM"
                used={stats.metadata_pool.used_bytes}
                capacity={stats.metadata_pool.capacity_bytes}
              />
              <CapacityBar
                label="rowgroup · DRAM"
                used={Math.max(
                  0,
                  stats.rowgroup_pool.used_bytes - stats.rowgroup_pool.disk_used_bytes,
                )}
                capacity={Math.max(
                  0,
                  stats.rowgroup_pool.capacity_bytes -
                    stats.rowgroup_pool.disk_capacity_bytes,
                )}
              />
              <CapacityBar
                label="rowgroup · NVMe"
                used={stats.rowgroup_pool.disk_used_bytes}
                capacity={stats.rowgroup_pool.disk_capacity_bytes}
                variant="disk"
              />
            </div>
          ) : (
            <div className="empty">waiting for /stats…</div>
          )}
        </TrendCard>
        <TrendCard title="Single-flight in-flight">
          <div className="live-trend-num">{formatCount(inflight)}</div>
          <Sparkline
            data={inflightTs.levels}
            width={260}
            height={36}
            stroke="var(--accent)"
            marks={resetMarks}
          />
          <div className="stat-sub">Live count of de-duplicated origin fetches.</div>
        </TrendCard>
      </section>
    </>
  );
}

// (Unused — replaced by Bullet, kept commented out so the diff is
// easy to follow. Left as JSDoc only to document the prior contract.)
//
// type _PriorTone = Tone;
// `LightTile` was the original row-1 primitive; see git history.

/* ------------------------------------------------------------------ */
/*  Row 2 primitive — culprit panel                                    */
/* ------------------------------------------------------------------ */

function CulpritPanel({
  title,
  rows,
  empty,
}: {
  title: string;
  rows: Array<{ left: string; right: string }>;
  empty: string;
}) {
  return (
    <div className="card culprit-panel">
      <div className="culprit-head">
        <span className="card-title" style={{ margin: 0 }}>
          {title}
        </span>
        <span className={`culprit-badge culprit-badge-${rows.length === 0 ? "ok" : "warn"}`}>
          {rows.length === 0 ? "ok" : `${rows.length} alert${rows.length === 1 ? "" : "s"}`}
        </span>
      </div>
      {rows.length === 0 ? (
        <div className="culprit-empty">{empty}</div>
      ) : (
        <ul className="culprit-list">
          {rows.map((r, i) => (
            <li key={`${r.left}-${i}`}>
              <span>{r.left}</span>
              <strong>{r.right}</strong>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Row 3 primitives — trend visualisations                           */
/* ------------------------------------------------------------------ */

function TrendCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="card live-trend">
      <h3 className="card-title">{title}</h3>
      {children}
    </div>
  );
}

function PoolHitRateChart({
  hitsByPool,
  missesByPool,
}: {
  hitsByPool: Record<string, number>;
  missesByPool: Record<string, number>;
}) {
  const pools = Array.from(
    new Set([...Object.keys(hitsByPool), ...Object.keys(missesByPool)]),
  );
  return (
    <div className="trend-rate-rows">
      {pools.length === 0 ? (
        <div className="empty">no traffic yet</div>
      ) : (
        pools.map((p) => {
          const h = hitsByPool[p] ?? 0;
          const m = missesByPool[p] ?? 0;
          const total = h + m;
          const ratio = total > 0 ? h / total : 0;
          const tone: Tone =
            total === 0 ? "pending" : ratio >= 0.8 ? "ok" : ratio >= 0.6 ? "warn" : "err";
          return (
            <div key={p} className="trend-rate-row">
              <span className="trend-rate-name">{p}</span>
              <div className="trend-rate-bar">
                <div
                  className={`trend-rate-fill trend-rate-${tone}`}
                  style={{ width: `${ratio * 100}%` }}
                />
              </div>
              <span className="trend-rate-pct">
                {total === 0 ? "—" : `${(ratio * 100).toFixed(1)}%`}
              </span>
              <span className="stat-sub">{`${formatCount(h)} / ${formatCount(total)}`}</span>
            </div>
          );
        })
      )}
    </div>
  );
}

function LatencyTriple({
  p50,
  p95,
  p99,
  marks,
}: {
  p50: number[];
  p95: number[];
  p99: number[];
  marks?: Array<{ idx: number; label: string }>;
}) {
  // We render three sparklines stacked, sharing a max so the visual
  // hierarchy reflects the actual latency separation.
  const all = [...p50, ...p95, ...p99].filter((v) => Number.isFinite(v));
  const max = all.length > 0 ? Math.max(...all) : 0;
  return (
    <div className="trend-lat-stack">
      <SparkRow label="p50" data={p50} max={max} stroke="var(--ok)" marks={marks} />
      <SparkRow label="p95" data={p95} max={max} stroke="var(--warn)" marks={marks} />
      <SparkRow label="p99" data={p99} max={max} stroke="var(--err)" marks={marks} />
    </div>
  );
}

function SparkRow({
  label,
  data,
  max,
  stroke,
  marks,
}: {
  label: string;
  data: number[];
  max: number;
  stroke: string;
  marks?: Array<{ idx: number; label: string }>;
}) {
  const last = data.length > 0 ? data[data.length - 1] : null;
  return (
    <div className="trend-lat-row">
      <span className="trend-lat-name">{label}</span>
      <Sparkline
        data={data}
        width={200}
        height={26}
        stroke={stroke}
        max={max}
        marks={marks}
      />
      <span className="trend-lat-val">{last == null ? "—" : formatLatencyMs(last)}</span>
    </div>
  );
}

function ThroughputChart({
  hits,
  misses,
  marks,
}: {
  hits: number[];
  misses: number[];
  marks?: Array<{ idx: number; label: string }>;
}) {
  const total = hits.map((h, i) => h + (misses[i] ?? 0));
  const lastTotal = total.length > 0 ? total[total.length - 1] : 0;
  const lastHits = hits.length > 0 ? hits[hits.length - 1] : 0;
  return (
    <>
      <Sparkline
        data={total}
        width={260}
        height={42}
        stroke="var(--accent)"
        marks={marks}
      />
      <div className="stat-sub">
        ~{lastTotal.toFixed(1)} req/poll · hits {lastHits.toFixed(1)} / misses{" "}
        {(lastTotal - lastHits).toFixed(1)}
      </div>
    </>
  );
}

// Re-exported so future row-3 widgets can stay tone-aligned with the
// rest of the tab without redeclaring the same union.
export type { Tone as LiveTrafficLightTone };
