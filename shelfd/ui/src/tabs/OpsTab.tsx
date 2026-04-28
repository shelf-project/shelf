import { useMemo } from "react";
import StatCard from "../components/StatCard";
import CapacityBar from "../components/CapacityBar";
import TrafficLight from "../components/TrafficLight";
import AnimatedNumber from "../components/AnimatedNumber";
import { getMetricsText, type Stats } from "../api/client";
import {
  groupBy,
  histogramQuantile,
  parseMetrics,
  sumMatching,
  sumSeries,
} from "../api/metrics";
import { formatBytes, formatCount, formatLatencyMs, formatPercent } from "../format";
import { usePolled } from "../polling";
import { useTimeseries } from "../hooks/useTimeseries";

type Props = {
  stats: Stats | null;
};

export default function OpsTab({ stats }: Props) {
  const { data: metricsText, error } = usePolled(getMetricsText);
  const series = useMemo(
    () => (metricsText ? parseMetrics(metricsText) : []),
    [metricsText],
  );

  const hits = sumSeries(series, "shelf_hits_total");
  const misses = sumSeries(series, "shelf_misses_total");
  const total = hits + misses;
  const hitRate = total > 0 ? hits / total : null;

  // Per-pool breakdown — lets operators tell whether the footer pool
  // or the row-group pool is missing. They often diverge wildly.
  const hitsByPool = groupBy(series, "shelf_hits_total", "pool");
  const missesByPool = groupBy(series, "shelf_misses_total", "pool");
  const poolRate = (p: string) => {
    const h = hitsByPool[p] ?? 0;
    const m = missesByPool[p] ?? 0;
    const t = h + m;
    return { hits: h, total: t, rate: t > 0 ? h / t : null };
  };
  const metaRate = poolRate("metadata");
  const rgRate = poolRate("rowgroup");

  const originFallbacks = sumMatching(
    series,
    "shelfd_error_total",
    (l) => l["component"] === "origin",
  );
  const fallbackRate = total > 0 ? originFallbacks / total : null;

  const p95 = histogramQuantile(
    series,
    "shelf_request_seconds",
    (l) => l["path"] === "/cache",
    0.95,
  );
  const p50 = histogramQuantile(
    series,
    "shelf_request_seconds",
    (l) => l["path"] === "/cache",
    0.5,
  );

  // Rolling histories drive the sparklines on each stat card.
  const hitsSeries = useTimeseries(hits);
  const missesSeries = useTimeseries(misses);
  const p95Series = useTimeseries(p95 ?? null);
  const fallbackSeries = useTimeseries(originFallbacks);
  const pinnedSeries = useTimeseries(stats?.pinned_bytes ?? null);
  const metaUsedSeries = useTimeseries(stats?.metadata_pool.used_bytes ?? null);
  const rgDramUsedSeries = useTimeseries(
    stats ? Math.max(0, stats.rowgroup_pool.used_bytes - stats.rowgroup_pool.disk_used_bytes) : null,
  );
  const rgDiskUsedSeries = useTimeseries(stats?.rowgroup_pool.disk_used_bytes ?? null);

  const hitsPerSec = hitsSeries.rate;
  const missesPerSec = missesSeries.rate;
  const requestsPerSec =
    hitsPerSec === null || missesPerSec === null ? null : hitsPerSec + missesPerSec;

  const hitRateTone = hitRate === null ? null : hitRate >= 0.95 ? "ok" : hitRate >= 0.85 ? "warn" : "err";
  const fallbackTone =
    fallbackRate === null ? null : fallbackRate <= 0.01 ? "ok" : fallbackRate <= 0.05 ? "warn" : "err";

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

      <section className="stat-grid">
        <StatCard
          label="Hit rate (cumulative)"
          value={hitRate == null ? "—" : formatPercent(hitRate)}
          sub={
            total === 0
              ? "no traffic yet"
              : `${formatCount(hits)} hits / ${formatCount(total)} reads`
          }
          tone={hitRateTone}
          history={hitsSeries.deltas}
          stroke={hitRateTone === "err" ? "var(--err)" : hitRateTone === "warn" ? "var(--warn)" : "var(--ok)"}
        />
        <StatCard
          label="Throughput"
          value={
            requestsPerSec == null ? "—" : (
              <AnimatedNumber
                value={requestsPerSec}
                format={(n) => `${n.toFixed(n >= 10 ? 0 : 1)}`}
                threshold={0.1}
              />
            )
          }
          unit="req/s"
          sub={
            hitsPerSec == null || missesPerSec == null
              ? "warming up…"
              : `${hitsPerSec.toFixed(1)} hit/s · ${missesPerSec.toFixed(1)} miss/s`
          }
          history={hitsSeries.deltas.map((d, i) => d + (missesSeries.deltas[i] ?? 0))}
        />
        <StatCard
          label="p95 /cache latency"
          value={p95 == null ? "—" : formatLatencyMs(p95)}
          sub={
            p50 != null
              ? `p50 ${formatLatencyMs(p50)} · from shelf_request_seconds`
              : "from shelf_request_seconds"
          }
          history={p95Series.levels}
          stroke="#f2a65a"
        />
        <StatCard
          label="Origin fallback"
          value={fallbackRate == null ? "—" : formatPercent(fallbackRate)}
          sub={`${formatCount(originFallbacks)} errors (origin)`}
          tone={fallbackTone}
          history={fallbackSeries.deltas}
          stroke={fallbackTone === "err" ? "var(--err)" : fallbackTone === "warn" ? "var(--warn)" : "var(--ok)"}
        />
      </section>

      <section className="card">
        <h3 className="card-title">Hit rate by pool</h3>
        <div className="pool-split">
          <PoolCard
            name="metadata"
            rate={metaRate.rate}
            hits={metaRate.hits}
            total={metaRate.total}
            history={hitsSeries.deltas /* same cadence; fine for visual */}
            colour="#4fa3ff"
          />
          <PoolCard
            name="rowgroup"
            rate={rgRate.rate}
            hits={rgRate.hits}
            total={rgRate.total}
            history={hitsSeries.deltas}
            colour="#a78bfa"
          />
        </div>
      </section>

      <section className="card">
        <h3 className="card-title">SLO</h3>
        <TrafficLight ratio={hitRate} label="Cumulative hit rate" />
      </section>

      <section className="card">
        <h3 className="card-title">Capacity</h3>
        {!stats ? (
          <div className="empty">waiting for /stats…</div>
        ) : (
          <div className="capacity-row">
            <div className="card" style={{ padding: 12, background: "var(--bg-elev-2)" }}>
              <div className="cap-head" style={{ marginBottom: 6 }}>
                <strong>metadata</strong>
                <span>DRAM only (ADR-0008)</span>
              </div>
              <CapacityBar
                label="DRAM"
                used={stats.metadata_pool.used_bytes}
                capacity={stats.metadata_pool.capacity_bytes}
                history={metaUsedSeries.levels}
              />
              <div className="stat-sub" style={{ marginTop: 6 }}>
                pinned: {formatBytes(stats.pinned_bytes)} · {formatCount(stats.pinned_count)} keys
              </div>
            </div>
            <div className="card" style={{ padding: 12, background: "var(--bg-elev-2)" }}>
              <div className="cap-head" style={{ marginBottom: 6 }}>
                <strong>rowgroup</strong>
                <span>hybrid DRAM + NVMe</span>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                <CapacityBar
                  label="DRAM"
                  used={Math.max(0, stats.rowgroup_pool.used_bytes - stats.rowgroup_pool.disk_used_bytes)}
                  capacity={Math.max(
                    0,
                    stats.rowgroup_pool.capacity_bytes - stats.rowgroup_pool.disk_capacity_bytes,
                  )}
                  history={rgDramUsedSeries.levels}
                />
                <CapacityBar
                  label="NVMe"
                  used={stats.rowgroup_pool.disk_used_bytes}
                  capacity={stats.rowgroup_pool.disk_capacity_bytes}
                  variant="disk"
                  history={rgDiskUsedSeries.levels}
                />
              </div>
            </div>
          </div>
        )}
      </section>

      {/* Hidden but useful: keeps the pinned series alive so future
        * panels can pick it up without re-subscribing. */}
      <span style={{ display: "none" }} data-pinned-len={pinnedSeries.levels.length} />
    </>
  );
}

function PoolCard({
  name,
  rate,
  hits,
  total,
  history,
  colour,
}: {
  name: string;
  rate: number | null;
  hits: number;
  total: number;
  history: number[];
  colour: string;
}) {
  const tone = rate === null ? null : rate >= 0.95 ? "ok" : rate >= 0.85 ? "warn" : "err";
  return (
    <div className={"pool-card" + (tone ? ` pool-${tone}` : "")}>
      <div className="pool-name">{name}</div>
      <div className="pool-value">
        {rate === null ? "—" : formatPercent(rate)}
      </div>
      <div className="pool-sub">
        {total === 0 ? "no traffic" : `${formatCount(hits)} / ${formatCount(total)}`}
      </div>
      <div className="pool-bar">
        <div
          className="pool-bar-fill"
          style={{
            width: `${(rate ?? 0) * 100}%`,
            background: colour,
          }}
        />
      </div>
      {history.length > 1 ? (
        <div style={{ marginTop: 6 }}>
          <svg width="100%" height="20" viewBox={`0 0 220 20`} preserveAspectRatio="none" aria-hidden>
            <Spark values={history} stroke={colour} />
          </svg>
        </div>
      ) : null}
    </div>
  );
}

function Spark({ values, stroke }: { values: number[]; stroke: string }) {
  if (values.length < 2) return null;
  const w = 220;
  const h = 20;
  const max = Math.max(1, ...values);
  const step = w / (values.length - 1);
  const pts = values
    .map((v, i) => `${(i * step).toFixed(1)},${(h - (v / max) * (h - 2) - 1).toFixed(1)}`)
    .join(" ");
  return <polyline points={pts} fill="none" stroke={stroke} strokeWidth="1.3" opacity="0.85" />;
}
