/** Lab tab — researcher / on-call deep-dive surface.
 *
 * Dense, single-column scrollable. No metaphors, no trophies — every
 * panel is the literal histogram or matrix that a tuning sweep needs.
 * This tab is also the "junk drawer" that guarantees every metric in
 * `EXPOSED_SERIES` has a home (per the design plan §4 covenant).
 *
 * Panels:
 *   1. Admission decision split (admit/reject_size/reject_model/...)
 *   2. Eviction reason split (capacity/admin/ttl/unpin/reload)
 *   3. Peer fan-in matrix (HRW peer-fetch outcomes)
 *   4. Outcome × latency heat-strip (catches `hit_disk` 16 s anomaly)
 *   5. Warm-up overlay (rolling hit ratio per pod)
 *   6. Engine reset audit log
 *   7. Bytes-saved budget (running total)
 *   8. MV ROI (Phase C — hidden until any non-zero series exists)
 */

import { useMemo } from "react";
import { getMetricsText } from "../api/client";
import {
  histogramBuckets,
  listSeries,
  parseMetrics,
  sumSeries,
} from "../api/metrics";
import { usePolled } from "../polling";
import { useTimeseries } from "../hooks/useTimeseries";
import {
  formatBytes,
  formatCount,
  formatDelta,
  formatLatencyMs,
  formatPercent,
} from "../format";

export default function LabTab() {
  const { data: metricsText, error } = usePolled(getMetricsText);
  const series = useMemo(
    () => (metricsText ? parseMetrics(metricsText) : []),
    [metricsText],
  );

  const admissions = listSeries(series, "shelf_admissions_total");
  const evictions = listSeries(series, "shelf_evictions_total");
  const peerHit = listSeries(series, "shelf_peer_hit_total");
  const peerMiss = listSeries(series, "shelf_peer_miss_total");
  const peerTimeout = listSeries(series, "shelf_peer_timeout_total");
  const peerError = listSeries(series, "shelf_peer_error_total");
  const rolling = listSeries(series, "shelf_rolling_hit_ratio_bps");
  const resets = listSeries(series, "shelf_engine_resets_total").filter(
    (r) => r.value > 0,
  );

  const shimBytes = sumSeries(series, "shelf_s3_shim_response_bytes_total");
  const originBytes = sumSeries(series, "shelf_origin_request_bytes_total");
  const savedBytes = Math.max(0, shimBytes - originBytes);
  const savedRatio = shimBytes > 0 ? savedBytes / shimBytes : null;

  // Tier S4 — track the cumulative bytes-saved counter so we can
  // render a `Δ vs 5m` caption alongside the absolute total.
  const savedBytesTs = useTimeseries(savedBytes);
  const savedBytesDelta = formatDelta(
    savedBytesTs.levels[savedBytesTs.levels.length - 1] ?? null,
    savedBytesTs.levels[0] ?? null,
    "higher-is-better",
    "absolute",
  );

  const mvHits = listSeries(series, "shelf_mv_hits_total");
  const mvBytes = listSeries(series, "shelf_mv_bytes_served_total");
  const showMvPanel = mvHits.some((r) => r.value > 0) || mvBytes.some((r) => r.value > 0);

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

      {/* ------- 1. Admission split ------- */}
      <section className="card">
        <h3 className="card-title">Admission decisions</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Where the size/model gate sent each candidate. <code>reject_size</code>
          dominating means the threshold is too aggressive for the working set.
        </p>
        <BucketSplit
          rows={admissions}
          dim="decision"
          colorMap={{
            admit: "var(--ok)",
            reject_size: "var(--warn)",
            reject_model: "var(--accent-disk)",
            reject_other: "var(--err)",
          }}
        />
      </section>

      {/* ------- 2. Eviction split ------- */}
      <section className="card">
        <h3 className="card-title">Eviction reasons</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Capacity is the only "natural" reason — anything else (admin, ttl, unpin,
          reload) is operator action. The capacity / admin split was the goal of A5.
        </p>
        <BucketSplit
          rows={evictions}
          dim="reason"
          colorMap={{
            capacity: "var(--accent)",
            admin: "var(--accent-disk)",
            ttl: "var(--warn)",
            unpin: "var(--ok)",
            reload: "var(--fg-dim)",
          }}
        />
      </section>

      {/* ------- 3. Peer fan-in matrix ------- */}
      <section className="card">
        <h3 className="card-title">Peer fan-in</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          On a local miss, shelfd may race the HRW primary against origin. The
          payoff ratio is <code>peer_hit / (peer_hit + peer_miss + peer_timeout +
          peer_error)</code>.
        </p>
        <PeerMatrix
          hits={peerHit}
          miss={peerMiss}
          timeout={peerTimeout}
          error={peerError}
        />
      </section>

      {/* ------- 4. Outcome × latency heat-strip ------- */}
      <section className="card">
        <h3 className="card-title">Latency heat-strip</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Histogram bins of <code>shelf_request_seconds</code> per outcome. A wall
          of dark cells past <code>~10 s</code> on <code>hit_disk</code> is the
          classic Foyer LODC RateLimitPicker tell.
        </p>
        <Heatstrip series={series} />
      </section>

      {/* ------- 5. Warm-up overlay ------- */}
      <section className="card">
        <h3 className="card-title">Warm-up overlay</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Current rolling hit ratio per pool. Persistent gap between pools is
          usually KEDA-driven — see runbook §3.
        </p>
        <RollingBars rows={rolling} />
      </section>

      {/* ------- 6. Engine reset audit ------- */}
      <section className="card">
        <h3 className="card-title">Engine resets — audit log</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Non-zero on a healthy cluster is a paging signal. The cumulative count
          resets when the pod restarts; pair with{" "}
          <code>kube_pod_container_status_restarts_total</code>.
        </p>
        {resets.length === 0 ? (
          <div className="culprit-empty">No engine resets observed.</div>
        ) : (
          <table className="lab-table">
            <thead>
              <tr>
                <th>Pool</th>
                <th>Reason</th>
                <th>Cumulative</th>
              </tr>
            </thead>
            <tbody>
              {resets.map((r, i) => (
                <tr key={i}>
                  <td>
                    <code>{r.labels["pool"]}</code>
                  </td>
                  <td>
                    <code>{r.labels["reason"]}</code>
                  </td>
                  <td>{formatCount(r.value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {/* ------- 7. Bytes-saved budget ------- */}
      <section className="card">
        <h3 className="card-title">Bytes-saved budget</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Cumulative since pod start. The ratio is{" "}
          <code>1 - origin_bytes / shim_bytes</code> — it's the same number the
          Story tab headlines.
        </p>
        <div className="lab-budget-grid">
          <div>
            <span className="stat-sub">Bytes returned to Trino</span>
            <strong>{formatBytes(shimBytes)}</strong>
          </div>
          <div>
            <span className="stat-sub">Bytes pulled from S3</span>
            <strong>{formatBytes(originBytes)}</strong>
          </div>
          <div>
            <span className="stat-sub">Bytes saved</span>
            <strong>{formatBytes(savedBytes)}</strong>
            {savedBytesDelta.tone !== "pending" ? (
              <span className={`lab-budget-delta delta-${savedBytesDelta.tone}`}>
                <span aria-hidden>{savedBytesDelta.glyph}</span>{" "}
                {Number.isFinite(Number(savedBytesDelta.text))
                  ? formatBytes(Math.abs(Number(savedBytesDelta.text)))
                  : savedBytesDelta.text}{" "}
                vs 5m
              </span>
            ) : null}
          </div>
          <div>
            <span className="stat-sub">Ratio</span>
            <strong>{savedRatio == null ? "—" : formatPercent(savedRatio)}</strong>
          </div>
        </div>
      </section>

      {/* ------- 8. MV ROI (lazy, Phase C) ------- */}
      {showMvPanel ? (
        <section className="card">
          <h3 className="card-title">Materialized view impact</h3>
          <p className="stat-sub" style={{ margin: "0 0 8px" }}>
            Hits served from a pinned Iceberg MV snapshot. Numerator of the
            "MV killed origin bytes" panel.
          </p>
          <table className="lab-table">
            <thead>
              <tr>
                <th>MV</th>
                <th>Hits</th>
                <th>Bytes served</th>
                <th>Avg bytes/hit</th>
              </tr>
            </thead>
            <tbody>
              {mvHits.map((r) => {
                const name = r.labels["mv_name"] ?? "?";
                const bytes =
                  mvBytes.find((b) => b.labels["mv_name"] === name)?.value ?? 0;
                const avg = r.value > 0 ? bytes / r.value : 0;
                return (
                  <tr key={name}>
                    <td>
                      <code>{name}</code>
                    </td>
                    <td>{formatCount(r.value)}</td>
                    <td>{formatBytes(bytes)}</td>
                    <td>{formatBytes(avg)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>
      ) : (
        <section className="card lab-mv-stub">
          <h3 className="card-title">Materialized view impact</h3>
          <div className="culprit-empty">
            Hidden until <code>shelf_mv_hits_total</code> reports a non-zero series
            (gated on H3 — the MV pin-watcher).
          </div>
        </section>
      )}
    </>
  );
}

/* ------------------------------------------------------------------ */
/*  Sub-component — admission / eviction split (100% bar + table)     */
/* ------------------------------------------------------------------ */

function BucketSplit({
  rows,
  dim,
  colorMap,
}: {
  rows: Array<{ key: string; labels: Record<string, string>; value: number }>;
  dim: string;
  colorMap: Record<string, string>;
}) {
  const total = rows.reduce((a, r) => a + r.value, 0);
  if (total === 0) {
    return <div className="culprit-empty">No samples observed.</div>;
  }
  // Aggregate per dim value (across pools).
  const agg = new Map<string, number>();
  for (const r of rows) {
    const k = r.labels[dim] ?? "?";
    agg.set(k, (agg.get(k) ?? 0) + r.value);
  }
  const items = Array.from(agg.entries()).sort((a, b) => b[1] - a[1]);
  return (
    <>
      <div className="lab-bucket-bar" role="img" aria-label={`${dim} split`}>
        {items.map(([k, v]) => {
          const pct = (v / total) * 100;
          if (pct < 0.1) return null;
          return (
            <div
              key={k}
              style={{ width: `${pct}%`, background: colorMap[k] ?? "var(--accent)" }}
              title={`${k}: ${formatCount(v)} (${pct.toFixed(1)}%)`}
            >
              {pct >= 8 ? <span>{k}</span> : null}
            </div>
          );
        })}
      </div>
      <table className="lab-table">
        <thead>
          <tr>
            <th>{dim}</th>
            <th>Count</th>
            <th>Share</th>
          </tr>
        </thead>
        <tbody>
          {items.map(([k, v]) => (
            <tr key={k}>
              <td>
                <span
                  className="lab-swatch"
                  style={{ background: colorMap[k] ?? "var(--accent)" }}
                  aria-hidden
                />
                <code>{k}</code>
              </td>
              <td>{formatCount(v)}</td>
              <td>{formatPercent(v / total)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

/* ------------------------------------------------------------------ */
/*  Sub-component — peer fan-in matrix                                */
/* ------------------------------------------------------------------ */

function PeerMatrix({
  hits,
  miss,
  timeout,
  error,
}: {
  hits: Array<{ labels: Record<string, string>; value: number }>;
  miss: Array<{ labels: Record<string, string>; value: number }>;
  timeout: Array<{ labels: Record<string, string>; value: number }>;
  error: Array<{ labels: Record<string, string>; value: number }>;
}) {
  const pools = Array.from(
    new Set(
      [...hits, ...miss, ...timeout, ...error].map((r) => r.labels["pool"] ?? "?"),
    ),
  );
  if (pools.length === 0) {
    return <div className="culprit-empty">No peer probes observed.</div>;
  }
  const sumByPool = (
    rows: Array<{ labels: Record<string, string>; value: number }>,
    pool: string,
  ) => rows.filter((r) => r.labels["pool"] === pool).reduce((a, r) => a + r.value, 0);
  return (
    <table className="lab-table">
      <thead>
        <tr>
          <th>Pool</th>
          <th>peer_hit</th>
          <th>peer_miss</th>
          <th>peer_timeout</th>
          <th>peer_error</th>
          <th>Payoff</th>
        </tr>
      </thead>
      <tbody>
        {pools.map((p) => {
          const h = sumByPool(hits, p);
          const m = sumByPool(miss, p);
          const t = sumByPool(timeout, p);
          const e = sumByPool(error, p);
          const total = h + m + t + e;
          const ratio = total > 0 ? h / total : null;
          return (
            <tr key={p}>
              <td>
                <code>{p}</code>
              </td>
              <td>{formatCount(h)}</td>
              <td>{formatCount(m)}</td>
              <td>{formatCount(t)}</td>
              <td>{formatCount(e)}</td>
              <td>
                <strong>{ratio == null ? "—" : formatPercent(ratio)}</strong>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/* ------------------------------------------------------------------ */
/*  Sub-component — heat-strip                                        */
/* ------------------------------------------------------------------ */

function Heatstrip({
  series,
}: {
  series: import("../api/metrics").Sample[];
}) {
  const outcomes = ["hit_memory", "hit_disk", "miss", "passthrough"];
  // Sample one bucket vector per outcome; we render the cumulative
  // counts directly as cells (lighter = lower density).
  const rows = outcomes.map((o) => ({
    outcome: o,
    buckets: histogramBuckets(series, "shelf_request_seconds", (l) => l["outcome"] === o),
  }));
  const allMax = Math.max(
    1,
    ...rows.flatMap((r) => r.buckets.map((b) => b.count)),
  );

  if (rows.every((r) => r.buckets.length === 0)) {
    return <div className="culprit-empty">No latency observations yet.</div>;
  }
  return (
    <div className="heatstrip">
      {rows.map((r) => (
        <div key={r.outcome} className="heatstrip-row">
          <span className="heatstrip-name">{r.outcome}</span>
          <div className="heatstrip-cells">
            {r.buckets.length === 0 ? (
              <span className="heatstrip-empty">—</span>
            ) : (
              r.buckets.map((b, i) => {
                const intensity = b.count / allMax;
                const labelLe = Number.isFinite(b.le)
                  ? formatLatencyMs(b.le)
                  : "+∞";
                return (
                  <span
                    key={i}
                    className="heatstrip-cell"
                    title={`≤ ${labelLe}: ${formatCount(b.count)}`}
                    style={{ opacity: 0.15 + intensity * 0.85 }}
                  >
                    {Number.isFinite(b.le) && b.le <= 1 ? "" : labelLe[0] ?? ""}
                  </span>
                );
              })
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Sub-component — warm-up overlay (per-pool rolling-hit-ratio bars) */
/* ------------------------------------------------------------------ */

function RollingBars({
  rows,
}: {
  rows: Array<{ labels: Record<string, string>; value: number }>;
}) {
  if (rows.length === 0) {
    return (
      <div className="culprit-empty">
        Pods haven't reported a rolling hit ratio yet.
      </div>
    );
  }
  return (
    <div className="lab-rolling">
      {rows.map((r) => {
        const ratio = r.value / 10_000;
        const tone = ratio >= 0.8 ? "ok" : ratio >= 0.6 ? "warn" : "err";
        return (
          <div key={r.labels["pool"]} className="lab-rolling-row">
            <span className="lab-rolling-name">{r.labels["pool"]}</span>
            <div className="lab-rolling-track">
              <div
                className={`lab-rolling-fill lab-rolling-${tone}`}
                style={{ width: `${Math.min(100, ratio * 100)}%` }}
              />
            </div>
            <span className="lab-rolling-pct">{(ratio * 100).toFixed(1)}%</span>
          </div>
        );
      })}
    </div>
  );
}
