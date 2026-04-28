/** Hot tables tab — per-Iceberg-table leaderboard.
 *
 * First UI consumer of `shelf_hits_by_table_total` /
 * `shelf_misses_by_table_total` (track G-4). The series is pool +
 * table-keyed; we fold across pools by default so a "table" is one
 * row regardless of whether the hit landed in the metadata or
 * rowgroup pool. Toggle splits them.
 *
 * Two creative additions over a vanilla leaderboard:
 *   1. A trophy / dot column that signals top-10 / top-100 visually
 *      (good for a stakeholder screenshot, costs nothing).
 *   2. A "cold table" warning row that auto-pins the worst offenders
 *      to the top — tables that are *paying* the cache cost (≥ 10
 *      misses/min) without benefiting (< 30 % hit rate). Empty most
 *      of the time, screams when relevant.
 */

import { useMemo, useState } from "react";
import { getMetricsText } from "../api/client";
import { listSeries, parseMetrics } from "../api/metrics";
import { useTimeseriesByKey } from "../hooks/useTimeseries";
import { usePolled } from "../polling";
import { formatCount, formatDelta, formatPercent } from "../format";
import Sparkline from "../components/Sparkline";

type Mode = "all" | "metadata" | "rowgroup";

type Row = {
  pool: string;
  table: string;
  hits: number;
  misses: number;
  hitsKey: string;
  missesKey: string;
};

export default function HotTablesTab() {
  const { data: metricsText, error } = usePolled(getMetricsText);
  const [mode, setMode] = useState<Mode>("all");

  const series = useMemo(
    () => (metricsText ? parseMetrics(metricsText) : []),
    [metricsText],
  );

  const hitsList = listSeries(series, "shelf_hits_by_table_total");
  const missesList = listSeries(series, "shelf_misses_by_table_total");

  // Fold to one row per (pool, table); the metric's own labels
  // already give us that. Filter by `mode` once.
  const filt = (rs: typeof hitsList) =>
    rs.filter(
      (r) =>
        mode === "all" ||
        (r.labels["pool"] ?? "") === mode,
    );

  // Build the unified row set keyed by (pool, table).
  const rowKey = (labels: Record<string, string>) =>
    `${labels["pool"] ?? "?"}|${labels["table"] ?? "?"}`;
  const allKeys = new Set<string>([
    ...filt(hitsList).map((r) => rowKey(r.labels)),
    ...filt(missesList).map((r) => rowKey(r.labels)),
  ]);
  const rows: Row[] = Array.from(allKeys).map((k) => {
    const [pool, table] = k.split("|");
    const h = filt(hitsList).find((r) => rowKey(r.labels) === k);
    const m = filt(missesList).find((r) => rowKey(r.labels) === k);
    return {
      pool,
      table,
      hits: h?.value ?? 0,
      misses: m?.value ?? 0,
      hitsKey: h?.key ?? `none-h-${k}`,
      missesKey: m?.key ?? `none-m-${k}`,
    };
  });
  rows.sort((a, b) => b.hits + b.misses - (a.hits + a.misses));

  // Per-row hit sparkline: pump the hits counter set into useTimeseriesByKey.
  const hitsTrack = useTimeseriesByKey(
    filt(hitsList).map((r) => ({ key: r.key, value: r.value })),
  );
  const missesTrack = useTimeseriesByKey(
    filt(missesList).map((r) => ({ key: r.key, value: r.value })),
  );

  // Cold-table flag: ≥ 10 misses/min AND < 30 % hit rate. We compute
  // miss rate from the recent deltas.
  const cold: Row[] = rows.filter((r) => {
    const total = r.hits + r.misses;
    if (total < 10) return false;
    const ratio = r.hits / total;
    if (ratio >= 0.3) return false;
    const missDeltas = missesTrack.get(r.missesKey)?.deltas ?? [];
    const missRatePerMin =
      missDeltas.length === 0
        ? 0
        : (missDeltas.reduce((a, b) => a + b, 0) / Math.max(1, missDeltas.length)) * 12;
    return missRatePerMin >= 10;
  });

  const topRows = rows.slice(0, 50);

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

      <section className="card hot-controls">
        <h3 className="card-title">Hot tables</h3>
        <p className="stat-sub" style={{ margin: "0 0 8px" }}>
          Live leaderboard from <code>shelf_hits_by_table_total</code> /{" "}
          <code>shelf_misses_by_table_total</code>. Cardinality is bounded — unparsed
          keys fold to <code>other</code>.
        </p>
        <div className="hot-mode-row" role="tablist">
          {(["all", "metadata", "rowgroup"] as Mode[]).map((m) => (
            <button
              key={m}
              role="tab"
              aria-selected={mode === m}
              className={"chip" + (mode === m ? " chip-active" : "")}
              onClick={() => setMode(m)}
            >
              {m}
            </button>
          ))}
          <span className="hot-summary">
            {rows.length === 0
              ? "no labelled traffic yet"
              : `${rows.length} table${rows.length === 1 ? "" : "s"} · ${formatCount(
                  rows.reduce((a, r) => a + r.hits + r.misses, 0),
                )} reads`}
          </span>
        </div>
      </section>

      {cold.length > 0 ? (
        <section className="card cold-warning">
          <h3 className="card-title">Cold tables — paying without benefiting</h3>
          <p className="stat-sub" style={{ margin: "0 0 8px" }}>
            Hit rate &lt; 30 % AND miss rate ≥ 10/min. These are candidates to pin or
            exclude from the cache.
          </p>
          <ul className="cold-list">
            {cold.map((r) => (
              <li key={`${r.pool}-${r.table}`}>
                <span>
                  {r.pool} · <strong>{r.table}</strong>
                </span>
                <span>{formatPercent(r.hits / (r.hits + r.misses))} hit rate</span>
                <span>{formatCount(r.misses)} misses</span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      <section className="card hot-table-card">
        <table className="hot-table">
          <thead>
            <tr>
              <th style={{ width: 36 }}>#</th>
              <th>Table</th>
              <th>Pool</th>
              <th>Trend</th>
              <th>Hit rate</th>
              <th>Hits</th>
              <th>Misses</th>
            </tr>
          </thead>
          <tbody>
            {topRows.length === 0 ? (
              <tr>
                <td colSpan={7} className="empty">
                  Waiting for shelf_hits_by_table_total to start reporting…
                </td>
              </tr>
            ) : (
              topRows.map((r, i) => {
                const total = r.hits + r.misses;
                const ratio = total === 0 ? null : r.hits / total;
                const tone =
                  ratio == null
                    ? "pending"
                    : ratio >= 0.8
                      ? "ok"
                      : ratio >= 0.6
                        ? "warn"
                        : "err";
                const trend = hitsTrack.get(r.hitsKey)?.deltas ?? [];
                return (
                  <tr key={`${r.pool}-${r.table}`}>
                    <td className="hot-rank">
                      {i < 3 ? (
                        <span className={`trophy trophy-${i + 1}`} aria-label={`top ${i + 1}`}>
                          {i === 0 ? "①" : i === 1 ? "②" : "③"}
                        </span>
                      ) : i < 10 ? (
                        <span className="trophy-dot trophy-dot-gold" aria-hidden />
                      ) : (
                        <span className="trophy-dot" aria-hidden />
                      )}
                      <span>{i + 1}</span>
                    </td>
                    <td>
                      <code>{r.table}</code>
                    </td>
                    <td>
                      <span className={`pool-pill pool-pill-${r.pool}`}>{r.pool}</span>
                    </td>
                    <td style={{ width: 110 }}>
                      <Sparkline
                        data={trend}
                        width={100}
                        height={20}
                        stroke={
                          tone === "err"
                            ? "var(--err)"
                            : tone === "warn"
                              ? "var(--warn)"
                              : "var(--ok)"
                        }
                      />
                    </td>
                    <td>
                      <span className={`hot-pct hot-pct-${tone}`}>
                        {ratio == null ? "—" : formatPercent(ratio)}
                      </span>
                      <HitRateDelta
                        hitsLevels={hitsTrack.get(r.hitsKey)?.levels ?? []}
                        missesLevels={missesTrack.get(r.missesKey)?.levels ?? []}
                      />
                    </td>
                    <td>{formatCount(r.hits)}</td>
                    <td>{formatCount(r.misses)}</td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </section>
    </>
  );
}

/** Tier S4 — render a `Δ vs 5m` caption beneath the per-row hit rate.
 *
 * We compute "then" from the oldest snapshot we have for both
 * counters; "now" from the latest. If either history is empty we
 * render `—` so the column never shifts width. */
function HitRateDelta({
  hitsLevels,
  missesLevels,
}: {
  hitsLevels: number[];
  missesLevels: number[];
}) {
  if (hitsLevels.length < 2 || missesLevels.length < 2) {
    return <span className="hot-delta delta-pending">— vs 5m</span>;
  }
  const ratioAt = (i: number) => {
    const h = hitsLevels[i] ?? 0;
    const m = missesLevels[i] ?? 0;
    const t = h + m;
    return t > 0 ? h / t : null;
  };
  const now = ratioAt(hitsLevels.length - 1);
  const then = ratioAt(0);
  const delta = formatDelta(now, then, "higher-is-better", "percent");
  if (delta.tone === "pending") {
    return <span className="hot-delta delta-pending">— vs 5m</span>;
  }
  return (
    <span className={`hot-delta delta-${delta.tone}`}>
      <span aria-hidden>{delta.glyph}</span> {delta.text}
    </span>
  );
}
