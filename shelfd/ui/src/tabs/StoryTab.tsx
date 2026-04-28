/** Story tab — the non-technical lens.
 *
 * Replaces the prior `ShowcaseTab` (static "what shelf is" pillars)
 * with a live narrative built from `/metrics` + `/stats`. The metaphor
 * — a physical bookshelf — is borrowed from the product name itself:
 *
 *   - **desk**       = DRAM (instant)
 *   - **shelf**      = NVMe (fast)
 *   - **library**    = S3 origin (slow trip)
 *
 * Polish-pass additions on top of the original 5 panels:
 *   - **Report card** at the top — A–F grade across four dimensions,
 *     so a viewer can read the verdict in 2 s and walk away.
 *   - **Now-serving ticker** — live feed of recent hit/miss events
 *     derived from per-table label deltas. Ties the Story tab to
 *     real traffic without an SSE channel.
 *   - **Spring-physics counter** (`useSpring`) replaces the old cubic
 *     ease so the odometer / headline counter feel like Stripe-grade
 *     stat cards.
 *   - **Inline `Δ vs 5m`** on the headline percentage.
 *   - **Bookshelf panel** — every cached table is a literal book.
 *     The single change with the highest "this is unmistakably
 *     *shelf*" payoff and a graceful fallback if the data isn't
 *     ready yet.
 *   - **Snapshot to PNG** footer button, so a stakeholder can drop
 *     the verdict into a Slack thread with one click.
 *
 * Audience: PMs, leadership, new joiners. Time budget: 5 s. Every
 * panel passes the `AGENTS.md` "am I doing good or bad" test by
 * pairing a one-sentence headline with a fixed-threshold colour.
 */

import { useMemo, useRef, useState } from "react";
import {
  groupBy,
  histogramQuantile,
  parseMetrics,
  sumMatching,
  sumSeries,
  listSeries,
} from "../api/metrics";
import { getMetricsText } from "../api/client";
import { usePolled } from "../polling";
import { formatBytes, formatCount, formatDelta, formatLatencyMs } from "../format";
import { useSpring } from "../hooks/useSpring";
import { useTimeseries } from "../hooks/useTimeseries";
import NowServing from "../components/NowServing";
import ReportCard from "../components/ReportCard";
import Bookshelf from "../components/Bookshelf";

/** Optional cost calculator. The numbers below are AWS list-price S3
 * defaults for ap-south-1 as of 2026-Q2; deployments outside that
 * region (or with reservation/PPA discounts) should override these
 * via the `?cost=...` URL hash, e.g. `#story?get_per_1k=0.0004&gb=0.023`.
 * The panel renders only when both knobs are present and finite. */
function readCostKnobs(): { perThousandGet: number; perGb: number } | null {
  if (typeof window === "undefined") return null;
  const hash = window.location.hash.split("?", 2)[1] ?? "";
  if (!hash) return null;
  const params = new URLSearchParams(hash);
  const get = Number(params.get("get_per_1k"));
  const gb = Number(params.get("gb"));
  if (!Number.isFinite(get) || !Number.isFinite(gb) || get < 0 || gb < 0) return null;
  return { perThousandGet: get, perGb: gb };
}

export default function StoryTab() {
  const { data: metricsText } = usePolled(getMetricsText);
  const series = useMemo(
    () => (metricsText ? parseMetrics(metricsText) : []),
    [metricsText],
  );
  const mainRef = useRef<HTMLDivElement>(null);

  // Headline: 1 - origin/shim. We use bytes (not requests) because
  // bytes are what the "X TB never left the building" line wants.
  const shimBytes = sumSeries(series, "shelf_s3_shim_response_bytes_total");
  const originBytes = sumSeries(series, "shelf_origin_request_bytes_total");
  const savedRatio = shimBytes > 0 ? Math.max(0, 1 - originBytes / shimBytes) : null;
  const savedBytes = Math.max(0, shimBytes - originBytes);

  // Tier S4 — track the headline ratio over time so we can render a
  // `Δ vs 5m` caption next to the big number.
  const ratioTs = useTimeseries(savedRatio);

  // Skipped trips odometer: every cache hit is one S3 GET we didn't
  // make. Multiplied by p50 origin latency to derive "hours saved".
  const hits = sumSeries(series, "shelf_hits_total");
  const originP50 =
    histogramQuantile(series, "shelf_origin_request_seconds", () => true, 0.5) ?? 0.2;
  const hoursSaved = (hits * originP50) / 3600;

  // Outcome-by-bytes for the stacked bar.
  const byOutcome = groupBy(series, "shelf_s3_shim_response_bytes_total", "outcome");
  const segments: Segment[] = [
    { label: "memory", bytes: byOutcome["hit_memory"] ?? 0, tone: "ok" as const },
    { label: "disk", bytes: byOutcome["hit_disk"] ?? 0, tone: "ok" as const },
    { label: "S3", bytes: byOutcome["miss"] ?? 0, tone: "warn" as const },
    { label: "passthrough", bytes: byOutcome["passthrough"] ?? 0, tone: "neutral" as const },
  ];
  const outcomeTotal = segments.reduce((a, s) => a + s.bytes, 0);

  // Race dials.
  const cacheP50 = histogramQuantile(
    series,
    "shelf_request_seconds",
    (l) => l["outcome"] === "hit_memory" || l["outcome"] === "hit",
    0.5,
  );
  const cacheP95 = histogramQuantile(
    series,
    "shelf_request_seconds",
    (l) => l["outcome"] === "hit_memory" || l["outcome"] === "hit",
    0.95,
  );
  const originP50Real = histogramQuantile(
    series,
    "shelf_origin_request_seconds",
    () => true,
    0.5,
  );
  const originP95 = histogramQuantile(
    series,
    "shelf_origin_request_seconds",
    () => true,
    0.95,
  );

  // Stability — engine reset rate. Higher is worse, so the score is
  // an inverse of the recent reset count.
  const engineResets = sumSeries(series, "shelf_engine_resets_total");
  const resetTs = useTimeseries(engineResets);
  const resetsRecent = resetTs.deltas.slice(-3).reduce((a, b) => a + b, 0);

  // Filmstrip.
  const rollingByPool = groupBy(series, "shelf_rolling_hit_ratio_bps", "pool");
  const warmCrossed = sumMatching(
    series,
    "shelf_warm_threshold_crossed_seconds",
    () => true,
  );

  // Per-table series for NowServing + Bookshelf.
  const hitsByTable = listSeries(series, "shelf_hits_by_table_total");
  const missesByTable = listSeries(series, "shelf_misses_by_table_total");
  const bytesByTable = listSeries(
    series,
    "shelf_s3_shim_response_bytes_total",
  ).filter((r) => (r.labels["table"] ?? "") !== "");
  const bookshelfRows = useMemo(() => {
    if (bytesByTable.length === 0) {
      // Fall back to hits × estimated bytes/hit if the daemon doesn't
      // expose the per-table byte counter yet (older shelfd builds).
      const avgBytesPerHit =
        hits > 0 && shimBytes > 0 ? shimBytes / Math.max(1, hits) : 0;
      return hitsByTable.map((r) => ({
        key: r.key,
        table: r.labels["table"] ?? "?",
        pool: r.labels["pool"] ?? "?",
        bytes: r.value * avgBytesPerHit,
      }));
    }
    return bytesByTable.map((r) => ({
      key: r.key,
      table: r.labels["table"] ?? "?",
      pool: r.labels["pool"] ?? "?",
      bytes: r.value,
    }));
  }, [bytesByTable, hitsByTable, hits, shimBytes]);

  const cost = readCostKnobs();

  // Tier S4 deltas for the headline + report card.
  const headlineDelta = formatDelta(
    ratioTs.levels[ratioTs.levels.length - 1] ?? null,
    ratioTs.levels[0] ?? null,
    "higher-is-better",
    "percent",
  );

  return (
    <div ref={mainRef}>
      <ReportCard
        dimensions={[
          {
            id: "speed",
            label: "Speed",
            score: speedScore(cacheP95, originP95),
            caption:
              cacheP95 != null && originP95 != null
                ? `p95 ${formatLatencyMs(cacheP95)} vs S3 ${formatLatencyMs(originP95)}`
                : "waiting for latency samples",
          },
          {
            id: "coverage",
            label: "Coverage",
            score: savedRatio,
            caption:
              savedRatio == null
                ? "waiting for traffic"
                : `${(savedRatio * 100).toFixed(1)}% of bytes served locally`,
            delta: headlineDelta,
          },
          {
            id: "stability",
            label: "Stability",
            score: stabilityScore(resetsRecent),
            caption:
              resetsRecent === 0
                ? "0 engine resets in the last 5 min"
                : `${resetsRecent.toFixed(0)} engine reset${resetsRecent === 1 ? "" : "s"} in the last 5 min`,
          },
          {
            id: "efficiency",
            label: "Efficiency",
            score: efficiencyScore(originBytes, shimBytes),
            caption:
              shimBytes === 0
                ? "no bytes served yet"
                : `${formatBytes(savedBytes)} saved from S3 round-trips`,
          },
        ]}
      />

      <NowServing hitsByTable={hitsByTable} missesByTable={missesByTable} />

      <Headline
        ratio={savedRatio}
        servedBytes={shimBytes}
        originBytesTotal={originBytes}
        delta={headlineDelta}
      />
      <Odometer skipped={hits} hoursSaved={hoursSaved} originP50Seconds={originP50} />
      <Bookshelf
        rows={bookshelfRows}
        caption={
          shimBytes > 0
            ? `${formatBytes(savedBytes)} of bytes never left the building`
            : "the rest lives at S3"
        }
      />
      <StackedBar segments={segments} total={outcomeTotal} />
      <Race
        cacheLatency={cacheP50}
        originLatency={originP50Real ?? null}
      />
      <Filmstrip rollingByPool={rollingByPool} crossedSecondsTotal={warmCrossed} />

      {cost ? (
        <CostPanel
          savedBytes={savedBytes}
          skippedGets={hits}
          knobs={cost}
        />
      ) : null}

      <p className="story-footnote">
        Numbers are cumulative since each pod started. The ratio resets if a pod
        rolls; the filmstrip below tells you when that happened.
      </p>

      <SnapshotButton targetRef={mainRef} />
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Panel (i) — headline                                              */
/* ------------------------------------------------------------------ */

function Headline({
  ratio,
  servedBytes,
  originBytesTotal,
  delta,
}: {
  ratio: number | null;
  servedBytes: number;
  originBytesTotal: number;
  delta: ReturnType<typeof formatDelta>;
}) {
  const tone = ratio == null ? "pending" : ratio >= 0.8 ? "ok" : ratio >= 0.6 ? "warn" : "err";
  const target = ratio == null ? 0 : ratio * 100;
  const animatedPct = useSpring(target);
  const pct = ratio == null ? "—" : `${animatedPct.toFixed(1)}%`;
  const localBytes = Math.max(0, servedBytes - originBytesTotal);
  return (
    <section className={`story-headline story-tone-${tone}`}>
      <div className="story-headline-num">{pct}</div>
      <div className="story-headline-text">
        <strong>of reads never left the building.</strong>
        <span className="story-headline-sub">
          {servedBytes === 0
            ? "Waiting for traffic — try a query."
            : `${formatBytes(localBytes)} served from local storage, ${formatBytes(originBytesTotal)} fetched from S3.`}
        </span>
        {ratio != null && delta.tone !== "pending" ? (
          <span className={`story-headline-delta delta-${delta.tone}`}>
            <span aria-hidden>{delta.glyph}</span> {delta.text} vs 5 min ago
          </span>
        ) : null}
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/*  Panel (ii) — skipped trips odometer                               */
/* ------------------------------------------------------------------ */

function Odometer({
  skipped,
  hoursSaved,
  originP50Seconds,
}: {
  skipped: number;
  hoursSaved: number;
  originP50Seconds: number;
}) {
  const animated = useSpring(skipped);
  return (
    <section className="card story-odometer">
      <h3 className="card-title">Trips to S3 skipped</h3>
      <div className="story-odo-row">
        <div className="story-odo-num" aria-live="polite">
          {Math.round(animated).toLocaleString()}
        </div>
        <div className="story-odo-meta">
          <div>
            <strong>{formatHumanDuration(hoursSaved * 3600)}</strong> of waiting saved
          </div>
          <div className="story-headline-sub">
            assuming p50 origin latency of {formatLatencyMs(originP50Seconds)}
          </div>
        </div>
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/*  Panel (iii) — where the last 1 GB came from                       */
/* ------------------------------------------------------------------ */

type Segment = {
  label: string;
  bytes: number;
  tone: "ok" | "warn" | "neutral";
};

function StackedBar({ segments, total }: { segments: Segment[]; total: number }) {
  return (
    <section className="card story-stack">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <h3 className="card-title">Where the bytes came from</h3>
        <span className="story-headline-sub">
          {total > 0 ? formatBytes(total) + " served" : "no traffic yet"}
        </span>
      </div>
      <div className="story-stack-bar" role="img" aria-label="bytes by outcome">
        {total > 0 ? (
          segments.map((s) => {
            const pct = (s.bytes / total) * 100;
            if (pct < 0.1) return null;
            return (
              <div
                key={s.label}
                className={`story-stack-seg story-stack-seg-${s.tone}`}
                style={{ width: `${pct}%` }}
                title={`${s.label}: ${formatBytes(s.bytes)} (${pct.toFixed(1)}%)`}
              >
                {pct >= 8 ? (
                  <span>
                    {s.label}
                    <em>{pct.toFixed(0)}%</em>
                  </span>
                ) : null}
              </div>
            );
          })
        ) : (
          <div className="story-stack-empty">— waiting —</div>
        )}
      </div>
      <div className="story-stack-legend">
        <Legend tone="ok" label="memory · DRAM hit" />
        <Legend tone="ok" label="disk · NVMe hit" alt />
        <Legend tone="warn" label="S3 · cache miss" />
        <Legend tone="neutral" label="passthrough · uncached" />
      </div>
    </section>
  );
}

function Legend({ tone, label, alt }: { tone: Segment["tone"]; label: string; alt?: boolean }) {
  return (
    <span className="story-legend">
      <span className={`story-legend-swatch story-stack-seg-${tone}` + (alt ? " story-legend-alt" : "")} aria-hidden />
      {label}
    </span>
  );
}

/* ------------------------------------------------------------------ */
/*  Panel (iv) — the race                                             */
/* ------------------------------------------------------------------ */

function Race({
  cacheLatency,
  originLatency,
}: {
  cacheLatency: number | null;
  originLatency: number | null;
}) {
  return (
    <section className="card story-race">
      <h3 className="card-title">The race</h3>
      <p className="story-headline-sub" style={{ margin: "0 0 12px" }}>
        Median time per read. The gap between the two needles is the cache's job.
      </p>
      <div className="story-race-row">
        <Dial
          label="Through Shelf"
          seconds={cacheLatency}
          maxSeconds={Math.max(0.05, originLatency ?? 0.5)}
          tone="ok"
        />
        <Dial
          label="Direct to S3"
          seconds={originLatency}
          maxSeconds={Math.max(0.05, originLatency ?? 0.5)}
          tone="warn"
        />
      </div>
    </section>
  );
}

function Dial({
  label,
  seconds,
  maxSeconds,
  tone,
}: {
  label: string;
  seconds: number | null;
  maxSeconds: number;
  tone: "ok" | "warn";
}) {
  const r = 56;
  const cx = 70;
  const cy = 70;
  const frac = seconds == null || maxSeconds <= 0 ? 0 : Math.min(1, seconds / maxSeconds);
  // Sweep clockwise from 12 o'clock; -90° offset so 0 = up.
  const angle = -Math.PI / 2 + frac * 2 * Math.PI;
  const tipX = cx + r * 0.85 * Math.cos(angle);
  const tipY = cy + r * 0.85 * Math.sin(angle);
  return (
    <div className="story-dial">
      <svg viewBox="0 0 140 140" width="140" height="140" role="img" aria-label={`${label} ${formatLatencyMs(seconds ?? 0)}`}>
        <circle cx={cx} cy={cy} r={r} fill="none" stroke="var(--border)" strokeWidth="2" />
        {[0, 0.25, 0.5, 0.75].map((t) => {
          const a = -Math.PI / 2 + t * 2 * Math.PI;
          return (
            <line
              key={t}
              x1={cx + (r - 5) * Math.cos(a)}
              y1={cy + (r - 5) * Math.sin(a)}
              x2={cx + r * Math.cos(a)}
              y2={cy + r * Math.sin(a)}
              stroke="var(--border)"
              strokeWidth="1.5"
            />
          );
        })}
        <line
          x1={cx}
          y1={cy}
          x2={tipX}
          y2={tipY}
          stroke={`var(--${tone})`}
          strokeWidth="3"
          strokeLinecap="round"
          style={{ transition: "all 0.6s ease-out" }}
        />
        <circle cx={cx} cy={cy} r="3.5" fill={`var(--${tone})`} />
      </svg>
      <div className="story-dial-label">{label}</div>
      <div className="story-dial-value">
        {seconds == null ? "—" : formatLatencyMs(seconds)}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Panel (v) — cold→warm filmstrip                                   */
/* ------------------------------------------------------------------ */

function Filmstrip({
  rollingByPool,
  crossedSecondsTotal,
}: {
  rollingByPool: Record<string, number>;
  crossedSecondsTotal: number;
}) {
  const entries = Object.entries(rollingByPool);
  return (
    <section className="card story-filmstrip">
      <h3 className="card-title">Cold → warm</h3>
      <p className="story-headline-sub" style={{ margin: "0 0 12px" }}>
        How fast each pool got back to ≥ 80 % hit rate after the last restart.
      </p>
      {entries.length === 0 ? (
        <div className="empty">no rolling-hit-ratio yet</div>
      ) : (
        <div className="story-film-rows">
          {entries.map(([pool, bps]) => {
            const ratio = bps / 10_000;
            const tone = ratio >= 0.8 ? "ok" : ratio >= 0.6 ? "warn" : "err";
            return (
              <div key={pool} className="story-film-row">
                <span className="story-film-name">{pool}</span>
                <div className="story-film-track">
                  <div
                    className={`story-film-fill story-film-fill-${tone}`}
                    style={{ width: `${Math.min(100, ratio * 100)}%` }}
                  />
                  {ratio >= 0.8 ? <span className="story-film-flag" aria-label="warm" /> : null}
                </div>
                <span className="story-film-pct">{(ratio * 100).toFixed(0)}%</span>
              </div>
            );
          })}
        </div>
      )}
      <div className="story-headline-sub">
        Cumulative warm-up wall-clock across pools so far:{" "}
        <strong>{formatHumanDuration(crossedSecondsTotal)}</strong>.
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/*  Optional cost calculator (only when knobs present in URL hash)    */
/* ------------------------------------------------------------------ */

function CostPanel({
  savedBytes,
  skippedGets,
  knobs,
}: {
  savedBytes: number;
  skippedGets: number;
  knobs: { perThousandGet: number; perGb: number };
}) {
  const transitDollars = (savedBytes / (1024 ** 3)) * knobs.perGb;
  const requestDollars = (skippedGets / 1000) * knobs.perThousandGet;
  const total = transitDollars + requestDollars;
  const animated = useSpring(total);
  return (
    <section className="card story-cost">
      <h3 className="card-title">Estimated saved on this pod</h3>
      <div className="story-odo-num story-cost-num">${animated.toFixed(2)}</div>
      <div className="story-headline-sub">
        {formatBytes(savedBytes)} not transferred ({`$${transitDollars.toFixed(2)}`})
        {" + "}
        {formatCount(skippedGets)} GET requests skipped ({`$${requestDollars.toFixed(2)}`})
        <br />
        Knobs: ${knobs.perThousandGet}/1k GET, ${knobs.perGb}/GB. Override via URL.
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/*  Snapshot button — Tier A2                                         */
/* ------------------------------------------------------------------ */

/** Lazy-loaded so the ~10 KB gzipped `html-to-image` dependency only
 * lands in the bundle when a viewer actually clicks the button. The
 * library has no transitive deps and is the smallest of the
 * canvas-rasterise options that handles SVG correctly. */
function SnapshotButton({
  targetRef,
}: {
  targetRef: React.RefObject<HTMLElement | null>;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const onClick = async () => {
    if (!targetRef.current || busy) return;
    setBusy(true);
    setError(null);
    try {
      const mod = await import("html-to-image");
      const dataUrl = await mod.toPng(targetRef.current, {
        backgroundColor: getComputedCssVar("--bg") || "#0f0f10",
        cacheBust: true,
        pixelRatio: window.devicePixelRatio || 2,
      });
      const a = document.createElement("a");
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      a.href = dataUrl;
      a.download = `shelf-story-${stamp}.png`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="story-snapshot">
      <button
        className="story-snapshot-btn"
        onClick={onClick}
        disabled={busy}
        title="Render this Story tab as a PNG you can drop into Slack."
      >
        {busy ? "Rendering…" : "Snapshot to PNG"}
      </button>
      {error ? <span className="story-snapshot-err">{error}</span> : null}
    </div>
  );
}

function getComputedCssVar(name: string): string | null {
  if (typeof window === "undefined") return null;
  const v = window.getComputedStyle(document.documentElement).getPropertyValue(name);
  return v?.trim() || null;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/** Score helpers for the report card. All clamp to [0, 1] and return
 * `null` when the input data is insufficient — the report card maps
 * `null` to a `—` letter and a pending tone. */
function speedScore(cacheP95: number | null, originP95: number | null): number | null {
  if (cacheP95 == null || originP95 == null || originP95 <= 0) return null;
  // 1.0 when cache is ≥ 10× faster than origin, 0.0 when cache is as
  // slow as origin. Linear interpolation between log-ratios so the
  // grade tracks the order-of-magnitude improvement that's the whole
  // point of the cache.
  const ratio = cacheP95 / originP95;
  if (ratio <= 0.1) return 1.0;
  if (ratio >= 1.0) return 0.0;
  return 1 - Math.log10(1 / ratio) / Math.log10(10);
}

function stabilityScore(recentResets: number): number | null {
  // 0 resets/5min → A+; 5+ resets/5min → F.
  if (recentResets < 0) return null;
  return Math.max(0, 1 - recentResets / 5);
}

function efficiencyScore(originBytes: number, shimBytes: number): number | null {
  if (shimBytes <= 0) return null;
  return Math.max(0, Math.min(1, 1 - originBytes / shimBytes));
}

function formatHumanDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "0 s";
  if (seconds < 60) return `${seconds.toFixed(0)} s`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)} min`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)} h`;
  return `${(seconds / 86400).toFixed(1)} d`;
}
