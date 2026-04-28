import { useMemo } from "react";
import { getMetricsText } from "../api/client";
import { parseMetrics, sumSeries } from "../api/metrics";
import { usePolled } from "../polling";
import { useTimeseries } from "../hooks/useTimeseries";
import { formatCount } from "../format";
import Sparkline from "../components/Sparkline";
import AnimatedNumber from "../components/AnimatedNumber";

const PILLARS = [
  {
    title: "Row-group granular",
    body:
      "Keys are sha256(etag || offset || length). Shelf caches a 64 KiB Parquet footer or a single 4 MiB row group — not the whole 512 MiB file.",
    icon: <svg viewBox="0 0 24 24" width="22" height="22" aria-hidden><rect x="3" y="4" width="18" height="4" rx="1" fill="currentColor" opacity="0.4" /><rect x="3" y="10" width="18" height="4" rx="1" fill="currentColor" /><rect x="3" y="16" width="18" height="4" rx="1" fill="currentColor" opacity="0.4" /></svg>,
  },
  {
    title: "Plan-aware prefetch",
    body:
      "A Trino coordinator plugin warms file and footer bytes while the planner is still assigning splits, so the first probe lands on warm data.",
    icon: <svg viewBox="0 0 24 24" width="22" height="22" aria-hidden><path d="M4 12h10M4 6h14M4 18h6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" /><path d="m15 8 4 4-4 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" fill="none" /></svg>,
  },
  {
    title: "Shared across replicas",
    body:
      "One cluster, four Trino replicas, one warm working set — no cold-start tax per replica. HRW hashing picks the owner (ADR-0002).",
    icon: <svg viewBox="0 0 24 24" width="22" height="22" aria-hidden><circle cx="12" cy="12" r="3" fill="currentColor" /><circle cx="12" cy="4" r="2" fill="currentColor" opacity="0.6" /><circle cx="12" cy="20" r="2" fill="currentColor" opacity="0.6" /><circle cx="4" cy="12" r="2" fill="currentColor" opacity="0.6" /><circle cx="20" cy="12" r="2" fill="currentColor" opacity="0.6" /><path d="M12 7v2m0 6v2M7 12h2m6 0h2" stroke="currentColor" strokeWidth="1.5" /></svg>,
  },
  {
    title: "Consensus-free",
    body:
      "Membership is the Kubernetes headless service; the pin list and tenant quotas are a versioned S3 ConfigMap. No Raft, no etcd (ADR-0001).",
    icon: <svg viewBox="0 0 24 24" width="22" height="22" aria-hidden><path d="M6 4h12v6l-6 4-6-4z" fill="currentColor" opacity="0.5" /><path d="M12 14v6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" /><path d="M9 20h6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" /></svg>,
  },
] as const;

export default function ShowcaseTab() {
  const { data: metricsText } = usePolled(getMetricsText);
  const hits = useMemo(() => {
    if (!metricsText) return 0;
    return sumSeries(parseMetrics(metricsText), "shelf_hits_total");
  }, [metricsText]);
  const misses = useMemo(() => {
    if (!metricsText) return 0;
    return sumSeries(parseMetrics(metricsText), "shelf_misses_total");
  }, [metricsText]);

  const hitsSeries = useTimeseries(hits);
  const missesSeries = useTimeseries(misses);
  const hitRate = hits + misses > 0 ? hits / (hits + misses) : null;

  return (
    <>
      <section className="hero">
        <div className="hero-grid">
          <div>
            <h1>A row-group-granular, plan-aware, Iceberg-native read cache for Trino.</h1>
            <p>
              Shelf lives between Trino's Hive/Iceberg reader and S3. It caches footers and row
              groups — not whole files — and ships the working set across replicas so none of
              them pay the cold-start tax. Rust, Apache 2.0, fail-open.
            </p>
            <div className="hero-badges">
              <span className="hero-live" title="Cumulative hits_total across all pools">
                <span className="live-dot" aria-hidden />
                live · <AnimatedNumber value={hits} format={formatCount} /> cumulative hits
              </span>
              <span className="hero-live">
                hit rate · {hitRate === null ? "—" : `${(hitRate * 100).toFixed(1)}%`}
              </span>
              <span className="hero-live">
                misses · <AnimatedNumber value={misses} format={formatCount} />
              </span>
            </div>
            <div style={{ marginTop: 14 }}>
              <Sparkline
                data={hitsSeries.deltas}
                width={360}
                height={48}
                stroke="var(--accent)"
              />
            </div>
          </div>
          <div className="hero-art" aria-hidden>
            <HeroArt
              missPulse={missesSeries.deltas[missesSeries.deltas.length - 1] ?? 0}
              hitPulse={hitsSeries.deltas[hitsSeries.deltas.length - 1] ?? 0}
            />
          </div>
        </div>
      </section>

      <section className="pillars">
        {PILLARS.map((p) => (
          <div key={p.title} className="card pillar">
            <span className="pillar-icon">{p.icon}</span>
            <h3 className="pillar-title">{p.title}</h3>
            <p className="pillar-body">{p.body}</p>
          </div>
        ))}
      </section>

      <section className="card diagram">
        <h3 className="card-title">Read path</h3>
        <ReadPathDiagram />
      </section>

      <section className="card">
        <h3 className="card-title">Operator entry points</h3>
        <ul style={{ margin: 0, paddingLeft: 18, color: "var(--fg-dim)", fontSize: 13 }}>
          <li>
            CLI: <code>shelfctl stats</code>, <code>shelfctl pin &lt;key&gt;</code>,{" "}
            <code>shelfctl reload</code>. Same HTTP contract as this UI.
          </li>
          <li>
            Grafana: dashboard UID <code>shelf-read-path</code> (SHELF-27) — the source of
            truth for SLO; this tab's traffic light mirrors its thresholds.
          </li>
          <li>
            Raw: <a href="/metrics" target="_blank" rel="noreferrer">/metrics</a>,{" "}
            <a href="/stats" target="_blank" rel="noreferrer">/stats</a>,{" "}
            <a href="/admin/ring" target="_blank" rel="noreferrer">/admin/ring</a>.
          </li>
        </ul>
      </section>
    </>
  );
}

function HeroArt({ missPulse, hitPulse }: { missPulse: number; hitPulse: number }) {
  // Animated micro-diagram: shelf pod with rings pulsing when hits/misses arrive.
  const hitAmp = Math.min(1, Math.max(0, hitPulse / 10));
  const missAmp = Math.min(1, Math.max(0, missPulse / 10));
  return (
    <svg viewBox="0 0 200 160" width="100%" height="100%" role="img" aria-label="shelf pod illustration">
      <defs>
        <radialGradient id="hero-glow" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.35" />
          <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
        </radialGradient>
      </defs>
      <circle cx="100" cy="80" r="70" fill="url(#hero-glow)" />
      <g stroke="var(--accent)" fill="none">
        <circle cx="100" cy="80" r={40 + hitAmp * 12} opacity={0.5 + hitAmp * 0.5}>
          <animate attributeName="r" values="40;52;40" dur="3s" repeatCount="indefinite" />
          <animate attributeName="opacity" values="0.15;0.55;0.15" dur="3s" repeatCount="indefinite" />
        </circle>
        <circle cx="100" cy="80" r={56 + missAmp * 10} opacity={0.25 + missAmp * 0.3} strokeDasharray="3 3">
          <animate attributeName="r" values="56;64;56" dur="4.5s" repeatCount="indefinite" />
          <animate attributeName="opacity" values="0.08;0.35;0.08" dur="4.5s" repeatCount="indefinite" />
        </circle>
      </g>
      <g transform="translate(70 55)" fill="var(--bg-elev)" stroke="var(--accent)">
        <rect x="0" y="0" width="60" height="50" rx="6" />
        <rect x="6" y="8" width="48" height="4" fill="var(--accent-dim)" stroke="none" />
        <rect x="6" y="18" width="36" height="4" fill="var(--accent-dim)" stroke="none" />
        <rect x="6" y="28" width="42" height="4" fill="var(--accent-dim)" stroke="none" />
        <rect x="6" y="38" width="30" height="4" fill="var(--accent-dim)" stroke="none" />
      </g>
    </svg>
  );
}

function ReadPathDiagram() {
  return (
    <svg viewBox="0 0 600 180" role="img" aria-label="Trino → shelf plugin → shelfd → S3">
      <defs>
        <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" fill="currentColor" />
        </marker>
      </defs>
      <g fontFamily="var(--mono)" fontSize="11" fill="currentColor">
        <Node x={30} y={60} w={100} h={60} title="Trino" sub="coordinator + workers" />
        <Node x={170} y={60} w={110} h={60} title="shelf plugin" sub="plan-aware prefetch" />
        <Node x={320} y={20} w={120} h={60} title="shelfd (this pod)" sub="/cache /admin /stats" accent />
        <Node x={320} y={100} w={120} h={60} title="shelfd (peers)" sub="HRW owner" />
        <Node x={480} y={60} w={90} h={60} title="S3 / MinIO" sub="origin" />
        <line x1={130} y1={90} x2={170} y2={90} stroke="currentColor" markerEnd="url(#arr)" />
        <line x1={280} y1={75} x2={320} y2={50} stroke="currentColor" markerEnd="url(#arr)" />
        <line x1={280} y1={105} x2={320} y2={130} stroke="currentColor" markerEnd="url(#arr)" />
        <line x1={440} y1={50} x2={480} y2={85} stroke="currentColor" strokeDasharray="4 3" markerEnd="url(#arr)" />
        <line x1={440} y1={130} x2={480} y2={95} stroke="currentColor" strokeDasharray="4 3" markerEnd="url(#arr)" />
      </g>
    </svg>
  );
}

function Node({
  x,
  y,
  w,
  h,
  title,
  sub,
  accent,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  title: string;
  sub: string;
  accent?: boolean;
}) {
  return (
    <g transform={`translate(${x} ${y})`}>
      <rect
        width={w}
        height={h}
        rx={6}
        fill="var(--bg-elev-2)"
        stroke={accent ? "var(--accent)" : "var(--border)"}
      />
      <text x={w / 2} y={h / 2 - 4} textAnchor="middle" fontWeight="600">
        {title}
      </text>
      <text x={w / 2} y={h / 2 + 12} textAnchor="middle" fill="var(--fg-mute)">
        {sub}
      </text>
    </g>
  );
}
