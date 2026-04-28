/** SLO traffic light mirroring the Grafana `shelf-read-path`
 * dashboard thresholds (green ≥ 0.8, amber ≥ 0.6, red otherwise). */

type Props = {
  ratio: number | null;
  label: string;
};

export default function TrafficLight({ ratio, label }: Props) {
  const { cls, text } = classify(ratio);
  return (
    <div className="traffic-light" aria-label={`${label}: ${text}`}>
      <span className={`tl-dot ${cls}`} aria-hidden />
      <div>
        <div className="card-title" style={{ margin: 0 }}>
          {label}
        </div>
        <div style={{ fontFamily: "var(--mono)", fontSize: 13 }}>{text}</div>
      </div>
    </div>
  );
}

function classify(r: number | null): { cls: string; text: string } {
  if (r == null || !Number.isFinite(r)) return { cls: "tl-warn", text: "no data" };
  if (r >= 0.8) return { cls: "tl-ok", text: `healthy · ${(r * 100).toFixed(1)}%` };
  if (r >= 0.6) return { cls: "tl-warn", text: `warming · ${(r * 100).toFixed(1)}%` };
  return { cls: "tl-err", text: `degraded · ${(r * 100).toFixed(1)}%` };
}
