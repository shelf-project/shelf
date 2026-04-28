import type { ReactNode } from "react";
import Sparkline from "./Sparkline";

type Props = {
  label: string;
  value: ReactNode;
  unit?: string;
  sub?: string;
  /** Rolling history to render as a sparkline underneath the value. */
  history?: number[];
  /** Override sparkline stroke. Defaults to `--accent`. */
  stroke?: string;
  /** Optional accent tint for the whole card border. */
  tone?: "ok" | "warn" | "err" | null;
};

export default function StatCard({ label, value, unit, sub, history, stroke, tone }: Props) {
  const cls =
    "card stat-card" +
    (tone === "ok" ? " stat-tone-ok" : tone === "warn" ? " stat-tone-warn" : tone === "err" ? " stat-tone-err" : "");
  return (
    <div className={cls}>
      <h3 className="card-title">{label}</h3>
      <div className="stat-value">
        <span>{value}</span>
        {unit ? <span className="stat-unit">{unit}</span> : null}
      </div>
      {sub ? <div className="stat-sub">{sub}</div> : null}
      {history && history.length > 1 ? (
        <Sparkline data={history} width={220} height={28} stroke={stroke ?? "var(--accent)"} />
      ) : null}
    </div>
  );
}
