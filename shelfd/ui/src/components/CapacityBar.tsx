import { formatBytes } from "../format";
import Sparkline from "./Sparkline";

type Props = {
  label: string;
  used: number;
  capacity: number;
  variant?: "dram" | "disk";
  /** Rolling history of `used_bytes`. Rendered as a small sparkline
   * above the bar so operators see whether the pool is filling,
   * draining, or flat. */
  history?: number[];
};

export default function CapacityBar({ label, used, capacity, variant = "dram", history }: Props) {
  const pct = capacity > 0 ? Math.min(100, (used / capacity) * 100) : 0;
  const tone = pct >= 90 ? "err" : pct >= 75 ? "warn" : "ok";
  const stroke =
    variant === "disk"
      ? "var(--accent-disk)"
      : tone === "err"
      ? "var(--err)"
      : tone === "warn"
      ? "var(--warn)"
      : "var(--accent)";
  return (
    <div className="cap">
      <div className="cap-head">
        <span>{label}</span>
        <span>
          {formatBytes(used)} / {capacity > 0 ? formatBytes(capacity) : "—"}
          {capacity > 0 ? ` (${pct.toFixed(0)}%)` : ""}
        </span>
      </div>
      {history && history.length > 1 ? (
        <Sparkline data={history} width={320} height={18} stroke={stroke} />
      ) : null}
      <div className="bar" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
        <div
          className={
            "bar-fill" +
            (variant === "disk" ? " bar-fill-disk" : "") +
            (tone === "warn" ? " bar-fill-warn" : "") +
            (tone === "err" ? " bar-fill-err" : "")
          }
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}
