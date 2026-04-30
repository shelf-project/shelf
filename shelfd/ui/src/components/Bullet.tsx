/** Stephen Few bullet chart.
 *
 * Replaces the row-1 traffic-light tiles on the Live tab. Few's
 * argument over a gauge: stacked horizontally, so they fit in one
 * sweep of the eye, no angle-comparison cognitive cost, and you can
 * see "where in the band am I" at a glance — which a traffic-light
 * dot deliberately throws away.
 *
 * Encoding (per Few 2005, "Designing Effective Bullet Graphs"):
 *   - 3 stacked bands behind the bar: poor / satisfactory / good
 *   - 1 thin foreground bar = current value
 *   - 1 vertical tick mark = target ("green" threshold)
 *
 * We keep the same `direction` / `thresholds` contract as the old
 * `LightTile` so callers can swap in place. The `target` band drawing
 * inverts when `direction === "lower-is-better"` (good lives at the
 * left).
 *
 * Caption beneath the bar shows current value, an optional delta
 * vs 5 min ago (Tier S4), and a small caption explaining the unit.
 * We never colour-encode without a glyph — the tone classes always
 * pair with a `→` / `↑` / `↓` glyph or a textual cue.
 */

import { type DeltaReadout } from "../format";

type Props = {
  label: string;
  value: number | null;
  format: (v: number) => string;
  thresholds: { green: number; amber: number };
  direction: "higher-is-better" | "lower-is-better";
  /** Upper end of the bar's data range. If omitted we infer:
   *  higher-is-better → 1.0 (ratios) or 1.5 × green threshold;
   *  lower-is-better  → 1.5 × amber threshold. */
  scaleMax?: number;
  /** Tier S4 — optional `Δ vs 5m ago` caption rendered under the bar. */
  delta?: DeltaReadout;
  help?: string;
};

export default function Bullet({
  label,
  value,
  format,
  thresholds,
  direction,
  scaleMax,
  delta,
  help,
}: Props) {
  // Resolve the bar's domain in the data's native units.
  const max = resolveMax(thresholds, direction, scaleMax);
  const v = value == null ? 0 : Math.min(max, Math.max(0, value));
  const fracBar = max > 0 ? v / max : 0;
  const fracTarget = max > 0 ? Math.min(1, thresholds.green / max) : 0;
  const fracAmber = max > 0 ? Math.min(1, thresholds.amber / max) : 0;

  const tone = classifyTone(value, thresholds, direction);
  const glyph =
    tone === "ok" ? "●" : tone === "warn" ? "▲" : tone === "err" ? "■" : "○";

  return (
    <div
      className={`bullet bullet-${tone}`}
      title={help}
      role="img"
      aria-label={`${label}: ${value == null ? "no data" : format(value)}, target ${
        direction === "higher-is-better" ? "≥" : "≤"
      } ${format(thresholds.green)}`}
    >
      <div className="bullet-head">
        <span className="bullet-glyph" aria-hidden>
          {glyph}
        </span>
        <span className="bullet-label">{label}</span>
        <span className="bullet-value">{value == null ? "—" : format(value)}</span>
      </div>
      <div className="bullet-track">
        {direction === "higher-is-better" ? (
          <>
            <span
              className="bullet-band bullet-band-poor"
              style={{ width: `${fracAmber * 100}%` }}
            />
            <span
              className="bullet-band bullet-band-sat"
              style={{
                left: `${fracAmber * 100}%`,
                width: `${(fracTarget - fracAmber) * 100}%`,
              }}
            />
            <span
              className="bullet-band bullet-band-good"
              style={{
                left: `${fracTarget * 100}%`,
                width: `${(1 - fracTarget) * 100}%`,
              }}
            />
          </>
        ) : (
          <>
            <span
              className="bullet-band bullet-band-good"
              style={{ width: `${fracTarget * 100}%` }}
            />
            <span
              className="bullet-band bullet-band-sat"
              style={{
                left: `${fracTarget * 100}%`,
                width: `${(fracAmber - fracTarget) * 100}%`,
              }}
            />
            <span
              className="bullet-band bullet-band-poor"
              style={{
                left: `${fracAmber * 100}%`,
                width: `${(1 - fracAmber) * 100}%`,
              }}
            />
          </>
        )}
        <span
          className={`bullet-bar bullet-bar-${tone}`}
          style={{ width: `${fracBar * 100}%` }}
        />
        <span
          className="bullet-target"
          style={{ left: `${fracTarget * 100}%` }}
          aria-hidden
        />
      </div>
      <div className="bullet-foot">
        <span className="bullet-caption">
          target {direction === "higher-is-better" ? "≥" : "≤"} {format(thresholds.green)}
        </span>
        {delta && delta.tone !== "pending" ? (
          <span className={`bullet-delta delta-${delta.tone}`}>
            <span aria-hidden>{delta.glyph}</span> {delta.text} vs 5m
          </span>
        ) : delta ? (
          <span className="bullet-delta delta-pending">— vs 5m</span>
        ) : null}
      </div>
    </div>
  );
}

function resolveMax(
  thresholds: { green: number; amber: number },
  direction: "higher-is-better" | "lower-is-better",
  scaleMax?: number,
): number {
  if (scaleMax != null && scaleMax > 0) return scaleMax;
  // Ratio-shaped (0..1) thresholds: cap at 1 so the band geometry is
  // honest. Otherwise pad the worse threshold by 50%.
  const upper = Math.max(thresholds.green, thresholds.amber);
  if (upper <= 1) return 1;
  return direction === "higher-is-better"
    ? Math.max(thresholds.green * 1.5, thresholds.amber * 1.2)
    : thresholds.amber * 1.5;
}

function classifyTone(
  value: number | null,
  thresholds: { green: number; amber: number },
  direction: "higher-is-better" | "lower-is-better",
): "ok" | "warn" | "err" | "pending" {
  if (value == null) return "pending";
  if (direction === "higher-is-better") {
    if (value >= thresholds.green) return "ok";
    if (value >= thresholds.amber) return "warn";
    return "err";
  }
  if (value <= thresholds.green) return "ok";
  if (value <= thresholds.amber) return "warn";
  return "err";
}
