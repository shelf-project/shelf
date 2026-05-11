/** Capacity vessel — the Live tab's signature overdrive moment.
 *
 * Replaces the flat `CapacityBar` with a glass cylinder whose fill
 * height tracks `used / capacity`. Two engraved tick marks (warn at
 * 75%, crit at 90%) sit on the glass; the surface ripples for ~1.2 s
 * after every fresh `/stats` poll so the operator sees data arriving.
 *
 * Design discipline (per `.impeccable.md`):
 *   - Motion = meaning. Surface tension only ripples *after* a fresh
 *     poll, never decoratively. The ripple amplitude scales with the
 *     change in fill, so a stable pool reads as still water and a
 *     spike reads as a splash.
 *   - "Good or bad?" test: the fill colour follows the same OK / WARN
 *     / ERR thresholds as the bar variant — operators can read state
 *     in 1 s without parsing prose.
 *   - Reduced-motion: the fill is a flat rectangle, no surface, no
 *     ripple — still beautiful, never "no animation".
 *
 * The component is unstyled-on-purpose for the meta column (label /
 * numbers / pct); CSS in `styles.css` owns the layout.
 */

import { useEffect, useRef, useState } from "react";
import { formatBytes } from "../format";
import { useReducedMotion } from "../hooks/useReducedMotion";
import { useSpring } from "../hooks/useSpring";

type Props = {
  label: string;
  used: number;
  capacity: number;
  variant?: "dram" | "disk";
};

const W = 38;
const H = 56;
const PAD_X = 4;
const PAD_TOP = 4;
const PAD_BOT = 4;

export default function CapacityVessel({ label, used, capacity, variant = "dram" }: Props) {
  const reduce = useReducedMotion();
  const pct = capacity > 0 ? Math.min(100, Math.max(0, (used / capacity) * 100)) : 0;
  const tone = pct >= 90 ? "crit" : pct >= 75 ? "warn" : "ok";
  const fillColor =
    variant === "disk"
      ? "var(--accent-disk)"
      : tone === "crit"
      ? "var(--err)"
      : tone === "warn"
      ? "var(--warn)"
      : "var(--accent)";

  // Spring-driven height so a fresh poll *settles* into the new level
  // rather than snapping. `useSpring` returns the target unchanged
  // under reduced-motion — same hook, two visual outcomes.
  const fillFrac = useSpring(pct / 100, 130, 22);

  // Surface ripple state: bumped each time the underlying value
  // changes meaningfully (>0.25%). Decays on a separate RAF loop so
  // the SVG path can interpolate without re-rendering React state.
  const lastValueRef = useRef<number>(pct);
  const rippleRef = useRef<{ start: number; amp: number } | null>(null);
  const [, setTick] = useState(0);

  useEffect(() => {
    if (reduce) return;
    const delta = Math.abs(pct - lastValueRef.current);
    lastValueRef.current = pct;
    if (delta < 0.25) return;
    rippleRef.current = {
      start: performance.now(),
      amp: Math.min(2.4, 0.6 + delta / 8),
    };
  }, [pct, reduce]);

  useEffect(() => {
    if (reduce) return;
    let raf = 0;
    const tick = () => {
      const r = rippleRef.current;
      if (r) {
        const age = (performance.now() - r.start) / 1000;
        if (age > 1.4) {
          rippleRef.current = null;
        } else {
          setTick((n) => (n + 1) & 0xff);
        }
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [reduce]);

  const innerH = H - PAD_TOP - PAD_BOT;
  const fillH = innerH * fillFrac;
  const fillY = PAD_TOP + (innerH - fillH);

  // Build the surface — a flat top edge if reduced-motion or no
  // active ripple, otherwise a sine wave whose phase advances and
  // amplitude decays with ripple age.
  const surface = surfacePath(fillY, fillH, rippleRef.current, reduce);

  return (
    <div className={`capacity-vessel capacity-vessel-tone-${tone}`} role="group" aria-label={label}>
      <div className="capacity-vessel-glass">
        <svg className="capacity-vessel-svg" viewBox={`0 0 ${W} ${H}`} aria-hidden>
          <defs>
            <clipPath id={`cap-clip-${label.replace(/\W+/g, "-")}`}>
              <rect
                x={PAD_X}
                y={PAD_TOP}
                width={W - PAD_X * 2}
                height={innerH}
                rx="3"
                ry="3"
              />
            </clipPath>
            <linearGradient id={`cap-grad-${label.replace(/\W+/g, "-")}`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor={fillColor} stopOpacity="0.95" />
              <stop offset="100%" stopColor={fillColor} stopOpacity="0.65" />
            </linearGradient>
          </defs>
          {/* Glass frame. */}
          <rect
            className="capacity-vessel-frame"
            x={PAD_X}
            y={PAD_TOP}
            width={W - PAD_X * 2}
            height={innerH}
            rx="3"
            ry="3"
          />
          {/* Liquid: clipped to frame, surface drawn as path. */}
          <g clipPath={`url(#cap-clip-${label.replace(/\W+/g, "-")})`}>
            <path
              className="capacity-vessel-fill"
              d={surface}
              fill={`url(#cap-grad-${label.replace(/\W+/g, "-")})`}
            />
          </g>
          {/* Engraved warn / crit ticks. */}
          <line
            x1={W - PAD_X - 1}
            y1={PAD_TOP + innerH * 0.25}
            x2={W - PAD_X - 4}
            y2={PAD_TOP + innerH * 0.25}
            stroke="var(--border)"
            strokeWidth="1"
          />
          <line
            x1={W - PAD_X - 1}
            y1={PAD_TOP + innerH * 0.1}
            x2={W - PAD_X - 4}
            y2={PAD_TOP + innerH * 0.1}
            stroke="var(--border)"
            strokeWidth="1"
          />
        </svg>
      </div>
      <div className="capacity-vessel-meta">
        <div className="capacity-vessel-label">{label}</div>
        <div className="capacity-vessel-pct">
          {capacity > 0 ? `${pct.toFixed(0)}%` : "—"}
        </div>
        <div className="capacity-vessel-numbers">
          {formatBytes(used)} / {capacity > 0 ? formatBytes(capacity) : "—"}
        </div>
      </div>
    </div>
  );
}

function surfacePath(
  fillY: number,
  fillH: number,
  ripple: { start: number; amp: number } | null,
  reduce: boolean,
): string {
  // Bottom-aligned trapezoid with a sinusoidal top edge.
  const left = PAD_X;
  const right = W - PAD_X;
  const bottom = PAD_TOP + (H - PAD_TOP - PAD_BOT);
  if (reduce || !ripple || fillH <= 0) {
    return `M ${left} ${fillY} L ${right} ${fillY} L ${right} ${bottom} L ${left} ${bottom} Z`;
  }
  const age = (performance.now() - ripple.start) / 1000;
  const decay = Math.max(0, 1 - age / 1.4);
  const amp = ripple.amp * decay;
  const wavelength = 16;
  const phase = age * 6;
  const segs = 12;
  let d = `M ${left} ${fillY + Math.sin((left / wavelength) * Math.PI * 2 + phase) * amp}`;
  for (let i = 1; i <= segs; i++) {
    const x = left + ((right - left) / segs) * i;
    const y = fillY + Math.sin((x / wavelength) * Math.PI * 2 + phase) * amp;
    d += ` L ${x} ${y}`;
  }
  d += ` L ${right} ${bottom} L ${left} ${bottom} Z`;
  return d;
}
