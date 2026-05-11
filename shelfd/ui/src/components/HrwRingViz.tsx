/** HRW ring visualisation — Admin tab's signature overdrive moment.
 *
 * Renders the same data the existing `RingTable` shows (one row per
 * pod) as a literal SVG circle: each pod is an arc, sized
 * proportionally to its weight (HRW selection probability share).
 * The `self` pod's arc is highlighted in `--accent`; the others use
 * the brand neutral.
 *
 * Coupling with `RingTable` (skill principle 3 — one moment of
 * poetry, the rest stays calm):
 *   - The table remains the source of truth for numeric values.
 *   - The ring viz is the visual hint of "here's the cache as a
 *     physical place, you live at this slot."
 *   - When `pulseOnPodId` is set (driven by AdminTab when a pin /
 *     unpin / evict succeeds against a key whose HRW owner is that
 *     pod), the corresponding arc pulses once via the
 *     `--evict-glow @property` keyframe in `styles.css`.
 *   - Hover dolly-zooms the arc and synchronously highlights the
 *     same row in `RingTable` via `data-active`.
 *
 * No new metric, no new API call — the `getRing` payload already
 * carries everything we need (pod_id, weight, healthy).
 */

import { useEffect, useState } from "react";
import type { RingRow } from "../api/client";

type Props = {
  rows: RingRow[] | null;
  self: string | null;
  /** When set, pulse the matching arc once. Cleared by the parent
   *  after the keyframe fires. */
  pulseOnPodId?: string | null;
  /** Bidirectional hover: parent passes the table's hovered pod id
   *  in, the ring reports its own hover out. Either path lights both
   *  surfaces. */
  hoveredPodId?: string | null;
  onHover?: (podId: string | null) => void;
};

const SIZE = 240;
const RADIUS = 92;
const STROKE = 16;
const CENTER = SIZE / 2;

export default function HrwRingViz({
  rows,
  self,
  pulseOnPodId,
  hoveredPodId,
  onHover,
}: Props) {
  const [innerHover, setInnerHover] = useState<string | null>(null);
  const [pulsing, setPulsing] = useState<string | null>(null);

  // Pulse trigger: when the parent flips `pulseOnPodId` we set the
  // pulsing pod, then clear after the keyframe runs.
  useEffect(() => {
    if (!pulseOnPodId) return;
    setPulsing(pulseOnPodId);
    const t = window.setTimeout(() => setPulsing(null), 760);
    return () => window.clearTimeout(t);
  }, [pulseOnPodId]);

  if (!rows || rows.length === 0) {
    return (
      <svg className="hrw-ring" viewBox={`0 0 ${SIZE} ${SIZE}`} aria-label="HRW ring (empty)">
        <circle cx={CENTER} cy={CENTER} r={RADIUS} className="hrw-ring-frame" />
        <text x={CENTER} y={CENTER + 4} className="hrw-ring-label" textAnchor="middle">
          {rows ? "no ring members" : "loading…"}
        </text>
      </svg>
    );
  }

  const sorted = [...rows].sort((a, b) => a.pod_id.localeCompare(b.pod_id));
  const totalWeight = sorted.reduce((a, r) => a + Math.max(0, r.weight), 0) || 1;
  let cursor = -Math.PI / 2;
  const arcs = sorted.map((r) => {
    const span = (Math.max(0, r.weight) / totalWeight) * Math.PI * 2;
    const start = cursor;
    const end = cursor + span;
    cursor = end;
    return { row: r, start, end, span };
  });

  const externalHover = hoveredPodId ?? null;
  const activePod = innerHover ?? externalHover;

  return (
    <svg
      className="hrw-ring"
      viewBox={`0 0 ${SIZE} ${SIZE}`}
      role="img"
      aria-label="HRW ring members"
    >
      <circle cx={CENTER} cy={CENTER} r={RADIUS} className="hrw-ring-frame" />
      {arcs.map(({ row, start, end, span }) => {
        const isSelf = self !== null && row.pod_id === self;
        const isActive = activePod === row.pod_id;
        const path = arcPath(CENTER, CENTER, RADIUS, start, end);
        const tone = !row.healthy
          ? "var(--err)"
          : isSelf
          ? "var(--accent)"
          : "color-mix(in oklab, var(--accent) 22%, var(--bg-elev))";
        return (
          <path
            key={row.pod_id}
            className="hrw-ring-arc"
            d={path}
            stroke={tone}
            strokeWidth={STROKE}
            fill="none"
            strokeLinecap="round"
            data-pod={row.pod_id}
            data-self={isSelf || undefined}
            data-active={isActive || undefined}
            data-pulse={pulsing === row.pod_id || undefined}
            opacity={span < 0.05 ? 0 : isSelf ? 1 : 0.78}
            onMouseEnter={() => {
              setInnerHover(row.pod_id);
              onHover?.(row.pod_id);
            }}
            onMouseLeave={() => {
              setInnerHover(null);
              onHover?.(null);
            }}
          >
            <title>
              {row.pod_id} · weight {row.weight.toFixed(3)} · {row.healthy ? "healthy" : "unhealthy"}
              {isSelf ? " · self" : ""}
            </title>
          </path>
        );
      })}
      {/* Self marker dot in the centre — visual anchor "this is me". */}
      <circle cx={CENTER} cy={CENTER} r={4} className="hrw-ring-self-dot" />
      <text x={CENTER} y={CENTER + 22} className="hrw-ring-label" textAnchor="middle">
        {self ? "self" : "n/a"}
      </text>
    </svg>
  );
}

/** Build a stroked-arc SVG path from two angles (radians, 0 = +x). */
function arcPath(
  cx: number,
  cy: number,
  r: number,
  startA: number,
  endA: number,
): string {
  const startX = cx + r * Math.cos(startA);
  const startY = cy + r * Math.sin(startA);
  const endX = cx + r * Math.cos(endA);
  const endY = cy + r * Math.sin(endA);
  const largeArc = endA - startA > Math.PI ? 1 : 0;
  return `M ${startX} ${startY} A ${r} ${r} 0 ${largeArc} 1 ${endX} ${endY}`;
}
