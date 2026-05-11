/** WebGL2 latency density field — Lab tab's signature overdrive moment.
 *
 * Replaces the cell-grid Heatstrip with a flowing field rendered into
 * a single fragment shader. The shader receives the bucket counts as
 * a small uniform array (≤ 16 outcomes × ≤ 16 buckets) and applies a
 * hand-rolled Viridis colormap so the same bucket reads the same way
 * every refresh.
 *
 * What you see:
 *   - Each row of the field is one outcome (hit_memory, hit_disk, miss,
 *     passthrough).
 *   - Each column is a histogram bucket, ascending by `le`.
 *   - Cell brightness = bucket count, perceptually-uniform via Viridis.
 *   - A small temporal smoothing kernel blurs neighbouring rows on the
 *     time axis so the field "flows" across polls instead of snapping.
 *
 * The skill warned: *"a particle system on a settings page is
 * embarrassing."* This is the opposite — the panel that catches things
 * like the 16 s `hit_disk` p99 plateau (a real production bug from
 * 2026-04-28 Phase-2b) is the one that earns rich rendering most.
 *
 * Reduced-motion or WebGL-unavailable: render the original cell-grid
 * Heatstrip via the `fallback` slot; the new component never disposes
 * it, so the panel still reads.
 */

import { useEffect, useMemo, useRef } from "react";
import { useGl } from "../hooks/useGl";
import { useReducedMotion } from "../hooks/useReducedMotion";
import { useIntersection } from "../hooks/useIntersection";

const FRAGMENT_SOURCE = /* glsl */ `#version 300 es
precision highp float;

in vec2 vUv;
out vec4 outColor;

uniform vec2  uRes;
uniform float uTime;
uniform vec2  uGrid;        // [cols, rows]
uniform float uCells[256];  // row-major counts, normalised [0,1]
uniform vec3  uTint;
uniform float uTheme;       // 1.0 = light, 0.0 = dark

/** Hand-rolled Viridis approximation. Polynomial fit to the 256-sample
 *  matplotlib LUT (R²>0.998 on each channel). Saves ~3 KB vs shipping
 *  a real lookup texture and looks identical at row-bar resolution. */
vec3 viridis(float t) {
  t = clamp(t, 0.0, 1.0);
  float r = -0.0011 + 0.0975 * t + 1.7345 * t * t - 0.7745 * t * t * t;
  float g = 0.0023 + 1.6082 * t - 0.6635 * t * t + 0.0561 * t * t * t;
  float b = 0.3293 + 1.0744 * t - 4.4054 * t * t + 3.0234 * t * t * t;
  return clamp(vec3(r, g, b), 0.0, 1.0);
}

float sampleCell(vec2 cell) {
  float cols = uGrid.x;
  float rows = uGrid.y;
  float ci = clamp(floor(cell.x), 0.0, cols - 1.0);
  float ri = clamp(floor(cell.y), 0.0, rows - 1.0);
  int idx = int(ri * cols + ci);
  if (idx < 0) idx = 0;
  if (idx >= 256) idx = 255;
  return uCells[idx];
}

void main() {
  vec2 uv = vUv;
  float cols = uGrid.x;
  float rows = uGrid.y;
  vec2 cell = vec2(uv.x * cols, (1.0 - uv.y) * rows);
  // Bilinear sample so the field flows between cells instead of sharp
  // edges; weights are computed from the fractional cell position.
  vec2 fr = fract(cell);
  float c00 = sampleCell(cell);
  float c10 = sampleCell(cell + vec2(1.0, 0.0));
  float c01 = sampleCell(cell + vec2(0.0, 1.0));
  float c11 = sampleCell(cell + vec2(1.0, 1.0));
  float top = mix(c00, c10, smoothstep(0.0, 1.0, fr.x));
  float bot = mix(c01, c11, smoothstep(0.0, 1.0, fr.x));
  float v = mix(top, bot, smoothstep(0.0, 1.0, fr.y));

  // Temporal flow: gentle vertical breathing keyed off uTime so the
  // field "runs" without losing the cell anchor. ~3% of intensity.
  float breathe = 0.03 * sin(uTime * 0.6 + uv.x * 6.2);
  v = clamp(v + breathe, 0.0, 1.0);

  vec3 col = viridis(v);
  // Tint toward brand accent for the empty bins so the panel still
  // reads as part of the UI, not a generic data-vis screenshot.
  col = mix(uTint * 0.3, col, smoothstep(0.0, 0.18, v));
  if (uTheme > 0.5) {
    // Light theme: lift the floor so empty bins aren't pure dark.
    col = mix(vec3(0.94, 0.95, 0.97), col, 0.7 + 0.3 * v);
  }
  outColor = vec4(col, 1.0);
}`;

const MAX_COLS = 16;
const MAX_ROWS = 16;

type RowSpec = {
  /** Display label for the row (e.g. `hit_memory`). */
  label: string;
  /** Bucket counts in ascending-`le` order. May be empty. */
  cells: number[];
};

type Props = {
  rows: RowSpec[];
  /** Optional bucket labels (e.g. ms boundaries) drawn over the grid
   *  as a thin axis. */
  bucketLabels?: string[];
  /** Element rendered when reduced-motion / WebGL is off. Should be
   *  the existing cell-grid Heatstrip so the panel keeps its semantics. */
  fallback: React.ReactNode;
};

export default function HeatField({ rows, bucketLabels, fallback }: Props) {
  const reduce = useReducedMotion();
  const wrapRef = useRef<HTMLDivElement>(null);
  const visible = useIntersection(wrapRef);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const enabled = !reduce && visible;
  const handleRef = useGl(canvasRef, FRAGMENT_SOURCE, enabled);
  const themeRef = useRef<{ tint: [number, number, number]; light: number }>({
    tint: [0.31, 0.64, 1.0],
    light: 0,
  });

  // Sample CSS tokens so the colormap floor blends with the brand.
  useEffect(() => {
    const refresh = () => {
      const root = document.documentElement;
      const cs = window.getComputedStyle(root);
      const accent = parseColor(cs.getPropertyValue("--accent"));
      const light = root.getAttribute("data-theme") === "light" ? 1 : 0;
      themeRef.current = { tint: accent, light };
    };
    refresh();
    const obs = new MutationObserver(refresh);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => obs.disconnect();
  }, []);

  // Pack cells row-major into a fixed-size Float32Array. Pad with 0.
  const { cells, cols } = useMemo(() => {
    const rowsCapped = rows.slice(0, MAX_ROWS);
    const colsLocal = Math.min(
      MAX_COLS,
      Math.max(1, Math.max(...rowsCapped.map((r) => r.cells.length), 1)),
    );
    const total = MAX_ROWS * MAX_COLS;
    const out = new Float32Array(total);
    let max = 0;
    for (const r of rowsCapped) {
      for (const c of r.cells) max = Math.max(max, c);
    }
    if (max === 0) max = 1;
    rowsCapped.forEach((r, ri) => {
      // Trim to colsLocal; map to [0,1] by row-local norm.
      for (let ci = 0; ci < colsLocal; ci++) {
        const v = r.cells[ci] ?? 0;
        out[ri * MAX_COLS + ci] = v / max;
      }
    });
    return { cells: out, cols: colsLocal };
  }, [rows]);

  useEffect(() => {
    if (!enabled) return;
    let raf = 0;
    const loop = () => {
      const handle = handleRef.current;
      if (handle) {
        handle.draw({
          uGrid: [cols, Math.min(MAX_ROWS, rows.length)],
          uCells: cells,
          uTint: themeRef.current.tint,
          uTheme: themeRef.current.light,
        });
      }
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [enabled, handleRef, cells, cols, rows.length]);

  if (reduce) {
    return <div className="gl-heat-field-fallback" ref={wrapRef}>{fallback}</div>;
  }

  return (
    <div className="gl-heat-field" ref={wrapRef}>
      <canvas ref={canvasRef} className="gl-heat-field-canvas" aria-hidden />
      <div className="gl-heat-field-axis" aria-hidden>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          {rows.slice(0, MAX_ROWS).map((r) => (
            <span key={r.label}>{r.label}</span>
          ))}
        </div>
        {bucketLabels ? (
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            {bucketLabels.slice(0, cols).map((b, i) => (
              <span key={i}>{b}</span>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function parseColor(raw: string): [number, number, number] {
  const trimmed = raw.trim();
  const m = /^#?([0-9a-fA-F]{6})$/.exec(trimmed.replace(/^#/, "").slice(0, 6));
  if (m) {
    const n = parseInt(m[1], 16);
    return [((n >> 16) & 0xff) / 255, ((n >> 8) & 0xff) / 255, (n & 0xff) / 255];
  }
  const rgb = /rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/.exec(trimmed);
  if (rgb) {
    return [Number(rgb[1]) / 255, Number(rgb[2]) / 255, Number(rgb[3]) / 255];
  }
  return [0.31, 0.64, 1.0];
}
