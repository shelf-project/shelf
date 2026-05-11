/** WebGL2 ambient backdrop — the "library reading-room glow" effect.
 *
 * What you see:
 *   - A slow, drifting caustic field tinted by the active theme's
 *     gradient tokens (`--bg-grad-1`, `--bg-grad-2`).
 *   - Every cache **hit** drops a soft expanding ripple at a
 *     deterministic-from-table-name screen position. The ring expands,
 *     fades, and dies; up to 6 active at any one time.
 *   - Every **miss** drops a subtle desaturated dimple — same shape,
 *     a third of the intensity, slightly blue-shifted.
 *
 * Why a shader and not CSS:
 *   - CSS can't do additive ripples that compose without z-index
 *     fighting (and can't do them at this density without jank).
 *   - The body's existing radial gradients stay underneath; the
 *     shader composites on top via `mix-blend-mode: plus-lighter` (dark
 *     theme) / `multiply` (light theme), so the ambient palette still
 *     reads as "shelf's brand colour" rather than "WebGL demo."
 *
 * Reduced-motion / WebGL-unsupported / off-screen:
 *   - `useReducedMotion` returns true → the canvas is unmounted and the
 *     body's existing radial gradients carry the ambient duty alone.
 *     Our `@media (prefers-reduced-motion)` rule in `styles.css` also
 *     hides the canvas defensively.
 *   - WebGL2 context creation fails → we render nothing; the body
 *     gradient is the fallback. No console error.
 *   - Off-screen (browser tab backgrounded) → `useIntersection` flips
 *     `enabled` to false, RAF stops, GPU goes idle.
 */

import { useEffect, useRef } from "react";
import { useReducedMotion } from "../hooks/useReducedMotion";
import { useIntersection } from "../hooks/useIntersection";
import { useGl } from "../hooks/useGl";
import type { CacheEvent } from "../api/metrics";

const FRAGMENT_SOURCE = /* glsl */ `#version 300 es
precision highp float;

in vec2 vUv;
out vec4 outColor;

uniform vec2  uRes;
uniform float uTime;
uniform vec3  uTint1;     // --bg-grad-1 sampled from CSS
uniform vec3  uTint2;     // --bg-grad-2 sampled from CSS
uniform float uTheme;     // 1.0 = light, 0.0 = dark
uniform float uRipples;   // count of active ripples [0,6]
uniform vec4  uR0;        // [x, y, ageSec, intensity]
uniform vec4  uR1;
uniform vec4  uR2;
uniform vec4  uR3;
uniform vec4  uR4;
uniform vec4  uR5;

/** A cheap 2D simplex-ish noise — enough variation for a slow drift
 *  without pulling in a 50-line classic-noise implementation. */
float hash(vec2 p) {
  p = fract(p * vec2(123.34, 456.21));
  p += dot(p, p + 45.32);
  return fract(p.x * p.y);
}
float noise(vec2 p) {
  vec2 i = floor(p);
  vec2 f = fract(p);
  vec2 u = f * f * (3.0 - 2.0 * f);
  return mix(
    mix(hash(i + vec2(0.0, 0.0)), hash(i + vec2(1.0, 0.0)), u.x),
    mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), u.x),
    u.y
  );
}

float caustic(vec2 uv, float t) {
  vec2 p = uv * 3.0;
  float n  = noise(p + vec2(0.0, t * 0.06));
  n += 0.5 * noise(p * 2.0 - vec2(t * 0.04, 0.0));
  n += 0.25 * noise(p * 4.0 + vec2(t * 0.02, t * 0.03));
  return n / 1.75;
}

float ripple(vec2 uv, vec4 r) {
  if (r.w <= 0.001) return 0.0;
  vec2 d = uv - r.xy;
  // Aspect-correct so the ring is a circle, not an ellipse.
  d.x *= uRes.x / uRes.y;
  float dist = length(d);
  // 1.4 s lifetime; expand from 0.0 to ~0.45 of viewport height.
  float t = clamp(r.z / 1.4, 0.0, 1.0);
  float radius = 0.45 * t;
  float ring   = 0.05 * (1.0 - t);
  // Soft donut.
  float intensity = smoothstep(radius - ring, radius, dist)
                  - smoothstep(radius, radius + ring, dist);
  return intensity * (1.0 - t) * r.w;
}

void main() {
  vec2 uv = vUv;
  float t = uTime;

  // Slow drifting caustic field, gentler in light mode so it doesn't
  // wash the page out.
  float c = caustic(uv, t);
  float ambient = mix(0.42, 0.28, uTheme);
  c = mix(ambient, ambient + 0.18, c);

  // Linear blend across two corner-tinted gradients reproduces the
  // body radial-gradient palette.
  float gx = uv.x;
  float gy = 1.0 - uv.y;
  vec3 base = mix(uTint1, uTint2, smoothstep(0.0, 1.0, gx + gy * 0.4));

  // Composite ripples additively. Up to 6 in flight; the shader runs
  // them all unconditionally because branching on a uniform-driven
  // count is no faster on most GPUs.
  float rsum = 0.0;
  if (uRipples > 0.5) rsum += ripple(uv, uR0);
  if (uRipples > 1.5) rsum += ripple(uv, uR1);
  if (uRipples > 2.5) rsum += ripple(uv, uR2);
  if (uRipples > 3.5) rsum += ripple(uv, uR3);
  if (uRipples > 4.5) rsum += ripple(uv, uR4);
  if (uRipples > 5.5) rsum += ripple(uv, uR5);

  vec3 col = base * c + rsum * vec3(0.65, 0.78, 1.0) * 0.45;

  // Light theme: hand off as RGB on a black canvas; mix-blend-mode in
  // CSS will composite multiplicatively. Dark theme: same RGB but
  // composited additively via plus-lighter.
  outColor = vec4(col, 1.0);
}`;

const MAX_RIPPLES = 6;

type Ripple = {
  x: number;
  y: number;
  bornAt: number;
  intensity: number;
};

type Props = {
  events: CacheEvent[];
};

export default function ShaderBackdrop({ events }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const reduce = useReducedMotion();
  const visible = useIntersection(wrapRef);
  const enabled = !reduce && visible;
  const handleRef = useGl(canvasRef, FRAGMENT_SOURCE, enabled);
  const ripplesRef = useRef<Ripple[]>([]);
  const themeRef = useRef<{ tint1: [number, number, number]; tint2: [number, number, number]; light: number }>({
    tint1: [0.07, 0.13, 0.18],
    tint2: [0.07, 0.12, 0.18],
    light: 0,
  });

  // Sample CSS custom properties so the shader composites onto the
  // brand palette. We read them on mount AND whenever the theme attr
  // changes (the theme.cycle() in App.tsx flips `data-theme` on <html>).
  useEffect(() => {
    if (typeof window === "undefined") return;
    const refresh = () => {
      const root = document.documentElement;
      const cs = window.getComputedStyle(root);
      const grad1 = parseColor(cs.getPropertyValue("--bg-grad-1"));
      const grad2 = parseColor(cs.getPropertyValue("--bg-grad-2"));
      const light = root.getAttribute("data-theme") === "light" ? 1 : 0;
      themeRef.current = { tint1: grad1, tint2: grad2, light };
    };
    refresh();
    const obs = new MutationObserver(refresh);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => obs.disconnect();
  }, []);

  // Convert each new event into a ripple. Determinism: a hash on the
  // table name picks a stable screen position so the same table always
  // ripples in the same place — repeated hits look like an active book.
  useEffect(() => {
    if (events.length === 0) return;
    const now = performance.now();
    const arr = ripplesRef.current.slice();
    for (const ev of events) {
      if (ev.kind !== "hit" && ev.kind !== "miss") continue;
      const h = hashName(ev.table);
      const x = 0.08 + 0.84 * fract(h * 0.61803398875);
      const y = 0.18 + 0.64 * fract(h * 0.41421356237 + 0.31);
      const intensity = ev.kind === "hit" ? Math.min(1, 0.55 + Math.log10(1 + ev.count) * 0.15) : 0.18;
      arr.push({ x, y, bornAt: now, intensity });
    }
    if (arr.length > MAX_RIPPLES) arr.splice(0, arr.length - MAX_RIPPLES);
    ripplesRef.current = arr;
  }, [events]);

  // Render loop — only runs when enabled.
  useEffect(() => {
    if (!enabled) return;
    let raf = 0;
    const loop = () => {
      const handle = handleRef.current;
      if (!handle) {
        raf = requestAnimationFrame(loop);
        return;
      }
      const now = performance.now();
      // Drop dead ripples in place.
      const live = ripplesRef.current.filter((r) => (now - r.bornAt) / 1000 < 1.4);
      ripplesRef.current = live;
      const flat: [number, number, number, number][] = [];
      for (let i = 0; i < MAX_RIPPLES; i++) {
        const r = live[i];
        if (!r) {
          flat.push([0, 0, 0, 0]);
        } else {
          const age = (now - r.bornAt) / 1000;
          flat.push([r.x, r.y, age, r.intensity]);
        }
      }
      handle.draw({
        uTint1: themeRef.current.tint1,
        uTint2: themeRef.current.tint2,
        uTheme: themeRef.current.light,
        uRipples: live.length,
        uR0: flat[0],
        uR1: flat[1],
        uR2: flat[2],
        uR3: flat[3],
        uR4: flat[4],
        uR5: flat[5],
      });
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [enabled, handleRef]);

  if (reduce) {
    // Reduced-motion: render nothing. The body's radial-gradient bg
    // already provides ambient light — see styles.css line 70-73.
    return null;
  }

  return (
    <div ref={wrapRef} className="gl-shader-backdrop" aria-hidden>
      <canvas ref={canvasRef} style={{ width: "100%", height: "100%", display: "block" }} />
    </div>
  );
}

function fract(x: number): number {
  return x - Math.floor(x);
}

function hashName(name: string): number {
  let h = 5381;
  for (let i = 0; i < name.length; i++) h = (h * 33) ^ name.charCodeAt(i);
  return Math.abs(h) % 1000003;
}

/** Parse `--bg-grad-1` into a `[r,g,b]` triplet in [0,1]. The token
 *  is set in `styles.css` to a hex literal (e.g. `#13202e`) — that's
 *  the only form we need to handle. */
function parseColor(raw: string): [number, number, number] {
  const trimmed = raw.trim();
  const m = /^#?([0-9a-fA-F]{6})$/.exec(trimmed.replace(/^#/, "").slice(0, 6));
  if (m) {
    const n = parseInt(m[1], 16);
    return [((n >> 16) & 0xff) / 255, ((n >> 8) & 0xff) / 255, (n & 0xff) / 255];
  }
  // Cheap rgb() fallback.
  const rgb = /rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/.exec(trimmed);
  if (rgb) {
    return [Number(rgb[1]) / 255, Number(rgb[2]) / 255, Number(rgb[3]) / 255];
  }
  return [0.08, 0.13, 0.19];
}
