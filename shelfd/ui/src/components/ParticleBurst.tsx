/** Tiny Canvas2D particle burst — shared across Hot tab + Lab tab.
 *
 * Render order:
 *   - Parent provides a `bursts` array of `{ id, x, y, color, ts }`
 *     anchors (relative to the parent's bounding box).
 *   - Component owns one `<canvas>` overlay sized to its parent (the
 *     parent must be `position: relative`); each anchor seeds 12
 *     particles that emit outward on a randomised vector and fade out
 *     over ~280 ms.
 *   - `prefers-reduced-motion` → render nothing. The skill's
 *     "decoration" rule: a burst is a celebration, not a metric. If
 *     the user has reduced-motion on, we owe them silence.
 *
 * Performance:
 *   - Particle count is bounded — at most `bursts.length * 12` live
 *     at once. With <= 4 simultaneous bursts that's 48 sprites, no
 *     concern at 60 fps even on a phone.
 *   - The RAF loop self-stops when the last particle dies, so an idle
 *     leaderboard pays zero cost.
 */

import { useEffect, useRef } from "react";
import { useReducedMotion } from "../hooks/useReducedMotion";

export type Burst = {
  id: string;
  x: number; // px relative to the canvas top-left
  y: number;
  color: string; // CSS colour, e.g. `var(--ok)` or `#ffffff`
  ts: number;
};

type Props = {
  bursts: Burst[];
};

type Particle = {
  x: number;
  y: number;
  vx: number;
  vy: number;
  bornAt: number;
  color: string;
};

const LIFE_MS = 320;
const PARTICLES_PER_BURST = 12;

export default function ParticleBurst({ bursts }: Props) {
  const reduce = useReducedMotion();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const particlesRef = useRef<Particle[]>([]);
  const seenRef = useRef<Set<string>>(new Set());

  // Spawn particles for any newly-arrived burst.
  useEffect(() => {
    if (reduce) return;
    for (const b of bursts) {
      if (seenRef.current.has(b.id)) continue;
      seenRef.current.add(b.id);
      const now = performance.now();
      for (let i = 0; i < PARTICLES_PER_BURST; i++) {
        const angle = (i / PARTICLES_PER_BURST) * Math.PI * 2 + Math.random() * 0.4;
        const speed = 80 + Math.random() * 60;
        particlesRef.current.push({
          x: b.x,
          y: b.y,
          vx: Math.cos(angle) * speed,
          vy: Math.sin(angle) * speed - 30, // slight upward bias
          bornAt: now,
          color: b.color,
        });
      }
    }
    // Drop stale ids so the set stays bounded.
    if (seenRef.current.size > 64) {
      seenRef.current = new Set(bursts.map((b) => b.id));
    }
  }, [bursts, reduce]);

  useEffect(() => {
    if (reduce) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const resize = () => {
      const w = Math.floor(canvas.clientWidth * dpr);
      const h = Math.floor(canvas.clientHeight * dpr);
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width = w;
        canvas.height = h;
      }
    };
    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(canvas);

    let last = performance.now();
    let raf = 0;
    const loop = (t: number) => {
      const dt = Math.min(0.05, (t - last) / 1000);
      last = t;
      const live: Particle[] = [];
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      for (const p of particlesRef.current) {
        const age = t - p.bornAt;
        if (age >= LIFE_MS) continue;
        // Light gravity so the upward bias decays into a fall.
        p.vy += 220 * dt;
        p.x += p.vx * dt;
        p.y += p.vy * dt;
        live.push(p);
        const lifeFrac = age / LIFE_MS;
        const alpha = 1 - lifeFrac;
        const radius = 2.4 * (1 - lifeFrac * 0.6) * dpr;
        ctx.globalAlpha = alpha;
        ctx.fillStyle = p.color;
        ctx.beginPath();
        ctx.arc(p.x * dpr, p.y * dpr, radius, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.globalAlpha = 1;
      particlesRef.current = live;
      if (live.length === 0) {
        raf = 0;
        return;
      }
      raf = requestAnimationFrame(loop);
    };
    // Restart loop whenever new bursts come in.
    if (particlesRef.current.length > 0) {
      raf = requestAnimationFrame(loop);
    }
    const id = window.setInterval(() => {
      if (raf === 0 && particlesRef.current.length > 0) {
        last = performance.now();
        raf = requestAnimationFrame(loop);
      }
    }, 100);
    return () => {
      cancelAnimationFrame(raf);
      window.clearInterval(id);
      ro.disconnect();
    };
  }, [reduce, bursts]);

  if (reduce) return null;
  return <canvas ref={canvasRef} className="particle-burst-canvas" aria-hidden />;
}
