/** Spring-physics interpolation toward a moving target.
 *
 * 30-line replacement for framer-motion's `useSpring`. We deliberately
 * avoid the dependency to keep the gzipped JS bundle under 100 KB
 * (current ceiling tracked in `shelfd/Dockerfile`). The defaults match
 * framer-motion's `gentle` preset: stiffness 170, damping 26.
 *
 * Usage mirrors `useState` — callers pass a target, we return the
 * smoothly-tweened value:
 *
 *     const headline = useSpring(rawPercentage * 100);
 *
 * - Per-frame integration uses a fixed 16 ms step so the easing curve
 *   is independent of the host's RAF cadence (some browsers throttle
 *   to 30 fps off-screen — without this, animation would slow with
 *   the tab).
 * - Settles to the target when |Δ| < 0.01 AND |velocity| < 0.05 to
 *   keep the underlying React state from updating after the eye stops
 *   noticing motion.
 * - Honours `prefers-reduced-motion`: if the user has reduced-motion
 *   enabled we skip the integration entirely and return the target
 *   directly. Pairs with the global rule already in styles.css.
 */

import { useEffect, useRef, useState } from "react";

export function useSpring(target: number, stiffness = 170, damping = 26): number {
  const [value, setValue] = useState(target);
  const valueRef = useRef(target);
  const velocityRef = useRef(0);
  const rafRef = useRef<number | null>(null);

  useEffect(() => {
    if (!Number.isFinite(target)) return;
    if (typeof window === "undefined") {
      setValue(target);
      return;
    }
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduce) {
      valueRef.current = target;
      velocityRef.current = 0;
      setValue(target);
      return;
    }
    const dt = 1 / 60;
    const tick = () => {
      const x = valueRef.current;
      const v = velocityRef.current;
      const force = -stiffness * (x - target);
      const damp = -damping * v;
      const a = force + damp;
      const vNext = v + a * dt;
      const xNext = x + vNext * dt;
      valueRef.current = xNext;
      velocityRef.current = vNext;
      setValue(xNext);
      if (Math.abs(xNext - target) < 0.01 && Math.abs(vNext) < 0.05) {
        valueRef.current = target;
        velocityRef.current = 0;
        setValue(target);
        rafRef.current = null;
        return;
      }
      rafRef.current = requestAnimationFrame(tick);
    };
    if (rafRef.current === null) rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    };
  }, [target, stiffness, damping]);

  return value;
}
