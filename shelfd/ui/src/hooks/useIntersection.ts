/** Pause heavy renderers when the surface is off-screen.
 *
 * The overdrive skill is explicit: *"Pause off-screen rendering. Kill
 * what you can't see."* The shader backdrop, the Canvas physics
 * bookshelf, the WebGL heat field, and the particle-burst helper all
 * check this hook to decide whether to schedule the next RAF. When the
 * user switches tabs (browser tab, not our tabs row), they all fall
 * silent.
 *
 * Returns `true` when the ref'd element has any portion of its bounding
 * box intersecting the viewport. Defaults to `true` before the observer
 * has run so first-paint isn't gated on the next animation frame.
 */

import { RefObject, useEffect, useState } from "react";

export function useIntersection<T extends Element>(
  ref: RefObject<T>,
  rootMargin = "64px",
): boolean {
  const [visible, setVisible] = useState(true);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const el = ref.current;
    if (!el || typeof IntersectionObserver === "undefined") return;
    const obs = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) setVisible(entry.isIntersecting);
      },
      { rootMargin, threshold: 0 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [ref, rootMargin]);

  // Browser-tab visibility is a separate axis: `document.hidden` flips
  // to true when the user backgrounds the entire window. We collapse it
  // into the same boolean so callers don't repeat the wiring.
  useEffect(() => {
    if (typeof document === "undefined") return;
    const onVis = () => {
      if (document.hidden) setVisible(false);
      else if (ref.current) {
        const rect = ref.current.getBoundingClientRect();
        const inView =
          rect.bottom > 0 &&
          rect.right > 0 &&
          rect.top < (window.innerHeight || 0) &&
          rect.left < (window.innerWidth || 0);
        setVisible(inView);
      }
    };
    document.addEventListener("visibilitychange", onVis);
    return () => document.removeEventListener("visibilitychange", onVis);
  }, [ref]);

  return visible;
}
