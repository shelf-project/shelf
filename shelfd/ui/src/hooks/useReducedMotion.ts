/** Single source of truth for `prefers-reduced-motion`.
 *
 * Replaces the duplicated checks in `useSpring.ts` and `AnimatedNumber.tsx`
 * and lets every overdrive-pass effect (the WebGL backdrop, the Canvas
 * physics bookshelf, the View Transitions, the heat field) gate itself
 * through one consistent hook that *also* updates live when the user
 * flips the OS toggle without reloading.
 *
 * Per `.impeccable.md` design principle 4: reduced-motion is a first-class
 * theme, not a fallback. Callers branch on the boolean and render a
 * hand-tuned static counterpart, never an absence.
 *
 * The hook also honours `?plain=1` (or `#...?plain=1`) — the skill's
 * "removal test" knob. Every overdrive moment already checks
 * `useReducedMotion`, so routing the flag through this single hook is
 * enough to disable shader backdrop, Canvas physics bookshelf, capacity
 * vessel ripples, particle bursts, and heat-field WebGL in one shot.
 */

import { useEffect, useState } from "react";

const QUERY = "(prefers-reduced-motion: reduce)";

function isPlainUrl(): boolean {
  if (typeof window === "undefined") return false;
  const search = window.location.search.replace(/^\?/, "");
  const hashQuery = window.location.hash.split("?", 2)[1] ?? "";
  return /(^|&)plain=1(&|$)/.test(search) || /(^|&)plain=1(&|$)/.test(hashQuery);
}

export function useReducedMotion(): boolean {
  const [reduce, setReduce] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return isPlainUrl() || window.matchMedia(QUERY).matches;
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (isPlainUrl()) {
      setReduce(true);
      return;
    }
    const mql = window.matchMedia(QUERY);
    const onChange = (e: MediaQueryListEvent) => setReduce(e.matches);
    if (mql.addEventListener) {
      mql.addEventListener("change", onChange);
      return () => mql.removeEventListener("change", onChange);
    }
    mql.addListener(onChange);
    return () => mql.removeListener(onChange);
  }, []);

  return reduce;
}
