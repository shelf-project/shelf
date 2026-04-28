import { useEffect, useRef, useState } from "react";

type Props = {
  value: number;
  /** Animation length in ms. */
  durationMs?: number;
  /** Format the tween frame into a display string. */
  format?: (n: number) => string;
  /** Skip animation below this absolute delta (saves paint when the
   * value barely moves). */
  threshold?: number;
};

/** Count-up animation for large numbers. Falls back to a direct
 * assignment when the user prefers reduced motion. */
export default function AnimatedNumber({
  value,
  durationMs = 600,
  format = (n) => Math.round(n).toString(),
  threshold = 1,
}: Props) {
  const [display, setDisplay] = useState(value);
  const fromRef = useRef(value);
  const startRef = useRef<number | null>(null);
  const rafRef = useRef<number | null>(null);

  useEffect(() => {
    if (Math.abs(value - display) < threshold) {
      setDisplay(value);
      return;
    }
    const prefersReduced = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced) {
      setDisplay(value);
      return;
    }
    fromRef.current = display;
    startRef.current = null;
    const tick = (ts: number) => {
      if (startRef.current === null) startRef.current = ts;
      const p = Math.min(1, (ts - startRef.current) / durationMs);
      const eased = 1 - Math.pow(1 - p, 3);
      setDisplay(fromRef.current + (value - fromRef.current) * eased);
      if (p < 1) rafRef.current = window.requestAnimationFrame(tick);
    };
    rafRef.current = window.requestAnimationFrame(tick);
    return () => {
      if (rafRef.current !== null) window.cancelAnimationFrame(rafRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value, durationMs, threshold]);

  return <>{format(display)}</>;
}
