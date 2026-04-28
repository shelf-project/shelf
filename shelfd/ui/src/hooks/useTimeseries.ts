import { useEffect, useRef, useState } from "react";

/** Keep a bounded history of a scalar signal and a rate-of-change
 * derivative (per second). Useful for driving sparklines and
 * "hits/sec" style displays off a cumulative counter.
 *
 * - `window` is the number of samples to retain. At 5 s poll cadence,
 *   60 samples ≈ 5 min of history.
 * - If the raw value resets (counter reset on restart), the derived
 *   rate is clamped at zero instead of going negative. */
export function useTimeseries(
  value: number | null | undefined,
  window = 60,
): { levels: number[]; deltas: number[]; rate: number | null } {
  const [levels, setLevels] = useState<number[]>([]);
  const [deltas, setDeltas] = useState<number[]>([]);
  const [rate, setRate] = useState<number | null>(null);
  const lastValueRef = useRef<number | null>(null);
  const lastTimeRef = useRef<number | null>(null);

  useEffect(() => {
    if (value === null || value === undefined || !Number.isFinite(value)) return;
    const now = Date.now();
    const prevV = lastValueRef.current;
    const prevT = lastTimeRef.current;
    lastValueRef.current = value;
    lastTimeRef.current = now;

    setLevels((prev) => {
      const next = [...prev, value];
      return next.length > window ? next.slice(-window) : next;
    });

    if (prevV === null || prevT === null) return;
    const dv = Math.max(0, value - prevV);
    const dt = Math.max(1, now - prevT) / 1000;
    setDeltas((prev) => {
      const next = [...prev, dv];
      return next.length > window ? next.slice(-window) : next;
    });
    setRate(dv / dt);
  }, [value, window]);

  return { levels, deltas, rate };
}
