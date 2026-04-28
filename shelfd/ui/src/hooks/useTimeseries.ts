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

/** Vector-valued counterpart of [`useTimeseries`]: tracks a set of
 * keyed counters and returns a Map<key, {levels, deltas, rate}>.
 *
 * Used by the Hot tables leaderboard (per-table hits/min sparkline)
 * and the Lab tab admission-decision histogram. The bookkeeping is
 * kept inside one ref so we don't trigger a render per child key.
 *
 * Keys that disappear from a poll cycle are dropped on the next
 * insert — we deliberately don't carry stale series forward, because
 * a freshly-emptied table should drain its sparkline rather than
 * pretend the last cold count is still flowing. */
export type Series = { levels: number[]; deltas: number[]; rate: number | null };

export function useTimeseriesByKey(
  rows: Array<{ key: string; value: number }> | null | undefined,
  window = 60,
): Map<string, Series> {
  const lastValues = useRef<Map<string, number>>(new Map());
  const lastTimes = useRef<Map<string, number>>(new Map());
  const histories = useRef<Map<string, Series>>(new Map());
  const [, force] = useState(0);

  useEffect(() => {
    if (!rows) return;
    const now = Date.now();
    const seen = new Set<string>();
    for (const r of rows) {
      seen.add(r.key);
      const prev = lastValues.current.get(r.key);
      const prevT = lastTimes.current.get(r.key);
      lastValues.current.set(r.key, r.value);
      lastTimes.current.set(r.key, now);
      const hist = histories.current.get(r.key) ?? {
        levels: [],
        deltas: [],
        rate: null,
      };
      const nextLevels = [...hist.levels, r.value];
      if (nextLevels.length > window) nextLevels.splice(0, nextLevels.length - window);
      let nextDeltas = hist.deltas;
      let nextRate = hist.rate;
      if (prev !== undefined && prevT !== undefined) {
        const dv = Math.max(0, r.value - prev);
        const dt = Math.max(1, now - prevT) / 1000;
        nextDeltas = [...hist.deltas, dv];
        if (nextDeltas.length > window) nextDeltas.splice(0, nextDeltas.length - window);
        nextRate = dv / dt;
      }
      histories.current.set(r.key, {
        levels: nextLevels,
        deltas: nextDeltas,
        rate: nextRate,
      });
    }
    // Drop keys the latest poll did not see — keeps the map bounded
    // when, e.g., a table stops appearing in the cardinality cap.
    for (const k of Array.from(histories.current.keys())) {
      if (!seen.has(k)) {
        histories.current.delete(k);
        lastValues.current.delete(k);
        lastTimes.current.delete(k);
      }
    }
    force((n) => n + 1);
  }, [rows, window]);

  return histories.current;
}
