/**
 * App-wide polling context.
 *
 * Why: Ops and Showcase both poll the same endpoints. Rolling their
 * cadence into one context means every subscriber ticks in lockstep
 * — the "hit rate" on Ops never drifts from the "cumulative hits"
 * counter on Showcase because they read the same fetch.
 *
 * Children hook `usePolled(fetcher)` and re-fetch on every shared
 * tick. The header surfaces `lastSuccess` as a freshness indicator,
 * and `paused` / `togglePaused` drives the pause button + the `p`
 * keyboard shortcut.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

export type PollingCtx = {
  tick: number;
  paused: boolean;
  intervalMs: number;
  setPaused: (v: boolean) => void;
  togglePaused: () => void;
  tickNow: () => void;
  lastSuccess: number | null;
  reportSuccess: () => void;
};

const Ctx = createContext<PollingCtx | null>(null);

type Props = { children: ReactNode; intervalMs?: number };

export function PollingProvider({ children, intervalMs = 5000 }: Props) {
  const [tick, setTick] = useState(0);
  const [paused, setPaused] = useState(false);
  const [lastSuccess, setLastSuccess] = useState<number | null>(null);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  const bump = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (!pausedRef.current && document.visibilityState === "visible") {
        bump();
      }
    }, intervalMs);
    const onVis = () => {
      if (document.visibilityState === "visible" && !pausedRef.current) bump();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      window.clearInterval(interval);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, [intervalMs, bump]);

  const reportSuccess = useCallback(() => setLastSuccess(Date.now()), []);
  const togglePaused = useCallback(() => setPaused((p) => !p), []);

  const value = useMemo<PollingCtx>(
    () => ({
      tick,
      paused,
      intervalMs,
      setPaused,
      togglePaused,
      tickNow: bump,
      lastSuccess,
      reportSuccess,
    }),
    [tick, paused, intervalMs, togglePaused, bump, lastSuccess, reportSuccess],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function usePolling(): PollingCtx {
  const v = useContext(Ctx);
  if (!v) throw new Error("usePolling() requires <PollingProvider>");
  return v;
}

/** Subscribe to the shared tick. Calls `fetcher()` once on mount and
 * on every tick bump. Reports success back to the polling context so
 * the header freshness indicator updates. */
export function usePolled<T>(
  fetcher: () => Promise<T>,
): { data: T | null; error: string | null; loading: boolean } {
  const { tick, reportSuccess } = usePolling();
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const v = await fetcherRef.current();
        if (cancelled) return;
        setData(v);
        setError(null);
        reportSuccess();
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tick, reportSuccess]);

  return { data, error, loading };
}
