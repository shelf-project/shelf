import { useEffect, useRef, useState, type MutableRefObject } from "react";

/** Poll `fetcher` on a fixed cadence. Visibility-aware: pauses while
 * the tab is hidden so we don't hammer shelfd from a background
 * window. Returns `{ data, error, loading }` and a ref to the most
 * recent value so callers can implement derived state (e.g. rate
 * counters) without re-rendering on every tick.
 */
export function usePoll<T>(
  fetcher: () => Promise<T>,
  intervalMs: number,
): { data: T | null; error: string | null; loading: boolean; last: MutableRefObject<T | null> } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const last = useRef<T | null>(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;

    const tick = async () => {
      try {
        const v = await fetcherRef.current();
        if (cancelled) return;
        last.current = v;
        setData(v);
        setError(null);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    const schedule = () => {
      timer = window.setTimeout(async () => {
        if (document.visibilityState === "visible") {
          await tick();
        }
        if (!cancelled) schedule();
      }, intervalMs);
    };

    tick();
    schedule();

    const onVis = () => {
      if (document.visibilityState === "visible") tick();
    };
    document.addEventListener("visibilitychange", onVis);

    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, [intervalMs]);

  return { data, error, loading, last };
}
