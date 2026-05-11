/** Subscribe to the cache-event stream derived from polled `/metrics`.
 *
 * The shader backdrop, the Canvas physics bookshelf, and the trophy-dot
 * particle burst all want the same answer: *"what just happened in the
 * cache between the last poll and this one?"* That answer is in `parseMetrics`
 * deltas, but each consumer maintaining its own diffing state would
 * produce inconsistent buffers (one component might miss an event because
 * its effect ran a tick later). This hook centralises the bookkeeping.
 *
 * Returns:
 *   - `events`: the events from the most recent poll (rebound on each
 *     successful fetch). Subscribers RAF off this to schedule effects.
 *   - `recent`: a bounded ring of the last `cap` events for components
 *     that want a feed-style view (NowServing, Bookshelf admission anim).
 *
 * The hook is poll-cadence-agnostic — it just consumes the latest
 * `Sample[]` from `parseMetrics(text)` and the shared `usePolled` tick.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { CacheEvent, deriveEvents, emptyEventStream } from "../api/metrics";
import { Sample } from "../api/metrics";
import { usePolled } from "../polling";
import { getMetricsText } from "../api/client";
import { parseMetrics } from "../api/metrics";

type EventStreamOptions = {
  /** Bounded ring buffer length for the `recent` view. */
  cap?: number;
};

/** Hook variant that fetches `/metrics`, parses, and emits events.
 *  Use this from callers that DON'T already have a parsed series. */
export function useEventStream(opts: EventStreamOptions = {}): {
  series: Sample[];
  events: CacheEvent[];
  recent: CacheEvent[];
} {
  const { cap = 24 } = opts;
  const { data: text } = usePolled(getMetricsText);
  const series = useMemo(() => (text ? parseMetrics(text) : []), [text]);
  const { events, recent } = useDeriveEvents(series, cap);
  return { series, events, recent };
}

/** Hook variant that takes a pre-parsed `Sample[]`. Use this when the
 *  caller already computes `series = parseMetrics(text)` (e.g. StoryTab
 *  derives a dozen scalars from it; re-parsing in this hook would
 *  double the cost). */
export function useDeriveEvents(
  series: Sample[],
  cap = 24,
): { events: CacheEvent[]; recent: CacheEvent[] } {
  const stateRef = useRef(emptyEventStream());
  const [events, setEvents] = useState<CacheEvent[]>([]);
  const [recent, setRecent] = useState<CacheEvent[]>([]);

  useEffect(() => {
    if (series.length === 0) return;
    const next = deriveEvents(series, stateRef.current);
    if (next.length === 0) {
      setEvents([]);
      return;
    }
    setEvents(next);
    setRecent((prev) => [...next.slice().reverse(), ...prev].slice(0, cap));
  }, [series, cap]);

  return { events, recent };
}
