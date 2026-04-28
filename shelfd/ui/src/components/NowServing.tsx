/** "Now serving" — a faux-realtime activity feed driven by /metrics polls.
 *
 * The pattern is the most "alive-feeling" element on a dashboard
 * (Stripe, Vercel, Stream, Sentry all use it). We don't have an SSE
 * channel from `shelfd`, but we can derive an honest feed from
 * successive `/metrics` snapshots at the existing 5 s poll cadence:
 * every counter delta is a real event that just happened, batched
 * into 5 s windows. We render those batches as fading-in chips.
 *
 * Honest constraints we keep:
 *   - One chip per (table, kind, poll), so the rate the user sees is
 *     literally the rate the metric reported.
 *   - Empty-state copy: "Waiting for traffic — fire a query." No
 *     placeholder events, no fake activity.
 *   - The pulse on the leading dot is the only colour-only cue, and
 *     it pairs with the chip's `kind=hit/miss` text — accessible per
 *     the AGENTS.md design rules.
 *   - Bounded ring buffer (12 entries) — never grows.
 *
 * Inputs are intentionally low-coupling: just the latest pair of
 * label-keyed series (hits + misses by table). The component owns
 * its own buffer because the parent shouldn't have to manage feed
 * state for a presentational widget.
 */

import { useEffect, useRef, useState } from "react";

type SeriesRow = { key: string; labels: Record<string, string>; value: number };

type FeedEvent = {
  id: string;
  ts: number;
  table: string;
  pool: string;
  kind: "hit" | "miss";
  count: number;
};

type Props = {
  hitsByTable: SeriesRow[];
  missesByTable: SeriesRow[];
  /** Cap on retained events. The last `cap` chips are visible; older
   * chips fade out. Defaults to 12, the empirical sweet spot for
   * "alive but not noisy" on a 5 s cadence. */
  cap?: number;
};

export default function NowServing({
  hitsByTable,
  missesByTable,
  cap = 12,
}: Props) {
  const [events, setEvents] = useState<FeedEvent[]>([]);
  const lastHits = useRef<Map<string, number>>(new Map());
  const lastMisses = useRef<Map<string, number>>(new Map());
  const seq = useRef(0);

  useEffect(() => {
    const now = Date.now();
    const newEvents: FeedEvent[] = [];

    const ingest = (
      rows: SeriesRow[],
      memo: Map<string, number>,
      kind: "hit" | "miss",
    ) => {
      for (const r of rows) {
        const prev = memo.get(r.key);
        memo.set(r.key, r.value);
        if (prev === undefined) continue; // first sight, no event
        const delta = r.value - prev;
        if (delta <= 0) continue;
        const table = r.labels["table"] ?? "?";
        const pool = r.labels["pool"] ?? "?";
        // Skip the "other" cardinality-overflow bucket — it's a
        // bookkeeping artefact, not an interesting story event.
        if (table === "other") continue;
        seq.current += 1;
        newEvents.push({
          id: `${now}-${seq.current}`,
          ts: now,
          table,
          pool,
          kind,
          count: Math.round(delta),
        });
      }
      // Drop keys absent in this poll so memo stays bounded.
      const seen = new Set(rows.map((r) => r.key));
      for (const k of Array.from(memo.keys())) {
        if (!seen.has(k)) memo.delete(k);
      }
    };

    ingest(hitsByTable, lastHits.current, "hit");
    ingest(missesByTable, lastMisses.current, "miss");

    if (newEvents.length === 0) return;
    setEvents((prev) => {
      // Newest first; cap retained events.
      const merged = [...newEvents.reverse(), ...prev];
      return merged.slice(0, cap);
    });
  }, [hitsByTable, missesByTable, cap]);

  if (events.length === 0) {
    return (
      <section className="now-serving now-serving-empty card">
        <h3 className="card-title">Now serving</h3>
        <p className="now-serving-help">
          Waiting for traffic — fire a query and the cache events will
          stream here.
        </p>
      </section>
    );
  }

  return (
    <section className="now-serving card">
      <div className="now-serving-head">
        <h3 className="card-title" style={{ margin: 0 }}>
          Now serving
        </h3>
        <span className="now-serving-pulse" aria-hidden />
      </div>
      <ul className="now-serving-list" aria-live="polite">
        {events.map((e) => (
          <li
            key={e.id}
            className={`ns-chip ns-chip-${e.kind}`}
            style={{ opacity: ageOpacity(Date.now() - e.ts) }}
          >
            <span className="ns-chip-dot" aria-hidden />
            <code className="ns-chip-table">{e.table}</code>
            <span className="ns-chip-meta">
              {e.count.toLocaleString()} {e.kind}
              {e.count === 1 ? "" : "s"} · {humanDelta(Date.now() - e.ts)}
            </span>
            <span className="ns-chip-pool">{e.pool}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function ageOpacity(ageMs: number): number {
  // Linear fade from 1.0 at 0s to 0.4 at 30s, then floor.
  const t = Math.min(1, Math.max(0, ageMs / 30_000));
  return 1 - t * 0.6;
}

function humanDelta(ageMs: number): string {
  const s = Math.max(0, Math.round(ageMs / 1000));
  if (s < 1) return "just now";
  if (s < 60) return `${s}s ago`;
  return `${Math.round(s / 60)}m ago`;
}
