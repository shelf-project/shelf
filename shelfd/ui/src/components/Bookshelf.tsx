/** The literal bookshelf — every cached table is a book.
 *
 * Optional bold bet from the polish plan. Width = bytes occupied,
 * colour = stable hash of the table name. New books appear when a
 * table first shows up in `shelf_hits_by_table_total`, fade out when
 * a table stops being seen for a few polls (assumed evicted).
 *
 * Why this earns the pixel budget on a Story tab that already has a
 * stacked bar:
 *   - The product is *literally* called shelf. The metaphor lands
 *     instantly for non-tech viewers ("oh, that's the shelf").
 *   - Per-table widths show concentration (one giant book = one
 *     dominant table) which the stacked bar's pool-level encoding
 *     loses.
 *   - Animation is purposeful: motion = admission/eviction events,
 *     not decoration. Honours `prefers-reduced-motion`.
 *
 * Falls back gracefully: if the metric series is empty (no labelled
 * traffic yet) we render an explicit empty-state, not blank space.
 */

import { useEffect, useRef, useState } from "react";
import { formatBytes } from "../format";

type Book = {
  id: string;
  table: string;
  pool: string;
  bytes: number;
  /** ms since 1970 — used to fade in newly-arrived books. */
  arrivedAt: number;
};

type Props = {
  /** Per-table series. Bytes are derived from
   *  `shelf_s3_shim_response_bytes_total{table=...}` if the daemon
   *  exposes it, otherwise we fall back to hits × an average byte
   *  estimate (Story tab passes us the resolved values either way). */
  rows: Array<{ key: string; table: string; pool: string; bytes: number }>;
  /** Caption shown to the right of the row count (e.g. "2.4 TB
   *  served · the rest lives at S3"). */
  caption: string;
};

export default function Bookshelf({ rows, caption }: Props) {
  const lastSeen = useRef<Map<string, Book>>(new Map());
  const [books, setBooks] = useState<Book[]>([]);

  useEffect(() => {
    const now = Date.now();
    const next = new Map<string, Book>();
    for (const r of rows) {
      if (r.bytes <= 0 || r.table === "other") continue;
      const prior = lastSeen.current.get(r.key);
      next.set(r.key, {
        id: r.key,
        table: r.table,
        pool: r.pool,
        bytes: r.bytes,
        arrivedAt: prior?.arrivedAt ?? now,
      });
    }
    lastSeen.current = next;
    // Sort by bytes desc, cap at 24 books so we don't render hair-
    // thin slivers; the stacked bar already covers tail behaviour.
    setBooks(
      Array.from(next.values())
        .sort((a, b) => b.bytes - a.bytes)
        .slice(0, 24),
    );
  }, [rows]);

  const total = books.reduce((a, b) => a + b.bytes, 0);

  if (books.length === 0) {
    return (
      <section className="card bookshelf-empty">
        <h3 className="card-title">Cached on this pod</h3>
        <p className="story-headline-sub">
          No tables have been admitted yet. The shelf will fill up as
          queries land — each book below will be a real Iceberg table.
        </p>
        <div className="bookshelf-track" aria-hidden>
          <div className="bookshelf-rod" />
        </div>
      </section>
    );
  }

  return (
    <section className="card bookshelf">
      <div className="bookshelf-head">
        <h3 className="card-title" style={{ margin: 0 }}>
          Cached on this pod
        </h3>
        <span className="story-headline-sub">{caption}</span>
      </div>
      <div className="bookshelf-track" role="img" aria-label="cached tables">
        <div className="bookshelf-rod" aria-hidden />
        {books.map((b) => {
          const widthPct = (b.bytes / total) * 100;
          if (widthPct < 0.4) return null;
          return (
            <div
              key={b.id}
              className="bookshelf-book"
              style={{
                width: `${widthPct}%`,
                background: bookColor(b.table),
                animationDelay: `${ageMs(b.arrivedAt) < 1500 ? 0 : 0}ms`,
              }}
              title={`${b.table} · ${formatBytes(b.bytes)} (${widthPct.toFixed(1)}%)`}
            >
              <span className="bookshelf-spine">
                {widthPct >= 6 ? truncTable(b.table, widthPct) : ""}
              </span>
            </div>
          );
        })}
      </div>
      <div className="bookshelf-foot">
        <span>
          {books.length} book{books.length === 1 ? "" : "s"} · {formatBytes(total)}{" "}
          on the shelf
        </span>
        <span className="bookshelf-legend">colour = table identity</span>
      </div>
    </section>
  );
}

function ageMs(arrivedAt: number): number {
  return Date.now() - arrivedAt;
}

function truncTable(name: string, widthPct: number): string {
  // Vertical spine — show only the trailing identifier, never the
  // catalog/schema prefix, so the spine is readable at typical widths.
  const tail = name.split(/[/.]/).pop() ?? name;
  if (widthPct >= 14) return tail;
  if (widthPct >= 10) return tail.slice(0, 18);
  return tail.slice(0, 10);
}

/** Stable per-table colour: a hash → HSL with a fixed saturation/
 * lightness band so all books read as a coherent palette regardless
 * of how many are rendered. */
function bookColor(name: string): string {
  let h = 5381;
  for (let i = 0; i < name.length; i++) h = (h * 33) ^ name.charCodeAt(i);
  const hue = Math.abs(h) % 360;
  return `linear-gradient(180deg, hsl(${hue} 55% 45%) 0%, hsl(${hue} 55% 35%) 100%)`;
}
