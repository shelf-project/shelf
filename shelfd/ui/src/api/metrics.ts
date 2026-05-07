/** Tiny Prometheus text-format parser.
 *
 * We only read a handful of series (hits, misses, fallbacks, request
 * latency histogram), so rolling our own beats pulling in a 30 KB
 * prom-client parser. The format we accept is the subset `shelfd`
 * emits via `prometheus::TextEncoder`:
 *
 *     # HELP shelf_hits_total ...
 *     # TYPE shelf_hits_total counter
 *     shelf_hits_total{pool="metadata"} 42
 *     shelf_hits_total{pool="rowgroup"} 13
 *     shelf_request_seconds_bucket{path="/cache",outcome="hit",le="0.001"} 7
 *     shelf_request_seconds_bucket{path="/cache",outcome="hit",le="+Inf"} 42
 *     shelf_request_seconds_sum{path="/cache",outcome="hit"} 0.123
 *     shelf_request_seconds_count{path="/cache",outcome="hit"} 42
 *
 * No exemplars, no scientific notation beyond what `parseFloat` already
 * handles, no multi-line HELP. That's the standard Rust exposition.
 */

export type Sample = {
  name: string;
  labels: Record<string, string>;
  value: number;
};

/** Parse a single `key="value"` label pair; handles simple escapes. */
function parseLabels(src: string): Record<string, string> {
  // `src` is the inside of `{...}`.
  const out: Record<string, string> = {};
  let i = 0;
  while (i < src.length) {
    while (i < src.length && (src[i] === "," || src[i] === " ")) i++;
    const eq = src.indexOf("=", i);
    if (eq < 0) break;
    const key = src.slice(i, eq).trim();
    let j = eq + 1;
    if (src[j] !== '"') break;
    j++;
    let val = "";
    while (j < src.length && src[j] !== '"') {
      if (src[j] === "\\" && j + 1 < src.length) {
        const esc = src[j + 1];
        val += esc === "n" ? "\n" : esc === "\\" ? "\\" : esc === '"' ? '"' : esc;
        j += 2;
      } else {
        val += src[j];
        j++;
      }
    }
    j++;
    out[key] = val;
    i = j;
  }
  return out;
}

export function parseMetrics(text: string): Sample[] {
  const out: Sample[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    // <name>{labels} value   OR   <name> value
    const braceStart = line.indexOf("{");
    let name: string;
    let labels: Record<string, string> = {};
    let rest: string;
    if (braceStart >= 0) {
      name = line.slice(0, braceStart).trim();
      const braceEnd = line.indexOf("}", braceStart);
      if (braceEnd < 0) continue;
      labels = parseLabels(line.slice(braceStart + 1, braceEnd));
      rest = line.slice(braceEnd + 1).trim();
    } else {
      const sp = line.indexOf(" ");
      if (sp < 0) continue;
      name = line.slice(0, sp);
      rest = line.slice(sp + 1).trim();
    }
    const valueStr = rest.split(/\s+/, 1)[0];
    const value = Number(valueStr);
    if (!Number.isFinite(value)) continue;
    out.push({ name, labels, value });
  }
  return out;
}

/** Sum a labelled counter across all its children. */
export function sumSeries(samples: Sample[], name: string): number {
  let total = 0;
  for (const s of samples) {
    if (s.name === name) total += s.value;
  }
  return total;
}

/** Return a map { labelValue -> summed count } for a given label key.
 * Handy for splitting a counter across a dimension (`pool`, `outcome`)
 * in one pass. */
export function groupBy(
  samples: Sample[],
  name: string,
  label: string,
): Record<string, number> {
  const out: Record<string, number> = {};
  for (const s of samples) {
    if (s.name !== name) continue;
    const k = s.labels[label] ?? "";
    out[k] = (out[k] ?? 0) + s.value;
  }
  return out;
}

/** Sum samples that match a label predicate. */
export function sumMatching(
  samples: Sample[],
  name: string,
  match: (labels: Record<string, string>) => boolean,
): number {
  let total = 0;
  for (const s of samples) {
    if (s.name === name && match(s.labels)) total += s.value;
  }
  return total;
}

/** Return the raw `[le, count]` bucket array for a histogram metric,
 * filtered by an arbitrary label predicate. Sorted ascending by `le`,
 * with `+Inf` mapped to `Number.POSITIVE_INFINITY`. Used by the Lab
 * tab heat-strip — the panel that catches anomalies like
 * `hit_disk` p99 pegged at 16.384 s, where the histogram shape itself
 * is the signal and a single-quantile readout would hide it. */
export function histogramBuckets(
  samples: Sample[],
  metric: string,
  match: (labels: Record<string, string>) => boolean,
): { le: number; count: number }[] {
  const out: { le: number; count: number }[] = [];
  for (const s of samples) {
    if (s.name !== `${metric}_bucket`) continue;
    if (!match(s.labels)) continue;
    const leStr = s.labels["le"];
    if (leStr === undefined) continue;
    const le = leStr === "+Inf" ? Number.POSITIVE_INFINITY : Number(leStr);
    if (!Number.isFinite(le) && le !== Number.POSITIVE_INFINITY) continue;
    out.push({ le, count: s.value });
  }
  out.sort((a, b) => a.le - b.le);
  return out;
}

/** Group all label combinations on a metric, returning one row per
 * distinct label tuple with the summed counter value. Each row keeps
 * the original `labels` map so callers can render a per-row sparkline
 * keyed on whatever dimension makes sense (table, pool, decision, …).
 * Pairs with [`useTimeseriesByKey`](../hooks/useTimeseries.ts) on the
 * leaderboard. */
export function listSeries(
  samples: Sample[],
  name: string,
): Array<{ key: string; labels: Record<string, string>; value: number }> {
  const map = new Map<string, { labels: Record<string, string>; value: number }>();
  for (const s of samples) {
    if (s.name !== name) continue;
    // Stable join: sort label keys so {a,b} and {b,a} fold together.
    const key = Object.keys(s.labels)
      .sort()
      .map((k) => `${k}=${s.labels[k]}`)
      .join("|");
    const existing = map.get(key);
    if (existing) {
      existing.value += s.value;
    } else {
      map.set(key, { labels: s.labels, value: s.value });
    }
  }
  return Array.from(map.entries()).map(([key, v]) => ({
    key,
    labels: v.labels,
    value: v.value,
  }));
}

// --- CacheEvent stream helpers ---

/** A single cache hit or miss event derived from per-table counter deltas. */
export type CacheEvent = {
  kind: "hit" | "miss";
  /** Table name from the `table` label of shelf_hits/misses_by_table_total. */
  table: string;
  /** Delta count since the previous poll. */
  count: number;
};

type EventStreamState = {
  prevHits: Record<string, number>;
  prevMisses: Record<string, number>;
};

/** Returns a fresh, empty event-stream accumulator for `deriveEvents`. */
export function emptyEventStream(): EventStreamState {
  return { prevHits: {}, prevMisses: {} };
}

/** Compute `CacheEvent[]` by diffing per-table counters against `state`.
 * Mutates `state` in-place so the caller's `useRef` stays consistent across
 * polls without needing reassignment. */
export function deriveEvents(
  samples: Sample[],
  state: EventStreamState,
): CacheEvent[] {
  const hits = new Map<string, number>();
  const misses = new Map<string, number>();
  for (const s of samples) {
    if (s.name === "shelf_hits_by_table_total") {
      const t = s.labels["table"] ?? "unknown";
      hits.set(t, (hits.get(t) ?? 0) + s.value);
    } else if (s.name === "shelf_misses_by_table_total") {
      const t = s.labels["table"] ?? "unknown";
      misses.set(t, (misses.get(t) ?? 0) + s.value);
    }
  }
  const events: CacheEvent[] = [];
  for (const [table, count] of hits) {
    const delta = count - (state.prevHits[table] ?? 0);
    if (delta > 0) events.push({ kind: "hit", table, count: delta });
    state.prevHits[table] = count;
  }
  for (const [table, count] of misses) {
    const delta = count - (state.prevMisses[table] ?? 0);
    if (delta > 0) events.push({ kind: "miss", table, count: delta });
    state.prevMisses[table] = count;
  }
  return events;
}

/** Approximate a percentile from a Prom histogram by linear
 * interpolation inside the bucket that first reaches the target
 * cumulative count. Not exact, but good enough for a single-value
 * ops card and consistent with what Grafana's `histogram_quantile`
 * does on a single sample. */
export function histogramQuantile(
  samples: Sample[],
  metric: string,
  match: (labels: Record<string, string>) => boolean,
  q: number,
): number | null {
  const buckets: { le: number; count: number }[] = [];
  for (const s of samples) {
    if (s.name !== `${metric}_bucket`) continue;
    if (!match(s.labels)) continue;
    const leStr = s.labels["le"];
    if (leStr === undefined) continue;
    const le = leStr === "+Inf" ? Number.POSITIVE_INFINITY : Number(leStr);
    if (!Number.isFinite(le) && le !== Number.POSITIVE_INFINITY) continue;
    buckets.push({ le, count: s.value });
  }
  if (buckets.length === 0) return null;
  buckets.sort((a, b) => a.le - b.le);
  const total = buckets[buckets.length - 1].count;
  if (total <= 0) return null;
  const target = q * total;
  let prevCount = 0;
  let prevLe = 0;
  for (const b of buckets) {
    if (b.count >= target) {
      if (!Number.isFinite(b.le)) return prevLe; // +Inf bucket
      const frac = (target - prevCount) / (b.count - prevCount || 1);
      return prevLe + (b.le - prevLe) * frac;
    }
    prevCount = b.count;
    prevLe = b.le;
  }
  return buckets[buckets.length - 1].le;
}
