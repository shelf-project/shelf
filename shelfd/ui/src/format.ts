/** Byte formatting helpers shared across Ops / Admin tabs.
 *
 * Binary units (KiB / MiB / GiB) — `shelfd` reports sizes in bytes and
 * the Iceberg / Parquet world is universally powers-of-two, so a
 * 64 KiB footer stays "64 KiB" here rather than "65.5 KB".
 */

const UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

export function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "—";
  if (n === 0) return "0 B";
  const i = Math.min(UNITS.length - 1, Math.floor(Math.log2(n) / 10));
  const val = n / Math.pow(1024, i);
  const precision = i === 0 ? 0 : val < 10 ? 2 : val < 100 ? 1 : 0;
  return `${val.toFixed(precision)} ${UNITS[i]}`;
}

export function formatPercent(n: number): string {
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

export function formatLatencyMs(seconds: number): string {
  if (!Number.isFinite(seconds)) return "—";
  const ms = seconds * 1000;
  if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`;
  if (ms < 10) return `${ms.toFixed(2)} ms`;
  if (ms < 100) return `${ms.toFixed(1)} ms`;
  return `${ms.toFixed(0)} ms`;
}

export function formatCount(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (n < 1000) return n.toFixed(0);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  return `${(n / 1_000_000_000).toFixed(1)}B`;
}

/** Stripe / Linear / Vercel pattern: "82% ↑ 3.2% vs 5m ago".
 *
 * Computes a directional delta between `now` and `then`, returns a
 * tone (`ok` / `warn` / `err`) that respects whether the metric is
 * higher-is-better or lower-is-better, and a tiny glyph that pairs
 * with colour for accessibility (never colour-only, per the AGENTS.md
 * design rules).
 *
 * Threshold for a "flat" verdict is the bigger of an absolute floor
 * (1e-6) and a relative band (0.5% of `then`). That's wide enough to
 * absorb single-poll counter jitter without flipping tones, narrow
 * enough that meaningful trends register on the next poll. */
export type DeltaTone = "ok" | "warn" | "err" | "pending";
export type DeltaDirection = "higher-is-better" | "lower-is-better";

export type DeltaReadout = {
  glyph: "↑" | "↓" | "→";
  text: string;
  tone: DeltaTone;
};

export function formatDelta(
  now: number | null | undefined,
  then: number | null | undefined,
  direction: DeltaDirection = "higher-is-better",
  unit: "percent" | "absolute" = "percent",
): DeltaReadout {
  if (
    now == null ||
    then == null ||
    !Number.isFinite(now) ||
    !Number.isFinite(then)
  ) {
    return { glyph: "→", text: "—", tone: "pending" };
  }
  const diff = now - then;
  const flatBand = Math.max(1e-6, Math.abs(then) * 0.005);
  const flat = Math.abs(diff) < flatBand;
  const better =
    direction === "higher-is-better" ? diff > 0 : diff < 0;
  const tone: DeltaTone = flat ? "warn" : better ? "ok" : "err";
  const glyph: DeltaReadout["glyph"] = flat ? "→" : diff > 0 ? "↑" : "↓";
  let text: string;
  if (unit === "percent") {
    if (Math.abs(then) < 1e-6) {
      text = flat ? "no change" : "first sample";
    } else {
      text = `${(Math.abs(diff / then) * 100).toFixed(1)}%`;
    }
  } else {
    text =
      Math.abs(diff) < 1
        ? `${diff.toFixed(2)}`
        : `${diff > 0 ? "+" : ""}${diff.toFixed(0)}`;
  }
  return { glyph, text, tone };
}
