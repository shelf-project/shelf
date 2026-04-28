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
