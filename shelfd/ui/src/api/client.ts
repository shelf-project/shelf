/** Typed wrappers around the shelfd HTTP contract.
 *
 * Every function here maps 1:1 onto an endpoint already consumed by
 * `shelfctl` (see `shelfctl/src/main.rs`). The UI deliberately does
 * not introduce new endpoints — if `shelfctl stats` and the browser
 * ever disagree, we've got a bigger problem.
 */

export type Pool = "metadata" | "rowgroup";

export type PoolStats = {
  capacity_bytes: number;
  used_bytes: number;
  disk_used_bytes: number;
  disk_capacity_bytes: number;
};

export type Stats = {
  pod_id: string;
  capacity_bytes: number;
  used_bytes: number;
  metadata_pool: PoolStats;
  rowgroup_pool: PoolStats;
  pinned_bytes: number;
  pinned_count: number;
  /** SHELF-20 lameduck flag from `/stats`. Optional in the wire
   * format (`#[serde(default)]` on the Rust side) so older daemons
   * stay compatible. Live tab's row-1 peer health tile reads this. */
  draining?: boolean;
};

export type RingRow = {
  pod_id: string;
  weight: number;
  healthy: boolean;
};

/** HTTP failure that preserves the server body (admin errors are
 * operator-facing; swallowing the body would be cruel). */
export class ApiError extends Error {
  readonly status: number;
  readonly body: string;
  constructor(status: number, body: string) {
    super(`HTTP ${status}: ${body || "<no body>"}`);
    this.status = status;
    this.body = body;
  }
}

async function readJson<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new ApiError(resp.status, body);
  }
  return (await resp.json()) as T;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return readJson<T>(resp);
}

export function getStats(): Promise<Stats> {
  return fetch("/stats").then((r) => readJson<Stats>(r));
}

/** Cheap liveness/readiness probes — mirror `shelfctl health`. Returns
 * `true` on any 2xx; treats everything else (including network error)
 * as unhealthy. We deliberately swallow the body to keep the header
 * dot decision cost at one boolean. */
export async function getHealthz(): Promise<boolean> {
  try {
    const r = await fetch("/healthz", { cache: "no-store" });
    return r.ok;
  } catch {
    return false;
  }
}
export async function getReadyz(): Promise<boolean> {
  try {
    const r = await fetch("/readyz", { cache: "no-store" });
    return r.ok;
  } catch {
    return false;
  }
}

export function getRing(): Promise<RingRow[]> {
  return fetch("/admin/ring").then((r) => readJson<RingRow[]>(r));
}

export function getMetricsText(): Promise<string> {
  return fetch("/metrics").then(async (r) => {
    if (!r.ok) throw new ApiError(r.status, await r.text().catch(() => ""));
    return r.text();
  });
}

export function postPin(key_hex: string, pool: Pool): Promise<unknown> {
  return postJson("/admin/pin", { key_hex, pool });
}

export function postUnpin(key_hex: string): Promise<unknown> {
  return postJson("/admin/unpin", { key_hex });
}

export function postEvict(key_hex: string, pool: Pool): Promise<unknown> {
  return postJson("/admin/evict", { key_hex, pool });
}

export async function postReload(): Promise<unknown> {
  const r = await fetch("/admin/reload", { method: "POST" });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new ApiError(r.status, body);
  }
  // The handler returns JSON on success but some stubs return
  // empty bodies — tolerate both.
  const text = await r.text();
  try {
    return JSON.parse(text);
  } catch {
    return { ok: true, note: text };
  }
}
