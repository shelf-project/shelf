import { useEffect, useState } from "react";
import KeyActionForm, { type ActionKind } from "../components/KeyActionForm";
import RingTable from "../components/RingTable";
import { useToasts } from "../components/Toasts";
import {
  ApiError,
  getRing,
  postEvict,
  postPin,
  postReload,
  postUnpin,
  type Pool,
  type Stats,
} from "../api/client";
import { usePolled, usePolling } from "../polling";
import { useShortcut } from "../shortcuts";

type Props = { stats: Stats | null };

export default function AdminTab({ stats }: Props) {
  const { data: ring, error: ringError } = usePolled(getRing);
  const { push, view: toastView } = useToasts();
  const [reloading, setReloading] = useState(false);
  const { tickNow } = usePolling();

  useShortcut("r", () => tickNow());

  // Register a "reload pin-list" shortcut only when this tab is open
  // — otherwise the global `?` help covers it via the palette entry.
  useEffect(() => () => void 0, []);

  const dispatch = async (kind: ActionKind, key_hex: string, pool: Pool) => {
    try {
      const res =
        kind === "pin"
          ? await postPin(key_hex, pool)
          : kind === "unpin"
          ? await postUnpin(key_hex)
          : await postEvict(key_hex, pool);
      push({
        kind: "ok",
        title: `${kind} ok`,
        body: summarize(res, `${kind} ${key_hex.slice(0, 12)}… (${pool})`),
      });
      // Admin actions change what /stats and /admin/ring report;
      // poke the shared ticker so the UI shows the new state right
      // away instead of waiting for the next interval.
      tickNow();
    } catch (e) {
      push({
        kind: "err",
        title: `${kind} failed`,
        body: e instanceof ApiError ? e.body || `HTTP ${e.status}` : String(e),
      });
    }
  };

  const reload = async () => {
    setReloading(true);
    try {
      const res = await postReload();
      push({
        kind: "ok",
        title: "pin-list reload triggered",
        body: summarize(res, "POST /admin/reload"),
      });
      tickNow();
    } catch (e) {
      push({
        kind: "err",
        title: "reload failed",
        body: e instanceof ApiError ? e.body || `HTTP ${e.status}` : String(e),
      });
    } finally {
      setReloading(false);
    }
  };

  return (
    <>
      <section className="card">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 12 }}>
          <h3 className="card-title">Pin-list</h3>
          <button className="btn btn-primary" onClick={reload} disabled={reloading}>
            {reloading ? "reloading…" : "Reload pin-list"}
          </button>
        </div>
        <p style={{ color: "var(--fg-dim)", fontSize: 12, margin: "4px 0 0" }}>
          Equivalent to <code>shelfctl reload</code> or sending <code>SIGHUP</code>. The 15-minute
          timer and <code>SIGHUP</code> both still fire; this button just bypasses the wait.
        </p>
      </section>

      <section className="card">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 12 }}>
          <h3 className="card-title">HRW ring</h3>
          <span className="stat-sub" style={{ margin: 0 }}>
            {ring ? `${ring.length} ${ring.length === 1 ? "member" : "members"}` : "…"}
          </span>
        </div>
        {ringError ? (
          <div style={{ color: "var(--err)", fontFamily: "var(--mono)", fontSize: 12 }}>
            {ringError}
          </div>
        ) : (
          <RingTable rows={ring} self={stats?.pod_id ?? null} />
        )}
      </section>

      <section className="card">
        <h3 className="card-title">Key actions</h3>
        <p style={{ color: "var(--fg-dim)", fontSize: 12, margin: "0 0 12px" }}>
          Pin/unpin bypass the size-threshold admission policy (ADR-0003). Evict drops a key from
          the pool without touching the pin set. Bulk paste is fine — each key is sent one at a
          time so you'll see partial progress if the first one fails.
        </p>
        <KeyActionForm onSubmit={dispatch} />
      </section>

      {toastView}
    </>
  );
}

function summarize(res: unknown, fallback: string): string {
  if (res && typeof res === "object") {
    try {
      const s = JSON.stringify(res);
      return s.length > 280 ? s.slice(0, 277) + "…" : s;
    } catch {
      return fallback;
    }
  }
  return fallback;
}
