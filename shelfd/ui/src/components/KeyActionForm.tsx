import { useEffect, useMemo, useRef, useState } from "react";
import type { Pool } from "../api/client";

export type ActionKind = "pin" | "unpin" | "evict";

type Props = {
  onSubmit: (kind: ActionKind, key_hex: string, pool: Pool) => Promise<void>;
};

const HEX64 = /^[0-9a-fA-F]{64}$/;

type Mode = "single" | "bulk";

export default function KeyActionForm({ onSubmit }: Props) {
  const [mode, setMode] = useState<Mode>("single");
  const [key, setKey] = useState("");
  const [bulk, setBulk] = useState("");
  const [pool, setPool] = useState<Pool>("rowgroup");
  const [busy, setBusy] = useState<ActionKind | null>(null);
  const [pending, setPending] = useState<{ kind: ActionKind; keys: string[]; pool: Pool } | null>(
    null,
  );
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);

  const bulkKeys = useMemo(() => parseBulk(bulk), [bulk]);
  const singleOk = HEX64.test(key.trim());
  const singleErr = key.length > 0 && !singleOk ? "expected 64 hex chars (sha256)" : null;
  const bulkValid = bulkKeys.valid.length > 0;

  const keyForAction = mode === "single" ? (singleOk ? [key.trim().toLowerCase()] : []) : bulkKeys.valid;
  const canAct = busy === null && keyForAction.length > 0 && (mode === "single" ? singleOk : bulkValid);

  const ask = (kind: ActionKind) => {
    if (!canAct) return;
    setPending({ kind, keys: keyForAction, pool });
  };

  const confirm = async () => {
    if (!pending) return;
    setBusy(pending.kind);
    setProgress({ done: 0, total: pending.keys.length });
    try {
      for (let i = 0; i < pending.keys.length; i++) {
        await onSubmit(pending.kind, pending.keys[i], pending.pool);
        setProgress({ done: i + 1, total: pending.keys.length });
      }
    } finally {
      setBusy(null);
      setProgress(null);
      setPending(null);
    }
  };

  return (
    <>
      <div className="mode-toggle" role="tablist" aria-label="Key action mode">
        <button
          role="tab"
          aria-selected={mode === "single"}
          className={"mode-btn" + (mode === "single" ? " mode-btn-active" : "")}
          onClick={() => setMode("single")}
        >
          Single
        </button>
        <button
          role="tab"
          aria-selected={mode === "bulk"}
          className={"mode-btn" + (mode === "bulk" ? " mode-btn-active" : "")}
          onClick={() => setMode("bulk")}
        >
          Bulk paste
        </button>
      </div>

      {mode === "single" ? (
        <div className="form-row">
          <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <span className="card-title" style={{ margin: 0 }}>Key (hex, 64 chars)</span>
            <input
              className={"input" + (singleErr ? " input-err" : "")}
              value={key}
              onChange={(e) => setKey(e.target.value)}
              spellCheck={false}
              placeholder="sha256(etag || offset || length)"
              aria-invalid={singleErr !== null}
            />
            <span className={"input-hint" + (singleErr ? " input-hint-err" : "")}>
              {singleErr ?? "content-addressed cache key"}
            </span>
          </label>
          <PoolPicker pool={pool} setPool={setPool} />
        </div>
      ) : (
        <div className="form-row">
          <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <span className="card-title" style={{ margin: 0 }}>Keys (one per line)</span>
            <textarea
              className={
                "input" + (bulk.length > 0 && bulkKeys.invalid > 0 ? " input-err" : "")
              }
              value={bulk}
              onChange={(e) => setBulk(e.target.value)}
              rows={6}
              spellCheck={false}
              placeholder={"deadbeef…cafebabe\ndeadbeef…cafe0001"}
              style={{ fontFamily: "var(--mono)", fontSize: 12, resize: "vertical" }}
            />
            <span
              className={"input-hint" + (bulkKeys.invalid > 0 ? " input-hint-err" : "")}
            >
              {bulkKeys.valid.length} valid
              {bulkKeys.invalid > 0 ? ` · ${bulkKeys.invalid} malformed (skipped)` : ""}
              {bulkKeys.duplicates > 0 ? ` · ${bulkKeys.duplicates} duplicates collapsed` : ""}
            </span>
          </label>
          <PoolPicker pool={pool} setPool={setPool} />
        </div>
      )}

      <div className="button-row">
        <button
          className="btn btn-primary"
          disabled={!canAct}
          onClick={() => ask("pin")}
        >
          {busy === "pin" ? (progress ? `pinning ${progress.done}/${progress.total}…` : "pinning…") : "Pin"}
        </button>
        <button
          className="btn"
          disabled={!canAct}
          onClick={() => ask("unpin")}
        >
          {busy === "unpin" ? (progress ? `unpinning ${progress.done}/${progress.total}…` : "unpinning…") : "Unpin"}
        </button>
        <button
          className="btn btn-danger"
          disabled={!canAct}
          onClick={() => ask("evict")}
        >
          {busy === "evict" ? (progress ? `evicting ${progress.done}/${progress.total}…` : "evicting…") : "Evict"}
        </button>
      </div>

      {pending ? (
        <ConfirmModal
          kind={pending.kind}
          keys={pending.keys}
          pool={pending.pool}
          busy={busy !== null}
          progress={progress}
          onCancel={() => {
            if (busy) return; // can't cancel mid-run — the REST calls are in flight
            setPending(null);
          }}
          onConfirm={confirm}
        />
      ) : null}
    </>
  );
}

function PoolPicker({ pool, setPool }: { pool: Pool; setPool: (p: Pool) => void }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <span className="card-title" style={{ margin: 0 }}>Pool</span>
      <select
        className="select"
        value={pool}
        onChange={(e) => setPool(e.target.value as Pool)}
      >
        <option value="rowgroup">rowgroup</option>
        <option value="metadata">metadata</option>
      </select>
      <span className="input-hint">unpin is pool-agnostic</span>
    </label>
  );
}

function ConfirmModal({
  kind,
  keys,
  pool,
  busy,
  progress,
  onCancel,
  onConfirm,
}: {
  kind: ActionKind;
  keys: string[];
  pool: Pool;
  busy: boolean;
  progress: { done: number; total: number } | null;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const cancelRef = useRef<HTMLButtonElement | null>(null);
  const confirmRef = useRef<HTMLButtonElement | null>(null);
  const isBulk = keys.length > 1;

  useEffect(() => {
    // Focus the primary action so Enter works immediately; screen
    // readers announce the dialog role because of `role="dialog"`.
    confirmRef.current?.focus();
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        if (!busy) onCancel();
      } else if (e.key === "Enter" && !busy) {
        // Only fire if the focus is inside the modal — avoid grabbing
        // Enter from arbitrary form controls.
        const modal = confirmRef.current?.closest(".modal");
        if (modal && modal.contains(document.activeElement)) {
          e.preventDefault();
          onConfirm();
        }
      } else if (e.key === "Tab") {
        // Tiny focus trap between the two buttons.
        const focusables = [cancelRef.current, confirmRef.current].filter(Boolean) as HTMLElement[];
        if (focusables.length === 0) return;
        const active = document.activeElement as HTMLElement | null;
        const idx = focusables.indexOf(active!);
        if (idx < 0) {
          e.preventDefault();
          focusables[0].focus();
          return;
        }
        const next = e.shiftKey ? (idx - 1 + focusables.length) % focusables.length : (idx + 1) % focusables.length;
        e.preventDefault();
        focusables[next].focus();
      }
    };
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, [busy, onCancel, onConfirm]);

  const actionCls = kind === "evict" ? "btn btn-danger" : "btn btn-primary";

  return (
    <div
      className="modal-backdrop"
      role="dialog"
      aria-label={`Confirm ${kind}`}
      onClick={() => {
        if (!busy) onCancel();
      }}
    >
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>Confirm {kind}{isBulk ? ` (${keys.length})` : ""}</h3>
        <p>
          This hits <code>POST /admin/{kind}</code>
          {isBulk ? " sequentially for each key" : ""}. It is idempotent but permanent.
        </p>
        <div className="kv">
          {isBulk ? (
            <>
              {keys.slice(0, 4).map((k) => (
                <div key={k}>{k}</div>
              ))}
              {keys.length > 4 ? <div>…and {keys.length - 4} more</div> : null}
            </>
          ) : (
            <>
              key_hex: {keys[0]}
              <br />
              {kind !== "unpin" ? <>pool: {pool}</> : <>pool: (ignored)</>}
            </>
          )}
          {isBulk && kind !== "unpin" ? <><br />pool: {pool}</> : null}
        </div>
        {progress ? (
          <div style={{ marginBottom: 12 }}>
            <div className="bar" style={{ height: 6 }}>
              <div
                className="bar-fill"
                style={{
                  width: `${(progress.done / Math.max(1, progress.total)) * 100}%`,
                }}
              />
            </div>
            <div className="stat-sub" style={{ marginTop: 4 }}>
              {progress.done} / {progress.total} done
            </div>
          </div>
        ) : null}
        <div className="button-row" style={{ justifyContent: "flex-end" }}>
          <button ref={cancelRef} className="btn" disabled={busy} onClick={onCancel}>
            Cancel <span className="kbd-inline">Esc</span>
          </button>
          <button ref={confirmRef} className={actionCls} disabled={busy} onClick={onConfirm}>
            {busy ? `${kind}ing…` : (
              <>Confirm {kind} <span className="kbd-inline">↵</span></>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

function parseBulk(text: string): { valid: string[]; invalid: number; duplicates: number } {
  const seen = new Set<string>();
  let invalid = 0;
  let duplicates = 0;
  const valid: string[] = [];
  for (const raw of text.split(/\s+/)) {
    const line = raw.trim();
    if (!line) continue;
    if (!HEX64.test(line)) {
      invalid++;
      continue;
    }
    const norm = line.toLowerCase();
    if (seen.has(norm)) {
      duplicates++;
      continue;
    }
    seen.add(norm);
    valid.push(norm);
  }
  return { valid, invalid, duplicates };
}
