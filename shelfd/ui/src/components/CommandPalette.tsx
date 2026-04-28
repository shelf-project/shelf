/**
 * ⌘K / Ctrl+K command palette.
 *
 * Commands are registered by the app (tab switches, pause/resume,
 * theme cycle, reload pin-list, copy pod_id) so the palette stays
 * tab-agnostic. Fuzzy matching is a simple subsequence scorer — good
 * enough for ~15 commands and zero dependencies.
 */

import { useEffect, useMemo, useRef, useState } from "react";

export type Command = {
  id: string;
  label: string;
  hint?: string;
  group?: string;
  /** Optional keyboard shortcut hint to surface in the row. */
  keys?: string;
  run: () => void | Promise<void>;
};

type Props = {
  open: boolean;
  commands: Command[];
  onClose: () => void;
};

export default function CommandPalette({ open, commands, onClose }: Props) {
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (open) {
      setQuery("");
      setSelected(0);
      // Focus after paint so the browser doesn't steal it back.
      const t = window.setTimeout(() => inputRef.current?.focus(), 0);
      return () => window.clearTimeout(t);
    }
  }, [open]);

  const results = useMemo(() => rank(commands, query), [commands, query]);

  useEffect(() => {
    if (selected >= results.length) setSelected(0);
  }, [results.length, selected]);

  if (!open) return null;

  const runAt = (i: number) => {
    const c = results[i];
    if (!c) return;
    onClose();
    void c.run();
  };

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelected((s) => Math.min(results.length - 1, s + 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelected((s) => Math.max(0, s - 1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      runAt(selected);
    } else if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    }
  };

  return (
    <div className="palette-backdrop" onClick={onClose} role="dialog" aria-label="Command palette">
      <div className="palette" onClick={(e) => e.stopPropagation()}>
        <input
          ref={inputRef}
          className="palette-input"
          placeholder="Type a command… (tab, reload, pin, theme, copy)"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setSelected(0);
          }}
          onKeyDown={onKey}
          spellCheck={false}
        />
        <div className="palette-list" role="listbox">
          {results.length === 0 ? (
            <div className="palette-empty">No matches</div>
          ) : (
            results.map((c, i) => (
              <button
                key={c.id}
                role="option"
                aria-selected={i === selected}
                className={"palette-row" + (i === selected ? " palette-row-active" : "")}
                onMouseEnter={() => setSelected(i)}
                onClick={() => runAt(i)}
              >
                <span className="palette-row-main">
                  {c.group ? <span className="palette-row-group">{c.group}</span> : null}
                  <span className="palette-row-label">{c.label}</span>
                </span>
                <span className="palette-row-side">
                  {c.hint ? <span className="palette-row-hint">{c.hint}</span> : null}
                  {c.keys ? <kbd className="kbd">{c.keys}</kbd> : null}
                </span>
              </button>
            ))
          )}
        </div>
        <div className="palette-foot">
          <span><kbd className="kbd">↑↓</kbd> navigate</span>
          <span><kbd className="kbd">↵</kbd> run</span>
          <span><kbd className="kbd">Esc</kbd> close</span>
        </div>
      </div>
    </div>
  );
}

/** Lightweight subsequence-match + position-bias scoring. */
function rank(commands: Command[], q: string): Command[] {
  if (!q.trim()) return commands;
  const needle = q.toLowerCase();
  const scored: { c: Command; s: number }[] = [];
  for (const c of commands) {
    const hay = `${c.group ?? ""} ${c.label} ${c.hint ?? ""}`.toLowerCase();
    const s = fuzzy(hay, needle);
    if (s > 0) scored.push({ c, s });
  }
  scored.sort((a, b) => b.s - a.s);
  return scored.map((x) => x.c);
}

function fuzzy(hay: string, needle: string): number {
  let h = 0;
  let score = 0;
  let streak = 0;
  for (let n = 0; n < needle.length; n++) {
    const ch = needle[n];
    let found = false;
    while (h < hay.length) {
      if (hay[h] === ch) {
        found = true;
        streak++;
        // Earlier matches + consecutive matches score higher.
        score += 10 + streak * 2 - h * 0.05;
        h++;
        break;
      } else {
        streak = 0;
        h++;
      }
    }
    if (!found) return 0;
  }
  return score;
}
