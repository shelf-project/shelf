/**
 * Global keyboard shortcut registry.
 *
 * Tabs, pause, theme, palette, and the help overlay all want their
 * own keystroke. Handing each one `useEffect(() => addEventListener
 * …)` ended up wedging stale closures on every tab switch, so we
 * centralise via a provider: children `useShortcut(pattern, fn)`
 * and the provider fans out a single document-level handler.
 *
 * A pattern is either a single character (matched case-insensitively
 * against `event.key`) or a synthetic name:
 *
 *   - "?"            — shift+/ on US layouts
 *   - "mod+k"        — ⌘K on macOS, Ctrl+K elsewhere
 *   - "Escape"
 *
 * Shortcuts do not fire while the user is typing in an input,
 * textarea, or contenteditable surface. The palette and modals are
 * responsible for installing their own Escape handlers.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  type ReactNode,
} from "react";

type Handler = (e: KeyboardEvent) => void;

type Ctx = {
  register: (pattern: string, fn: Handler) => () => void;
};

const C = createContext<Ctx | null>(null);

function isTypingTarget(t: EventTarget | null): boolean {
  if (!(t instanceof HTMLElement)) return false;
  const tag = t.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
  if (t.isContentEditable) return true;
  return false;
}

function matches(pattern: string, e: KeyboardEvent): boolean {
  if (pattern === "mod+k") {
    return (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k";
  }
  if (pattern === "?") {
    return e.key === "?" || (e.shiftKey && e.key === "/");
  }
  if (pattern === "Escape") return e.key === "Escape";
  // Default: single-character, case-insensitive, no modifiers.
  if (e.metaKey || e.ctrlKey || e.altKey) return false;
  return e.key.toLowerCase() === pattern.toLowerCase();
}

export function ShortcutsProvider({ children }: { children: ReactNode }) {
  const handlersRef = useRef<Map<string, Set<Handler>>>(new Map());

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.defaultPrevented) return;
      for (const [pattern, set] of handlersRef.current) {
        if (!matches(pattern, e)) continue;
        // Escape fires even inside inputs; everything else does not.
        if (pattern !== "Escape" && pattern !== "mod+k" && isTypingTarget(e.target)) continue;
        for (const fn of set) fn(e);
        return;
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const register = useCallback((pattern: string, fn: Handler) => {
    const map = handlersRef.current;
    let set = map.get(pattern);
    if (!set) {
      set = new Set();
      map.set(pattern, set);
    }
    set.add(fn);
    return () => {
      const s = map.get(pattern);
      if (!s) return;
      s.delete(fn);
      if (s.size === 0) map.delete(pattern);
    };
  }, []);

  const value = useMemo<Ctx>(() => ({ register }), [register]);
  return <C.Provider value={value}>{children}</C.Provider>;
}

export function useShortcut(pattern: string, fn: Handler, enabled = true) {
  const ctx = useContext(C);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  useEffect(() => {
    if (!ctx || !enabled) return;
    return ctx.register(pattern, (e) => fnRef.current(e));
  }, [ctx, pattern, enabled]);
}
