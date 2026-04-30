/**
 * Theme toggle: auto / light / dark.
 *
 * Operators inherit the Grafana dark theme, but shelfd also gets
 * embedded in status pages and screenshots that want a light
 * surface. We cycle through three modes:
 *
 *   - "auto" (default): tracks `prefers-color-scheme`
 *   - "light"
 *   - "dark"
 *
 * The choice persists to `localStorage` under `shelfd.theme`. The
 * actual palette lives in `styles.css` under `[data-theme="light"]`.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

export type ThemeMode = "auto" | "light" | "dark";
type Resolved = "light" | "dark";

type ThemeCtx = {
  mode: ThemeMode;
  resolved: Resolved;
  setMode: (m: ThemeMode) => void;
  cycle: () => void;
};

const Ctx = createContext<ThemeCtx | null>(null);
const LS_KEY = "shelfd.theme";

function readStored(): ThemeMode {
  try {
    const v = window.localStorage.getItem(LS_KEY);
    if (v === "light" || v === "dark" || v === "auto") return v;
  } catch {
    // ignore
  }
  return "auto";
}

function prefersDark(): boolean {
  try {
    return window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? true;
  } catch {
    return true;
  }
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ThemeMode>(() => readStored());
  const [systemDark, setSystemDark] = useState<boolean>(() => prefersDark());

  useEffect(() => {
    const mql = window.matchMedia("(prefers-color-scheme: dark)");
    const on = () => setSystemDark(mql.matches);
    mql.addEventListener?.("change", on);
    return () => mql.removeEventListener?.("change", on);
  }, []);

  const resolved: Resolved = mode === "auto" ? (systemDark ? "dark" : "light") : mode;

  useEffect(() => {
    document.documentElement.dataset.theme = resolved;
  }, [resolved]);

  const setMode = useCallback((m: ThemeMode) => {
    setModeState(m);
    try {
      window.localStorage.setItem(LS_KEY, m);
    } catch {
      // ignore
    }
  }, []);

  const cycle = useCallback(() => {
    setModeState((prev) => {
      const next: ThemeMode = prev === "auto" ? "light" : prev === "light" ? "dark" : "auto";
      try {
        window.localStorage.setItem(LS_KEY, next);
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  const value = useMemo<ThemeCtx>(
    () => ({ mode, resolved, setMode, cycle }),
    [mode, resolved, setMode, cycle],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useTheme(): ThemeCtx {
  const v = useContext(Ctx);
  if (!v) throw new Error("useTheme() requires <ThemeProvider>");
  return v;
}
