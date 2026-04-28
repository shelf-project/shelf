import { useEffect, useMemo, useState } from "react";
import StoryTab from "./tabs/StoryTab";
import LiveTab from "./tabs/LiveTab";
import HotTablesTab from "./tabs/HotTablesTab";
import LabTab from "./tabs/LabTab";
import AdminTab from "./tabs/AdminTab";
import ErrorBoundary from "./components/ErrorBoundary";
import HelpOverlay from "./components/HelpOverlay";
import CommandPalette, { type Command } from "./components/CommandPalette";
import CopyButton from "./components/CopyButton";
import {
  ApiError,
  getHealthz,
  getReadyz,
  getStats,
  postReload,
} from "./api/client";
import { usePolled, usePolling } from "./polling";
import { useShortcut } from "./shortcuts";
import { useTheme } from "./theme";

type TabId = "story" | "live" | "hot" | "lab" | "admin";

type TabDef = { id: TabId; label: string; hint: string };

const FULL_TABS: TabDef[] = [
  { id: "story", label: "Story", hint: "What this cache earned" },
  { id: "live", label: "Live", hint: "Hit rate, latency, capacity, incidents" },
  { id: "hot", label: "Hot tables", hint: "Per-table leaderboard" },
  { id: "lab", label: "Lab", hint: "Admission, eviction, peer, heat-strip" },
];

/** Public exposure switch — set the URL fragment to `#public` (or
 * `#story?...cost knobs...`) and only the stakeholder-facing Story
 * tab is rendered. Live + Lab leak operational detail (pod IDs, error
 * kinds, ring members) so they should never be visible to anyone
 * outside the cluster. The flag is URL-only on purpose: it requires
 * zero backend coordination, mirrors how operators already use
 * `#story` / `#live` to deep-link, and can be enforced by an ingress
 * rewriting the path to `/ui#public/...`. */
function isPublicMode(): boolean {
  if (typeof window === "undefined") return false;
  const h = window.location.hash.replace(/^#/, "");
  return h === "public" || h.startsWith("public?") || h.startsWith("public/");
}

export default function App() {
  const publicOnly = isPublicMode();
  const visibleTabs = publicOnly ? FULL_TABS.slice(0, 1) : FULL_TABS;
  const [tab, setTab] = useState<TabId>(initialTab(publicOnly));
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [now, setNow] = useState(Date.now());

  const { data: stats, error } = usePolled(getStats);
  const { data: healthz } = usePolled(getHealthz);
  const { data: readyz } = usePolled(getReadyz);
  const { paused, togglePaused, lastSuccess, intervalMs, tickNow } = usePolling();
  const theme = useTheme();

  useEffect(() => {
    // Preserve any post-`?` query string the URL hash may carry (e.g.
    // `#story?get_per_1k=0.0004&gb=0.023` for the Story cost panel)
    // when we sync the active tab back to `location.hash`.
    const existing = window.location.hash.replace(/^#/, "");
    const query = existing.includes("?") ? "?" + existing.split("?", 2)[1] : "";
    if (publicOnly) return;
    window.history.replaceState(null, "", `#${tab}${query}`);
  }, [tab, publicOnly]);

  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  useShortcut("1", () => setTab("story"));
  useShortcut("2", () => !publicOnly && setTab("live"));
  useShortcut("3", () => !publicOnly && setTab("hot"));
  useShortcut("4", () => !publicOnly && setTab("lab"));
  useShortcut("5", () => !publicOnly && setTab("admin"));
  useShortcut("p", () => togglePaused());
  useShortcut("t", () => theme.cycle());
  useShortcut("?", () => setHelpOpen((v) => !v));
  useShortcut("mod+k", (e) => {
    e.preventDefault();
    setPaletteOpen(true);
  });
  useShortcut("Escape", () => {
    if (paletteOpen) setPaletteOpen(false);
    else if (helpOpen) setHelpOpen(false);
  });

  const runReload = async () => {
    try {
      await postReload();
      tickNow();
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error("reload failed", e instanceof ApiError ? e.body : e);
    }
  };

  const commands = useMemo<Command[]>(() => {
    const base: Command[] = [
      { id: "tab-story", group: "Navigate", label: "Go to Story", keys: "1", run: () => setTab("story") },
    ];
    if (!publicOnly) {
      base.push(
        { id: "tab-live", group: "Navigate", label: "Go to Live", keys: "2", run: () => setTab("live") },
        { id: "tab-hot", group: "Navigate", label: "Go to Hot tables", keys: "3", run: () => setTab("hot") },
        { id: "tab-lab", group: "Navigate", label: "Go to Lab", keys: "4", run: () => setTab("lab") },
        { id: "tab-admin", group: "Navigate", label: "Go to Admin (pin / evict / ring)", keys: "5", run: () => setTab("admin") },
      );
    }
    base.push(
      { id: "refresh", group: "Live", label: "Refresh now", keys: "r", run: () => tickNow() },
      { id: "pause", group: "Live", label: paused ? "Resume polling" : "Pause polling", keys: "p", run: () => togglePaused() },
      { id: "theme", group: "Appearance", label: `Theme: ${theme.mode} (cycle)`, keys: "t", run: () => theme.cycle() },
    );
    if (!publicOnly) {
      base.push(
        { id: "reload-pins", group: "Admin", label: "Reload pin-list (POST /admin/reload)", run: () => void runReload() },
        {
          id: "copy-pod",
          group: "Admin",
          label: stats ? `Copy pod_id: ${stats.pod_id}` : "Copy pod_id",
          run: async () => {
            if (!stats) return;
            try { await navigator.clipboard.writeText(stats.pod_id); } catch { /* ignore */ }
          },
        },
        { id: "open-metrics", group: "Raw", label: "Open /metrics", run: () => { window.open("/metrics", "_blank"); } },
        { id: "open-stats", group: "Raw", label: "Open /stats", run: () => { window.open("/stats", "_blank"); } },
        { id: "open-ring", group: "Raw", label: "Open /admin/ring", run: () => { window.open("/admin/ring", "_blank"); } },
      );
    }
    base.push({ id: "help", group: "Help", label: "Show keyboard shortcuts", keys: "?", run: () => setHelpOpen(true) });
    return base;
  }, [paused, theme.mode, tickNow, togglePaused, theme, stats, publicOnly]);

  const activeIdx = visibleTabs.findIndex((t) => t.id === tab);
  const connectionLabel = error ? "offline" : stats ? "connected" : "connecting…";
  const connectionClass = error ? "status-err" : stats ? "status-ok" : "status-pending";
  const freshness = lastSuccess ? Math.max(0, Math.round((now - lastSuccess) / 1000)) : null;

  return (
    <div className={"app" + (publicOnly ? " app-public" : "")}>
      <header className="app-header">
        <div className="brand">
          <span className="brand-mark" aria-hidden>▌</span>
          <span className="brand-name">shelfd</span>
          <span className="brand-sub">row-group cache for Trino</span>
        </div>
        <div className="identity">
          {!publicOnly ? <HealthDots healthz={healthz} readyz={readyz} /> : null}
          {stats && !publicOnly ? (
            <span className="pod-id-wrap" title="Pod identity">
              <span className="pod-id">{stats.pod_id}</span>
              <CopyButton text={stats.pod_id} label="Copy pod_id" compact />
            </span>
          ) : null}
          <span className={"status " + connectionClass} title={error ?? undefined}>
            {connectionLabel}
          </span>
          <FreshnessBadge
            paused={paused}
            ageSec={freshness}
            intervalSec={Math.round(intervalMs / 1000)}
            onToggle={togglePaused}
            onRefresh={tickNow}
          />
          <button
            className="icon-btn"
            onClick={theme.cycle}
            aria-label={`Theme: ${theme.mode}`}
            title={`Theme: ${theme.mode} — click to cycle (t)`}
          >
            <ThemeIcon mode={theme.mode} />
          </button>
          {!publicOnly ? (
            <button
              className="icon-btn icon-btn-palette"
              onClick={() => setPaletteOpen(true)}
              aria-label="Open command palette"
              title="Command palette (⌘K)"
            >
              <kbd className="kbd">⌘K</kbd>
            </button>
          ) : null}
        </div>
      </header>
      {!publicOnly ? (
        <nav className="tabs" role="tablist" aria-label="Sections">
          <div
            className="tab-indicator"
            style={{
              width: `calc((100% - 16px) / ${visibleTabs.length})`,
              transform: `translateX(calc((100% + 8px) * ${Math.max(0, activeIdx)}))`,
            }}
            aria-hidden
          />
          {visibleTabs.map((t, i) => (
            <button
              key={t.id}
              role="tab"
              aria-selected={tab === t.id}
              className={"tab" + (tab === t.id ? " tab-active" : "")}
              onClick={() => setTab(t.id)}
            >
              <span className="tab-key" aria-hidden>{i + 1}</span>
              <span className="tab-label">{t.label}</span>
              <span className="tab-hint">{t.hint}</span>
            </button>
          ))}
        </nav>
      ) : null}
      <main className="content" role="tabpanel">
        <ErrorBoundary>
          {tab === "story" && <StoryTab />}
          {!publicOnly && tab === "live" && <LiveTab stats={stats} />}
          {!publicOnly && tab === "hot" && <HotTablesTab />}
          {!publicOnly && tab === "lab" && <LabTab />}
          {!publicOnly && tab === "admin" && <AdminTab stats={stats} />}
        </ErrorBoundary>
      </main>
      {!publicOnly ? (
        <footer className="app-footer">
          <span>Same contract as <code>shelfctl</code>. No new APIs.</span>
          <a href="/metrics" target="_blank" rel="noreferrer">raw /metrics</a>
          <a href="/stats" target="_blank" rel="noreferrer">raw /stats</a>
          <span className="foot-hint">press <kbd className="kbd">?</kbd> for shortcuts</span>
        </footer>
      ) : (
        <footer className="app-footer app-footer-public">
          <span>Public view · operational detail hidden</span>
        </footer>
      )}

      {!publicOnly ? (
        <CommandPalette
          open={paletteOpen}
          commands={commands}
          onClose={() => setPaletteOpen(false)}
        />
      ) : null}
      <HelpOverlay open={helpOpen} onClose={() => setHelpOpen(false)} />
    </div>
  );
}

function initialTab(publicOnly: boolean): TabId {
  if (publicOnly) return "story";
  const raw = window.location.hash.replace(/^#/, "").split("?", 1)[0];
  if (raw === "story" || raw === "live" || raw === "hot" || raw === "lab" || raw === "admin") return raw;
  // Legacy hashes from the prior 3-tab layout — preserve operator
  // muscle memory: "ops" lands on Live, "showcase" lands on Story.
  if (raw === "ops") return "live";
  if (raw === "showcase") return "story";
  return "story";
}

function FreshnessBadge({
  paused,
  ageSec,
  intervalSec,
  onToggle,
  onRefresh,
}: {
  paused: boolean;
  ageSec: number | null;
  intervalSec: number;
  onToggle: () => void;
  onRefresh: () => void;
}) {
  const stale = ageSec !== null && ageSec > intervalSec * 2 + 1;
  const label = paused
    ? "paused"
    : ageSec === null
    ? "live"
    : ageSec < 2
    ? "just now"
    : `${ageSec}s ago`;
  return (
    <span className={"freshness" + (paused ? " freshness-paused" : stale ? " freshness-stale" : "")}>
      <button
        className="freshness-toggle"
        onClick={onToggle}
        aria-label={paused ? "Resume polling" : "Pause polling"}
        title={paused ? "Resume polling (p)" : "Pause polling (p)"}
      >
        {paused ? (
          <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden>
            <path d="M2 1 L9 5 L2 9 z" fill="currentColor" />
          </svg>
        ) : (
          <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden>
            <rect x="2" y="1" width="2.5" height="8" fill="currentColor" />
            <rect x="5.5" y="1" width="2.5" height="8" fill="currentColor" />
          </svg>
        )}
      </button>
      <span className="freshness-label">{label}</span>
      <button
        className="freshness-refresh"
        onClick={onRefresh}
        aria-label="Refresh now"
        title="Refresh now (r)"
      >
        <svg width="11" height="11" viewBox="0 0 16 16" aria-hidden>
          <path
            d="M13 8a5 5 0 1 1-1.46-3.54L13 6V2"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          <path d="M13 2h-3" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        </svg>
      </button>
    </span>
  );
}

function HealthDots({ healthz, readyz }: { healthz: boolean | null; readyz: boolean | null }) {
  const dot = (label: string, state: boolean | null) => {
    const cls =
      state === null ? "health-dot health-pending" : state ? "health-dot health-ok" : "health-dot health-err";
    const txt = state === null ? "…" : state ? "ok" : "fail";
    return (
      <span className={cls} title={`${label}: ${txt}`} aria-label={`${label}: ${txt}`}>
        <span className="health-dot-inner" />
        <span className="health-dot-label">{label}</span>
      </span>
    );
  };
  return (
    <span className="health-strip" aria-label="Health probes">
      {dot("healthz", healthz)}
      {dot("readyz", readyz)}
    </span>
  );
}

function ThemeIcon({ mode }: { mode: "auto" | "light" | "dark" }) {
  if (mode === "light") {
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden>
        <circle cx="12" cy="12" r="4" fill="currentColor" />
        <g stroke="currentColor" strokeWidth="2" strokeLinecap="round">
          <path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.9 4.9l2.1 2.1M17 17l2.1 2.1M4.9 19.1l2.1-2.1M17 7l2.1-2.1" />
        </g>
      </svg>
    );
  }
  if (mode === "dark") {
    return (
      <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden>
        <path d="M20 14.5A8 8 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5z" fill="currentColor" />
      </svg>
    );
  }
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden>
      <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M12 3a9 9 0 0 1 0 18z" fill="currentColor" />
    </svg>
  );
}
