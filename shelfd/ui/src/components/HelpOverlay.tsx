type Props = {
  open: boolean;
  onClose: () => void;
};

const SHORTCUTS: { keys: string; label: string }[] = [
  { keys: "1 / 2 / 3", label: "Switch to Ops, Admin, Showcase" },
  { keys: "⌘K / Ctrl+K", label: "Command palette" },
  { keys: "r", label: "Refresh /stats, /metrics, /admin/ring now" },
  { keys: "p", label: "Pause / resume live polling" },
  { keys: "t", label: "Cycle theme (auto → light → dark)" },
  { keys: "?", label: "Toggle this help" },
  { keys: "Esc", label: "Close modals and overlays" },
];

export default function HelpOverlay({ open, onClose }: Props) {
  if (!open) return null;
  return (
    <div className="modal-backdrop" role="dialog" aria-label="Keyboard shortcuts" onClick={onClose}>
      <div className="modal" style={{ width: "min(520px, 92vw)" }} onClick={(e) => e.stopPropagation()}>
        <h3>Keyboard shortcuts</h3>
        <p style={{ margin: "0 0 12px" }}>
          Every action also has a button. The palette is optional — it's just fast.
        </p>
        <table className="shortcut-table">
          <tbody>
            {SHORTCUTS.map((s) => (
              <tr key={s.keys}>
                <td><kbd className="kbd">{s.keys}</kbd></td>
                <td>{s.label}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="button-row" style={{ justifyContent: "flex-end" }}>
          <button className="btn" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  );
}
