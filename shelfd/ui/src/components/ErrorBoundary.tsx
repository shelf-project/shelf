import { Component, type ReactNode } from "react";

type Props = { children: ReactNode };
type State = { error: Error | null };

/** Catches render errors so a bug in one tab (typically a new
 * metrics parser case) doesn't blank the entire operator interface. */
export default class ErrorBoundary extends Component<Props, State> {
  override state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  override componentDidCatch(error: Error) {
    // Surfaces in the browser console so operators can copy-paste
    // into an issue without inspecting React devtools.
    // eslint-disable-next-line no-console
    console.error("shelfd UI render error:", error);
  }

  private reset = () => this.setState({ error: null });

  override render() {
    if (!this.state.error) return this.props.children;
    return (
      <div className="card" style={{ borderColor: "var(--err)" }}>
        <h3 className="card-title" style={{ color: "var(--err)" }}>UI render error</h3>
        <p style={{ margin: "4px 0 8px", color: "var(--fg-dim)" }}>
          The admin surface kept running — only this tab crashed. Try again once and
          report the stack below if it reproduces.
        </p>
        <pre
          style={{
            whiteSpace: "pre-wrap",
            fontFamily: "var(--mono)",
            fontSize: 12,
            background: "var(--bg)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            padding: 10,
            margin: "0 0 12px",
          }}
        >
          {this.state.error.stack || this.state.error.message}
        </pre>
        <button className="btn" onClick={this.reset}>Try again</button>
      </div>
    );
  }
}
