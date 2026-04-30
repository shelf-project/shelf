import { useCallback, useState } from "react";

type Props = {
  text: string;
  /** Short accessible label, e.g. "Copy pod_id". */
  label?: string;
  /** Compact inline variant — fits next to mono text. */
  compact?: boolean;
};

export default function CopyButton({ text, label = "Copy", compact = false }: Props) {
  const [ok, setOk] = useState(false);

  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text);
      setOk(true);
      window.setTimeout(() => setOk(false), 900);
    } catch {
      // Clipboard denied (e.g. insecure context). Fall back to a
      // synthetic selection — works in every browser we support.
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand("copy");
        setOk(true);
        window.setTimeout(() => setOk(false), 900);
      } finally {
        document.body.removeChild(ta);
      }
    }
  }, [text]);

  return (
    <button
      type="button"
      className={"copy-btn" + (compact ? " copy-btn-compact" : "")}
      onClick={copy}
      aria-label={label}
      title={label}
    >
      {ok ? (
        <svg width="12" height="12" viewBox="0 0 16 16" aria-hidden>
          <path d="M13.5 4.5 6 12l-3.5-3.5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      ) : (
        <svg width="12" height="12" viewBox="0 0 16 16" aria-hidden>
          <rect x="4" y="4" width="9" height="9" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <rect x="2" y="2" width="9" height="9" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.4" opacity="0.6" />
        </svg>
      )}
    </button>
  );
}
