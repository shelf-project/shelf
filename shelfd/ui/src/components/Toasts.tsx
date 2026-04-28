import { useCallback, useRef, useState } from "react";

export type Toast = {
  id: number;
  kind: "ok" | "err";
  title: string;
  body?: string;
};

export function useToasts() {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const idRef = useRef(0);

  const push = useCallback((t: Omit<Toast, "id">) => {
    idRef.current += 1;
    const id = idRef.current;
    setToasts((prev) => [...prev, { id, ...t }]);
    window.setTimeout(() => {
      setToasts((prev) => prev.filter((x) => x.id !== id));
    }, t.kind === "err" ? 8000 : 4000);
  }, []);

  const view = (
    <div className="toast-stack" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={"toast " + (t.kind === "ok" ? "toast-ok" : "toast-err")}>
          <div className="toast-title">{t.title}</div>
          {t.body ? <div className="toast-body">{t.body}</div> : null}
        </div>
      ))}
    </div>
  );

  return { push, view };
}
