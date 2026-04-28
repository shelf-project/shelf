import type { RingRow } from "../api/client";
import CopyButton from "./CopyButton";

type Props = { rows: RingRow[] | null; self?: string | null };

export default function RingTable({ rows, self }: Props) {
  if (!rows) return <div className="empty">loading ring…</div>;
  if (rows.length === 0) return <div className="empty">no ring members reported</div>;
  const sorted = [...rows].sort((a, b) => a.pod_id.localeCompare(b.pod_id));
  return (
    <table className="ring-table">
      <thead>
        <tr>
          <th>pod_id</th>
          <th style={{ textAlign: "right" }}>weight</th>
          <th>healthy</th>
          <th aria-label="copy" />
        </tr>
      </thead>
      <tbody>
        {sorted.map((r) => {
          const isSelf = self && r.pod_id === self;
          return (
            <tr key={r.pod_id} className={isSelf ? "ring-self" : ""}>
              <td>
                {r.pod_id}
                {isSelf ? <span className="ring-self-badge">self</span> : null}
              </td>
              <td style={{ textAlign: "right" }}>{r.weight.toFixed(3)}</td>
              <td className={r.healthy ? "ring-healthy-y" : "ring-healthy-n"}>
                {r.healthy ? "yes" : "no"}
              </td>
              <td style={{ textAlign: "right", width: 32 }}>
                <CopyButton text={r.pod_id} label={`Copy ${r.pod_id}`} compact />
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
