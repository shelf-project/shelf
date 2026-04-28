type Props = {
  data: number[];
  width?: number;
  height?: number;
  /** Colour token; falls back to `--accent`. */
  stroke?: string;
  /** Draw a gradient fill under the line. */
  filled?: boolean;
  /** Render even with one sample (flat line). Defaults to false. */
  showSingle?: boolean;
  /** Optional fixed maximum. Defaults to the data max. */
  max?: number;
  label?: string;
};

/** Minimal sparkline. 40-line SVG — no d3, no recharts. The shelf
 * bundle stays tiny. */
export default function Sparkline({
  data,
  width = 180,
  height = 32,
  stroke = "var(--accent)",
  filled = true,
  showSingle = false,
  max,
  label,
}: Props) {
  if (!data.length) return <svg className="sparkline" width={width} height={height} aria-hidden />;
  if (data.length < 2 && !showSingle) {
    return <svg className="sparkline" width={width} height={height} aria-hidden />;
  }

  const lo = Math.min(...data);
  const hi = Math.max(max ?? -Infinity, ...data);
  const span = hi - lo || 1;
  const stepX = width / Math.max(1, data.length - 1);
  const y = (v: number) => height - 2 - ((v - lo) / span) * (height - 4);

  const pts = data.map((v, i) => `${(i * stepX).toFixed(2)},${y(v).toFixed(2)}`);
  const line = pts.join(" ");
  const area = `0,${height} ${line} ${width},${height}`;

  const id = `sl-grad-${Math.abs(hashString(stroke + data.length))}`;
  return (
    <svg
      className="sparkline"
      width={width}
      height={height}
      role="img"
      aria-label={label ?? "sparkline"}
    >
      {filled ? (
        <>
          <defs>
            <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={stroke} stopOpacity="0.35" />
              <stop offset="100%" stopColor={stroke} stopOpacity="0" />
            </linearGradient>
          </defs>
          <polygon points={area} fill={`url(#${id})`} />
        </>
      ) : null}
      <polyline points={line} fill="none" stroke={stroke} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={((data.length - 1) * stepX).toFixed(2)} cy={y(data[data.length - 1]).toFixed(2)} r="1.8" fill={stroke} />
    </svg>
  );
}

function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return h;
}
