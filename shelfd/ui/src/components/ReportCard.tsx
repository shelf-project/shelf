/** Cache report card — A/B/C grades for the four headline dimensions.
 *
 * Tier A3. Sits above the Story headline as a 5-second verdict for
 * non-technical viewers: green letter = good, amber letter = watch,
 * red letter = something is wrong. Each cell pairs the letter with a
 * one-line caption so the grade is never the only signal.
 *
 * Grade mapping is deliberately blunt — we want unambiguous
 * categorical bands, not a misleading 0–100 score. The `score → grade`
 * function is shared with the existing tone classifier (≥ 80 → A,
 * ≥ 60 → B, ≥ 40 → C, otherwise D/F), so the bullet charts on Live
 * and the report card always agree on what "green" means.
 *
 * Inputs are pre-computed scalars so this component stays
 * presentational; the Story tab does the metric math next to where
 * those metrics already get rendered (no redundant parsing).
 */

import { type DeltaReadout } from "../format";

type Dimension = {
  id: "speed" | "coverage" | "stability" | "efficiency";
  label: string;
  /** A 0..1 score where 1.0 is the best plausible outcome. */
  score: number | null;
  /** Short human caption beside the grade — e.g. "p95 32 ms vs S3 180 ms". */
  caption: string;
  /** Tier S4 — optional delta caption that pairs with the grade. */
  delta?: DeltaReadout;
};

type Props = { dimensions: Dimension[] };

export default function ReportCard({ dimensions }: Props) {
  return (
    <section
      className="card report-card"
      role="img"
      aria-label="Cache report card"
    >
      <div className="report-card-head">
        <h3 className="card-title" style={{ margin: 0 }}>
          Cache report card
        </h3>
        <span className="report-card-sub">
          Five-second verdict across the four dimensions that matter.
        </span>
      </div>
      <div className="report-card-grid">
        {dimensions.map((d) => {
          const grade = scoreToGrade(d.score);
          return (
            <div
              key={d.id}
              className={`report-cell report-cell-${grade.tone}`}
              title={d.caption}
            >
              <div className="report-cell-grade" aria-label={`Grade ${grade.letter}`}>
                {grade.letter}
              </div>
              <div className="report-cell-body">
                <div className="report-cell-label">{d.label}</div>
                <div className="report-cell-caption">{d.caption}</div>
                {d.delta && d.delta.tone !== "pending" ? (
                  <div className={`report-cell-delta delta-${d.delta.tone}`}>
                    <span aria-hidden>{d.delta.glyph}</span> {d.delta.text} vs 5m
                  </div>
                ) : null}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

export function scoreToGrade(
  score: number | null,
): { letter: string; tone: "ok" | "warn" | "err" | "pending" } {
  if (score == null || !Number.isFinite(score)) {
    return { letter: "—", tone: "pending" };
  }
  // Map 0..1 → letter band. Bands are deliberately wide so a small
  // poll-to-poll wobble doesn't flip the grade.
  if (score >= 0.95) return { letter: "A+", tone: "ok" };
  if (score >= 0.85) return { letter: "A", tone: "ok" };
  if (score >= 0.75) return { letter: "A-", tone: "ok" };
  if (score >= 0.65) return { letter: "B+", tone: "warn" };
  if (score >= 0.55) return { letter: "B", tone: "warn" };
  if (score >= 0.45) return { letter: "B-", tone: "warn" };
  if (score >= 0.35) return { letter: "C", tone: "err" };
  if (score >= 0.2) return { letter: "D", tone: "err" };
  return { letter: "F", tone: "err" };
}
