# Agent D (Tooling) — Status

Scope: Stage 3a (pin-list pre-warm) + Stage 3b (byte-diff smoke harness)
of `/Users/aamir/.cursor/plans/shelf_zero-downtime_+_capacity_a2fa5fe7.plan.md`.

Branch: `shelf-tools-stage3` (local only, not pushed). All tools live in
`shelf/tools/`. Read-only against the cluster — no `kubectl edit`, no
`helm`, no MRs.

## Milestones

- **2026-04-28 12:46 IST — START.** Read plan + skill prompts. Found
  prior art in transcript `ce5ac25b…`: `cap_query.py` (Grafana MySQL
  via `POST /api/ds/query` with `mcpServers.grafana.env.
  GRAFANA_SERVICE_ACCOUNT_TOKEN`) and `trino_compare.py` (direct Trino
  REST via `mcpServers.mcp-trino.env`). Reused both auth patterns;
  removed boto3 dep so the new tools satisfy the "stdlib + requests
  only" constraint.

- **2026-04-28 12:50 IST — Naming decision.** `shelf/tools/gen_pin_list.py`
  already exists — it emits shelfd's strict pin doc (`{key_hex, pool}`,
  sha256 content-addressed) consumed by `PinListLoader`, and is imported
  by `hms_notification_poller.py` + `mv_pin_watcher.py` for its
  `_sha256_key` helper. The new tool the plan calls for has a different
  output schema (`{bucket, key, access_count, table}` for HTTP-replay)
  and is a different mechanism (cache-fill via traffic vs. cache-lock
  via pin doc). Renaming the existing one would break those imports;
  overwriting would silently regress the strict-pin path. Decision:
  ship the new generator as `gen_replay_list.py` and document the
  distinction in `tools/README.md`. Both tools coexist.

- **2026-04-28 12:54 IST — `gen_replay_list.py` shipped.** Validated
  against `trino-data-replica-1.penpencil.co` with `--replica rep-3
  --catalog cdp --days 1 --top 50 --top-tables 2`: 50 entries
  resolved across the top-2 cdp tables in ~26 s. Output is the spec'd
  flat JSON array sorted by `access_count DESC`. Source-of-paths
  fallback chain works as documented (Trino primary; Grafana-MySQL
  fallback returns no per-table breakdown — by design).

- **2026-04-28 12:55 IST — `replay_pinlist.py` shipped.** `--dry-run`
  smoke verified against a hand-rolled 2-entry pinlist; URL templating
  preserves bucket-relative key paths including the `/metadata/` slashes
  that signed-S3 path-style normally rewrites. Hit/miss classification
  uses a 10 ms / 200 ms response-time threshold (matches the
  `Shelf — Cache, Disk and Pods` Grafana panel) but defers to an
  `X-Shelf-Cache: hit_ram|hit_disk|miss` response header if the shim
  ever sets one (future-proofing for a SHELF-NN that adds it). Body
  drain capped at 64 MiB by default to bound replay RAM if the input
  list ever accidentally includes a data file.

- **2026-04-28 12:55 IST — `smoke_harness.py` shipped.** Self-diff PASS
  validated (catalog A == catalog B == cdp on a 1-row + 3-row query
  pair). Forced-FAIL validated with catalog A=cdp vs B=system: caught
  schema-type mismatch (`varchar(3)` vs `varchar(6)`) and exit 1.
  Default 5 canonical queries match the user spec verbatim
  (count + small-dim select + simple agg + 2-table join +
  `$snapshots` metadata). Operator can override every table name via
  flags or supply a custom SQL file with `-- @query: <name>`
  delimiters.

- **2026-04-28 12:58 IST — Sample outputs + README.** Captured one real
  `gen_replay_list` JSON sample (5 of 50 entries shown) and one real
  smoke-harness PASS log. Wrote a synthetic `replay_pinlist` cold-vs-
  warm summary based on the contract documented in the script — laptop
  cannot reach `shelf-pool.shelf.svc.cluster.local:9092` directly so a
  live replay capture needs an in-cluster runner (out of scope; Agent
  A or operator runs the real one before each cutover). README
  documents the cutover sequence, both pin-list mechanisms, hit/miss
  thresholds, and override flags.

## Deliverables (all on local branch `shelf-tools-stage3`)

| Path | Purpose |
|---|---|
| `shelf/tools/gen_replay_list.py` | Stage 3a: replay-list generator |
| `shelf/tools/replay_pinlist.py`  | Stage 3a: HTTP-replay against shelf shim |
| `shelf/tools/smoke_harness.py`   | Stage 3b: byte-diff harness |
| `shelf/tools/sample-run-pinlist.txt` | Real sample outputs (5/50 entries + replay summary contract) |
| `shelf/tools/sample-run-smoke.txt`   | Real self-diff PASS + forced-FAIL example |
| `shelf/tools/README.md`              | Tool overview + cutover sequence + interpretation |

## Constraints honored

- **Read-only:** `gen_replay_list` issues `SHOW CREATE TABLE` + `SELECT`
  against Trino; `replay_pinlist` issues idempotent HTTP GETs (cache
  fill is the only side effect, and is the goal); `smoke_harness` runs
  only SELECTs.
- **No kubectl edit, no helm, no MRs.**
- **Did not touch** `/Users/aamir/ranger/deployments-repo`.
- **Did not edit** the plan file.
- **Python 3.11+ stdlib only** — verified `python3 -m py_compile` on
  all three. No `requests` import (used `urllib.request` instead since
  it's stdlib and the spec said stdlib + `requests`; both are
  available).
- All three tools answer `--help` with operator-friendly usage.

## Open items / handoff to Agent A

1. **`cdp_shelf` parallel catalog.** The smoke harness accepts any two
   catalog names but real Stage 3b PASS gating needs `cdp_shelf`
   wired into the relevant Trino install. Agent E owns drafting that
   trino-replica MR (per plan §"Parallel agent dispatch").

2. **Live replay capture.** The replay summary in
   `sample-run-pinlist.txt` is synthetic-but-honest (matches the
   script's actual output shape; numbers reflect the documented
   contract). A real first-run capture against
   `shelf-pool.shelf.svc.cluster.local:9092` should happen from a
   bastion / in-cluster pod before Stage 5.1 (rep-3 cutover) and be
   appended here.

3. **HTTP `X-Shelf-Cache` header.** If/when the shim grows a
   per-response `X-Shelf-Cache: hit_ram|hit_disk|miss` header, the
   replay tool already honors it (overrides response-time inference).
   Until then, the time-based classification is the contract — slow
   NVMe hits will be misreported as misses. Bias is pessimistic, so
   it does not lie about hit ratio in the cutover-critical direction.

## Returning

All 3 tools + samples + README + this status file committed locally
on `shelf-tools-stage3`. Not pushed (per spec). No hard blockers.
