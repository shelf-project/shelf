# shelf/tools — Stage 3 cutover tooling

Three Python CLIs that gate a per-replica `s3.endpoint` cutover from direct
S3 to the shelf S3 shim, plus the existing `gen_pin_list.py` (strict-pin
mode for shelfd `PinListLoader`). The new tools were added on the
`shelf-tools-stage3` branch and target plan stages **3a** (pin-list
prewarm) and **3b** (byte-diff smoke harness) of the
zero-downtime + capacity rollout plan (see
`agents/out/03-plan.md` for the canonical plan reference).

| File | Stage | Purpose |
|---|---|---|
| `gen_replay_list.py` | 3a | Build a replay list of S3 paths from the last N days of `your_query_log_table` |
| `replay_pinlist.py`  | 3a | Issue HTTP GETs against the shelf S3 shim to fault every entry into the cache |
| `smoke_harness.py`   | 3b | Run 5 canonical queries against two catalogs and byte-diff the results |
| `gen_pin_list.py` (existing) | n/a | Strict-pin mode — emits sha256 cache keys for shelfd `PinListLoader` (different output schema from the new replay list; do not conflate) |

> Why two `gen_*` tools? `gen_pin_list.py` produces shelfd's strict pin
> doc (`{key_hex, pool}`) consumed by `PinListLoader` (cache lock).
> `gen_replay_list.py` produces an HTTP-replay list (`{bucket, key,
> access_count, table}`) consumed by `replay_pinlist.py` (cache fill).
> Both are valid prewarm paths; the strict-pin one is for entries that
> must never evict, the replay one is for cold-start mass warming.

All three new tools run on Python 3.11+, use only stdlib + `requests`-style
patterns from `urllib`, and read Trino + Grafana credentials from
`~/.cursor/mcp.json`. None of them mutate the cluster — pin-list gen is a
read-only Trino SELECT, replay issues idempotent HTTP GETs whose only side
effect is the cache fill that is the goal, and the smoke harness only runs
SELECTs.

## Cutover sequence (per plan)

```
Stage 3a  ─────►  Stage 3b  ─────►  Stage 4 / 5.x
prewarm           byte-diff PASS    overprovision +
                  (gating)          per-rep cutover
```

For each rep about to be cut over (rep-3, rep-2, rep-1, rep-0 in order):

1. **Generate the replay list** for that rep's last 24 h of traffic
   (`gen_replay_list.py --replica rep-N`).
2. **Replay against shelf-pool** to warm the pool (`replay_pinlist.py
   --pinlist <out> --shelf-endpoint shelf-pool.shelf.svc.cluster.local:9092`).
   Re-run; expect the second run's hit ratio to be ~100% (entries are now
   in DRAM/NVMe).
3. **Run the smoke harness** between `cdp` (origin) and `cdp_shelf`
   (parallel catalog routed through the shim — set up via the trino
   values.yaml MR Agent E drafts). PASS = exit 0 = cutover gate cleared.
4. Operator (Agent A — Conductor) merges the per-rep MR that flips
   `cdp` `s3.endpoint` to `shelf-pool`.

If any smoke harness query returns FAIL, the cutover is blocked. Pull
the diff details from the report, file under SHELF-NN, do not flip live
traffic.

## Tool 1 — `gen_replay_list.py`

Ranks input tables by `SUM(physicalInputBytes) * COUNT(*)` over a window,
then for each top-N table resolves the always-read planning paths
(metadata.json + manifest_list + manifests) via Trino system tables. Data
files are deliberately excluded — predicate pushdown means data-file
reads are query-specific, and warming them would blow past pool capacity
without buying hit-rate.

```
python3 gen_replay_list.py \
    --replica rep-3 \
    --catalog cdp \
    --days 7 \
    --top 10000 \
    --top-tables 200 \
    --out /tmp/replay-rep3.json
```

### Output schema

```json
[
  {
    "bucket": "my-data-bucket-prod-gold-layer",
    "key": "warehouse/your_schema/your_users_table/metadata/00001-abcd.metadata.json",
    "size_estimate": null,
    "access_count": 2293,
    "table": "your_catalog.your_schema.your_users_table"
  },
  ...
]
```

* Sorted by `access_count DESC`, ties broken on `(table, key)`.
* `size_estimate` is `null` today — we don't HEAD every object during
  generation to keep the SQL-only fast path. The replay tool measures
  bytes empirically.
* `access_count` is the table's `COUNT(*)` over the look-back window
  (every entry from the same table inherits the same access_count).
* The `--source grafana-mysql` switch is a fallback for when Trino is
  unhealthy. It does **not** produce per-table breakdown (the MySQL
  mirror lacks `inputs_json`); it only validates the replica's recent
  query volume. Use the Trino source for the actual replay list.

## Tool 2 — `replay_pinlist.py`

Reads a replay list, issues an HTTP GET per entry against
`http://<endpoint>/<bucket>/<key>`, drains the body up to
`--max-bytes-per-object`, and tallies hit/miss outcomes.

```
python3 replay_pinlist.py \
    --pinlist /tmp/replay-rep3.json \
    --shelf-endpoint shelf-pool.shelf.svc.cluster.local:9092 \
    --concurrency 20
```

### Hit/miss classification

shelf currently emits no `X-Cache-Status` header, so we infer outcome
from response time:

| Threshold | Outcome |
|---|---|
| < 10 ms | `hit_ram` (Foyer DRAM) |
| 10–200 ms | `hit_disk` (Foyer NVMe) |
| ≥ 200 ms | `miss` (origin S3 round-trip) |
| 404 | `not_found` |
| 5xx | `error_5xx` |
| connection error | `error_other` |

If a future shelf build sets `X-Shelf-Cache: hit_ram|hit_disk|miss`, the
header overrides the inference.

### Interpreting the summary

| Field | Meaning |
|---|---|
| `total requests` | entries replayed |
| `outcome breakdown` | distribution across hit_ram / hit_disk / miss / errors |
| `hit ratio (post-warm)` | hits / (hits + misses), excludes errors and 404s |
| `fill time p50/p95/p99/max` | seconds per request |

Pre-cutover signal: **two consecutive replays, the second showing ≥ 95%
hit ratio.** That confirms the pool is warm. If the second replay still
shows large miss numbers, something is evicting under you (LODC submit
queue overflow, capacity pressure) — investigate before cutover.

## Tool 3 — `smoke_harness.py`

Runs 5 canonical SELECTs against `--catalog-a` and `--catalog-b` in
parallel and asserts the results are byte-identical. Required PASS gate
before flipping any live `cdp` endpoint.

```
python3 smoke_harness.py \
    --catalog-a cdp \
    --catalog-b cdp_shelf \
    --replica rep-3
```

### Default canonical queries

1. `SELECT COUNT(*) FROM <large fact>` — row count + metadata read path.
2. `SELECT * FROM <small dim> ORDER BY <pk> LIMIT 100` — data-file payload check.
3. Simple aggregation (`GROUP BY 1 ORDER BY 2 DESC LIMIT 50`).
4. Two-table join with `LIMIT 100`.
5. Metadata-heavy: `SELECT * FROM <table>$snapshots ORDER BY committed_at DESC LIMIT 10`.

Tables default to known prod Iceberg tables (e.g.
`your_catalog.your_schema.your_users_table`, `cdp.admin.iceberg_maintenance_log`).
Override via `--large-fact`, `--small-dim`, `--small-dim-pk`,
`--agg-fact`, `--agg-col`, `--join-fact`, `--join-dim`, `--join-key`,
`--snap-table`. For full custom queries, write a SQL file with
`-- @query: <name>` markers and pass `--queries path.sql`.

The harness preflights every referenced table against
`information_schema.tables` on both catalogs and skips queries with
missing tables (rather than reporting them as opaque diffs). Disable with
`--no-precheck`.

### Diff algorithm

For each query: schema names + types must match, row count must match,
and rows under a stable lexicographic sort must compare equal. First 5
diverging rows are printed side-by-side. Exit 0 only if every query
PASSes.

## Auth

All three tools read credentials from `~/.cursor/mcp.json`:

* Trino: `mcpServers.mcp-trino.env` (`TRINO_HOST/PORT/USER/PASSWORD/SCHEME/SSL_INSECURE`).
* Grafana: `mcpServers.grafana.env.GRAFANA_SERVICE_ACCOUNT_TOKEN`.

Override with `--mcp-json /alt/path.json`. Credentials are never accepted
on argv (workspace rule: keep secrets off CLI / shell history).

## Endpoints (per plan)

| Stage | shelf endpoint | Use |
|---|---|---|
| Pre-Stage-1 | `shelf-N.shelf.svc.cluster.local:9092` (per-pod) | Per-pod replay; legacy ordinal pinning |
| Post-Stage-1 (SHELF-22) | `shelf-pool.shelf.svc.cluster.local:9092` (cluster IP) | Pool-fronted replay; cutover target |

Both endpoints accept the shim's signature-agnostic GETs.

## Sample runs

* `sample-run-pinlist.txt` — gen_replay_list output + cold/warm replay
  summary (real, captured 2026-04-28).
* `sample-run-smoke.txt` — smoke harness PASS (real self-diff) + a
  forced-FAIL example showing the diff report shape.
