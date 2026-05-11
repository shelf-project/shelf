# SHELF-32 F2 P2-conditional gate evaluation — 2026-04-30

> Phase 2 Track C — P1.5 of `rc.6_release_plan_cc5d2311.plan.md`. Decide whether the
> Foyer 0.12.2 → 0.22.3 bump (Dependabot **PR #22**) clears the F2 P2-conditional
> gate from the Apr 30 deep-research report.
>
> **F2 gate, verbatim:** SHELF-35 replay must show **≥ 5 percentage points** hit-ratio
> lift for Sieve over the tuned-S3-FIFO baseline before SHELF-32 (and therefore
> the Foyer 0.22 bump that vehicles it) promotes from P2-conditional to P0.

## 1. Verdict

**CANNOT EVALUATE — DEFER P1.5 to rc.7.**

The harness exists; the production-trace data and the Sieve policy implementation
that the gate measures do not. There is no Sieve-vs-tuned-S3-FIFO delta to compare
against the 5 pp threshold today, and no plausible path to producing one inside the
rc.6 prep window without two upstream tickets landing first.

## 2. Replay output state at write-time

| Artefact | Source | State on `origin/main` @ `ffb4fa2` |
|---|---|---|
| `tools/replay/` Python harness | PR #41 (merged 2026-04-30 04:30 UTC) | Present — `main.py`, `policies.py`, `simulator.py`, `trace.py`, `sql/extract_trace_30d.sql`, 14 unit tests passing in 0.011 s per the SHELF-35 handoff. |
| `tools/replay/policies.py` registered policies | PR #41 | `LRU`, `FIFO`, `S3FIFO`, `BeladyMin` only. **Sieve is explicitly NOT implemented.** Source comment: *"Sieve / W-TinyLFU / 3L-Cache are deliberately deferred to SHELF-35b — they each warrant their own ADR and their own per-policy TSV; v1 of the harness is the *infrastructure*, not every algorithm."* |
| `agents/out/SHELF-35/` | PR #41 | Only `handoff.md`. **Zero TSV files** — no production-trace replay has been committed. |
| Synthetic smoke run | SHELF-35 handoff | Wrote `/tmp/shelf35-smoke.tsv` (uncommitted, ephemeral) for 5000 queries × 80 tables × 3 capacities × 4 policies. Showed Belady strictly dominates LRU/FIFO/S3-FIFO at every capacity below saturation. **Did NOT include Sieve** — the harness cannot run a policy it does not implement. |
| Production trace extraction | Operator action listed in SHELF-35 handoff | **Not yet performed.** Quote: *"Operator action: run `tools/replay/sql/extract_trace_30d.sql` against rep-3, export the CSV, replay against rep-1 + rep-2 capacity sweeps, file the resulting TSVs under `agents/out/SHELF-35/replay-<algo>-<date>.tsv`."* |
| PR #22 (Foyer bump) | Dependabot | OPEN, `mergeable: MERGEABLE`, `mergeStateStatus: CLEAN`, `isDraft: false`, `headRefOid f98e442`. Latest comment Apr 30 05:45 UTC by `aamir306` re-confirming the P2-conditional re-tier. |

## 3. Hit-ratio delta — not measurable

The gate threshold is `(Sieve hit ratio) − (tuned-S3-FIFO hit ratio) ≥ 5 pp` on a
production trace. Computing it requires three inputs that are **simultaneously**
absent:

1. **A Sieve implementation in the harness.** SHELF-35 v1 ships LRU / FIFO / S3-FIFO /
   Belady; Sieve is deferred to SHELF-35b/c per the in-source comment and the handoff
   table. No 50-LOC subclass exists yet, and per the same handoff, "adding a new
   policy is a 50-LOC subclass of the existing protocol *once the gate-validation
   case is made*" — so SHELF-35b/c is itself a separate ticket, not a quick fix.
2. **A real trace.** Synthetic traces cannot legitimately measure this gate; the
   handoff's own validation discipline says *"Replay output must reproduce the live
   cluster's last-7-day hit ratio within ±2 pp, otherwise discard the run; do NOT
   use as a baseline."* That requires an operator-extracted CSV from rep-3, which
   is the pending operator action above.
3. **The tuned-S3-FIFO baseline parameters.** rc.5 ships rep-1 on a tuned S3-FIFO
   small-queue ratio; the harness's `S3FIFO` class faithfully models the
   pre-bump default but the *tuned* parameters from rep-1 prod (`small_queue_capacity_ratio`,
   promote-on-≥-2 thresholds) need to be plumbed into the harness as a config
   knob before the comparison is fair to the F1 deep-research finding.

Even an optimistic timeline that lands SHELF-35c (Sieve + ADR) and the rep-3 trace
extraction inside the rc.6 prep window would still leave the parameter-plumbing
fix as a third blocker — which is why this evaluation flags **defer**, not "almost
ready".

## 4. Recommendation

| Item | Action |
|---|---|
| **rc.6 inclusion** | **No.** Drop P1.5 from rc.6 scope. Do not unpark PR #22; do not author the migration PR. |
| **PR #22 state** | Leave OPEN, parked. The Apr 30 `aamir306` comment already records the P2-conditional rationale; no additional comment required this week — would just be noise. |
| **Re-evaluation cadence** | **Weekly during rc.6 prep window** (every Wednesday until rc.7 scope locks). Each check is a 5-min grep — not a worker dispatch. |
| **Block lift conditions (all three required)** | (a) `agents/out/SHELF-35/replay-*.tsv` files committed from a real rep-3 30-day trace; (b) a Sieve policy class lands in `tools/replay/policies.py` (= SHELF-35c); (c) one of those TSVs shows `(Sieve hit_ratio − S3FIFO hit_ratio) ≥ 0.05` at the production capacity tier (≈ 14 GiB DRAM). |
| **If gate stays unevaluatable through rc.6 → rc.7 cycles** | Re-tier SHELF-32 from P2-conditional to P3 (track-only) and close PR #22 as `not-planned` with a link to this doc. The cold-cache restart cost is a permanent floor; without ≥ 5 pp evidence the bump is net-negative. |

### Weekly check command

```bash
cd /Users/aamir/trino/shelf
git fetch origin main --quiet
echo "--- replay TSVs committed: ---"
git ls-tree -r origin/main agents/out/SHELF-35/ | grep -c '\.tsv$'
echo "--- Sieve in policies.py (= SHELF-35c landed): ---"
git show origin/main:tools/replay/policies.py | grep -c '^class Sieve'
echo "--- PR #22 state: ---"
gh pr view 22 --repo shelf-project/shelf --json state,mergeable --jq .
```

If line 1 = 0 OR line 2 = 0 → gate cannot evaluate, defer one more week.
If both line 1 ≥ 1 AND line 2 ≥ 1 → re-run this evaluation properly.

## 5. Migration plan — not delivered (gate did not clear)

Per user constraints (*"If still draft / open: this task does NOT push fixes to
its branch (Dependabot owns it)"*) and per the verdict above, the migration plan
is not authored. A skeleton breakage map is included below for the day the gate
clears, so the next evaluator does not re-derive it from scratch.

### Foyer 0.12.2 → 0.22.3 breakage map (skeleton, not load-bearing)

| Site | Foyer 0.12 surface | Foyer 0.22 surface | Notes |
|---|---|---|---|
| `shelfd/src/store.rs` rowgroup pool builder | `LargeEngineOptions::with_flushers(...).with_buffer_pool_size(...).with_submit_queue_size_threshold(...)` | `BlockEngineConfig` (partition-based) | Knob names retained per workspace memory PR #22 stop-condition note. NVMe on-disk layout breaks. |
| `shelfd/src/store.rs` device builder | `DirectFsDeviceOptions::with_file_size(...)` | `FsDeviceBuilder` — `with_file_size` removed | Partition abstraction replaces single file-size partitioning. |
| `shelfd/src/store.rs` cache constructor | `EvictionPolicy::Lru` enum | Enum shape changed | Verify variant names against 0.22 docs. |
| `CapacityEvictionListener` impl | `EventListener::on_memory_release(key, value)` | `on_leave(reason: Event, key, value)` where `Event ∈ {Evict, Replace, Remove, Clear}` | Improvement, not regression — addresses the SHELF-A5 "everything counts as `capacity`" caveat. Re-label `shelf_evictions_total{reason}` accordingly. |
| Hit-on-NVMe accounting | `cache.stats()` | Removed | Two call-sites need replacement. Likely via per-listener counter. |
| Key trait | `Key: Hash + Eq + Send + Sync` | `Key: Hash + Eq + Send + Sync + Code` | ~10 LOC `Code` impl on Shelf's existing key type. |
| LODC log target | `target = "foyer_storage::large::generic"` for "submit queue overflow" | New module path | `shelfd/src/lodc_backpressure.rs` log-target filter must update; `shelf_lodc_drops_total{reason="submit_queue_overflow"}` PromQL stays. |
| `HybridCache::get` (post-0.17) | `get(&key)` | `obtain(&key)` | Per workspace memory PR #22 note. |
| Hash function | Foyer 0.12 hasher | Different in 0.17+ | Not visible to Shelf's content-addressed keys (we hash externally with sha256), but Foyer's internal index is incompatible — cold cache mandatory regardless of any other consideration. |

### Cutover strategy (when the day comes)

Same shape as the dev HMS Hive 3.1.3 image migration:

1. **Parallel deploy** — `shelf-pool-v2` StatefulSet with `shelfd:1.0.0-rc.X+foyer22`, identical replicas/resources, zero traffic.
2. **Service-level cutover** — flip `shelf-pool` Service selector from `version=v1` → `version=v2`. Single API call; reverts in seconds via the same patch.
3. **Cold-cache wall-clock** — every shelf pod loses NVMe and must re-warm from S3 via the SHELF-23 peer-fetch path. Expected: same shape as a fresh rc.5 boot (Foyer NVMe replay completed within 5 min `startupProbe` per workspace memory) but **without** any pre-existing partner data. **Estimate: 60–120 min to clear the post-cutover ICEBERG_CANNOT_OPEN_SPLIT spike**, longer if the cutover lands on rep-0's heaviest hour.
4. **Quiet-window only** — same governance as the rep-0 cutover lock: `14:00–15:30 IST` or `22:30–00:00 IST`, never `09:00–11:00 IST`.
5. **Auto-rollback armed** — same 7-trigger watcher as rep-0 day-1 cutover (workspace memory rolling-restart playbook).

### Acceptable risk to re-confirm before unparking

- The current 4-pod rep-1 / 6-pod rep-0 pool is on rc.5's tuned S3-FIFO. F1 deep-research already concluded S3-FIFO+W-TinyLFU buys 10-30 % miss-rate reduction over plain LRU. SHELF-33 W-TinyLFU admission gate has already merged (#46 → `1.0.0-rc.7` track) and is default-OFF. **A non-zero share of the lift Sieve would be measured against has therefore already been picked up in-tree without the Foyer bump cost.** The gate-clearance bar is "Sieve > tuned S3-FIFO + W-TinyLFU", not "Sieve > stock LRU". Re-state this when the gate is re-evaluated.

## 6. References

- Source plan: `/Users/aamir/.cursor/plans/rc.6_release_plan_cc5d2311.plan.md` § P1.5
- Workspace memory: `/Users/aamir/trino/AGENTS.md` — Apr 30 deep-research F1–F5 verdicts, F2 entry
- PR #41 (SHELF-35 v1, MERGED 2026-04-30 04:30 UTC, 9 files / ~1247 additions)
- PR #22 (Foyer bump, OPEN, MERGEABLE, parked at >200 LOC migration stop-condition)
- SHELF-35 handoff: `agents/out/SHELF-35/handoff.md` on `origin/main` @ `c3f9c1c`
- ADR-0010, BLUEPRINT.md §10.4 (replay benchmark spec)
- Trino #26436 / ADR-0005 (`SplitCompletedEvent` removal — limits SHELF-35 v1 to `(query, table)` granularity)

---
*Eval written by Phase 2 Track C P1.5 worker. Read-only research; no code changes,
no PR mutations, no cluster mutations, no main push.*
