# INCIDENT — rep-1 cdp writes broken by !17873; revert packet

**Status:** writes blocked, revert ready, **NOT applied**.
**Severity:** P1 — every dbt-driven write to `cdp.*` on rep-1 fails.
**First seen:** 2026-04-27 14:23 UTC, query
`20260427_142345_00651_3tmhs` on rep-1, dbt-trino-1.9.2 connection
`trino_dbt_cdp_prod_data_replica1`.
**Surface:** `HIVE_WRITER_CLOSE_ERROR (16777242)` →
`io.trino.spi.TrinoException: Error committing write to Parquet
file` → `software.amazon.awssdk.services.s3.model.S3Exception
(Status Code: 405, Request ID: null)`.

---

## What broke

!17873 set rep-1's `cdp.properties` so that **all** S3 traffic for the
`cdp` catalog flows through the shelf shim:

```properties
# rep-1 cdp.properties — current state after !17873
s3.endpoint=http://shelf-1.shelf.alluxio.svc.cluster.local:9092
```

The shim is GET/HEAD only. From `shelf/shelfd/src/s3_shim.rs:46`:

```rust
pub fn router(state: Arc<ServerState>) -> axum::Router {
    axum::Router::new()
        .route(
            "/:bucket/*key",
            get(handle_get_object).head(handle_head_object),
        )
        .with_state(state)
}
```

Anything that isn't `GET` or `HEAD` returns axum's default 405. The
trace's `Request ID: null` is the diagnostic fingerprint — real AWS
S3 always populates a request ID; only an in-cluster proxy returning
a bare 405 leaves it empty.

### Failure chain (single query)

1. dbt's `iceberg_maintain_single_table` macro fired on rep-1 after
   `expire_snapshots` against `cdp_revenue.gold_batch_subject_schedules_pw`.
2. The post-action SQL:
   ```sql
   INSERT INTO cdp.admin.iceberg_maintenance_log
       (log_date, schema_name, table_name, action, status, detail, run_source)
   VALUES (CURRENT_TIMESTAMP, 'cdp_revenue', 'gold_batch_subject_schedules_pw',
           'expire_snapshots', 'OK',
           'mode=standard,isolated=true', 'dbt_auto_maintain');
   ```
3. Iceberg's `IcebergPageSink.closeWriter` fired
   `S3OutputStream.putObject` against
   `pw-data-cdp-prod-temp/warehouse/admin/iceberg_maintenance_log-…/data/log_date_day=2026-04-27/*.parquet`.
4. Request hit `shelf-1:9092` → axum 405.
5. ParquetWriter.close failed → Trino tried `DeleteObject` to clean
   the orphan → also 405 (suppressed `Failed to delete file` in the
   trace).
6. Whole INSERT terminated as `HIVE_WRITER_CLOSE_ERROR`.

### Blast radius

Any cdp-catalog write on rep-1 — not just dbt maintenance:

- `INSERT`, `CTAS`, `UPDATE`, `DELETE`, `MERGE` against any cdp table.
- `ALTER TABLE … EXECUTE optimize` / `expire_snapshots` /
  `remove_orphan_files`.
- Every Iceberg metadata commit (each write produces new manifest +
  snapshot files, all of which need to PUT to S3).

dbt iceberg-maintain runs nightly + ad-hoc, so the failure rate is
high until reverted.

Rep-2 is not affected — Metabase admin queries are read-only, so the
GET/HEAD-only shim is sufficient.

---

## The revert MR (deployments-repo)

### Title

```
trino-replica1: revert cdp.s3.endpoint to AWS S3 (revert !17873) — unblock writes
```

### Body

```markdown
## Summary

Reverts !17873. Restores rep-1's `cdp.properties.s3.endpoint` to the
canonical AWS S3 endpoint so writes succeed.

## Why

The shelf S3 shim (`shelfd:0.1.0-preview-3` and `-4`) is read-only
(GET + HEAD only — see shelf/shelfd/src/s3_shim.rs:46). After !17873
pointed rep-1's cdp catalog at `shelf-1:9092`, every write returned
HTTP 405 from the shim's axum router. This caused
`HIVE_WRITER_CLOSE_ERROR` on every cdp write (dbt's iceberg-maintain
macro, ad-hoc INSERTs, OPTIMIZE/EXPIRE_SNAPSHOTS).

Reference query: `20260427_142345_00651_3tmhs`, rep-1,
2026-04-27 14:23 UTC.

## What this does

Single-line revert of the rep-1 catalog properties:

```diff
   # rep-1 cdp.properties
-  s3.endpoint=http://shelf-1.shelf.alluxio.svc.cluster.local:9092
+  # s3.endpoint  -- removed; defaults to AWS S3 region endpoint
```

(Or whichever explicit value !17873 replaced — restore that.)

## Impact

- Rep-1 reads stop traversing shelf-1 → no read-cache benefit on
  rep-1 until SHELF-21 (write-passthrough in shim) ships as
  `shelfd:0.1.0-preview-5`. Rep-1 hit ratio for cdp goes from
  whatever it was → 0% (direct S3) until then.
- Rep-2 unchanged — Metabase admin reads continue to traverse
  shelf-2.
- All cdp writes on rep-1 succeed again immediately on next pod
  restart.

## Forward plan

SHELF-21 adds `PUT` / `POST` / `DELETE` / multipart proxy handlers
to the shim, with targeted Foyer + HEAD-LRU invalidation on
successful writes. Tracking doc:
shelf/shelfd/docs/design-notes/SHELF-21-shim-write-passthrough.md
(to be added). Once preview-5 is built, validated on rep-2 in
shadow mode, and rolled to all shelf pods, rep-1 cdp.s3.endpoint
will be re-pointed to shelf-1.

## Verify

After ArgoCD reconciles + rep-1 coordinator restarts:

```bash
# 1. cdp.properties no longer pins shelf-1
kubectl -n trino-db exec -c coordinator deploy/trino-coordinator -- \
  cat /etc/trino/catalog/cdp.properties | grep -i endpoint

# 2. Re-run the failing INSERT (or any small write) and watch it succeed.
trino --server <rep1> --catalog cdp --schema admin --execute \
  "INSERT INTO iceberg_maintenance_log
   (log_date, schema_name, table_name, action, status, detail, run_source)
   VALUES (CURRENT_TIMESTAMP, 'test', 'revert_smoke', 'noop', 'OK',
           'post-revert smoke', 'manual')"

# 3. dbt iceberg-maintain replays cleanly on its next schedule.
```
```

### Files to change

In whichever path holds rep-1's Trino catalog files in the
deployments-repo (mirrors what !17873 modified). One-line revert.

---

## One-shot apply (when MR is merged)

ArgoCD reconciles automatically; the Trino coordinator pod restart
picks up the new catalog file. To force the restart immediately:

```bash
kubectl -n trino-db rollout restart deploy/trino-coordinator
kubectl -n trino-db rollout restart sts/trino-worker
kubectl -n trino-db rollout status sts/trino-worker --timeout=10m
```

(Confirm exact resource names — names may differ per replica
release.)

## Rollback of the rollback (if you need to put shelf-1 back)

Once SHELF-21 ships and `shelfd:0.1.0-preview-5` is on all pods, the
deployments-repo can simply re-apply !17873:

```diff
   # rep-1 cdp.properties
+  s3.endpoint=http://shelf-1.shelf.alluxio.svc.cluster.local:9092
```

**Do not re-cut over until preview-5 is on all three shelf pods.**

---

## Verify the revert worked

- [ ] `cdp.properties` on rep-1 coordinator shows no
      `s3.endpoint=http://shelf-1...` line.
- [ ] Re-run the failed query — `INSERT INTO
      cdp.admin.iceberg_maintenance_log` succeeds.
- [ ] Grafana → Trino dashboard for rep-1: error rate on cdp writes
      drops to baseline (~0).
- [ ] Shelf-1 metrics on Grafana: GET/HEAD traffic from
      rep-1-coordinator drops to 0 (rep-1 no longer talks to shelf-1).
- [ ] dbt iceberg-maintain succeeds on next scheduled run.

## Lessons / follow-ups

1. **Pre-cutover smoke test missed writes.** The rep-1 cutover
   verification only spot-checked Metabase reads. Add a write smoke
   test to the cutover runbook: a no-op `INSERT INTO
   cdp.admin.iceberg_maintenance_log` BEFORE declaring success.
   Tracked in todo `cutover-write-smoke`.
2. **Shim's read-only nature wasn't surfaced loud enough in the
   cutover MR.** Add an explicit "writes will fail until SHELF-21"
   warning to the cutover-MR template.
3. **SHELF-21 is the structural fix.** See design note
   (forthcoming) — shim grows write-passthrough handlers using the
   same `aws_sdk_s3::Client` it already uses for origin reads, plus
   targeted Foyer + HEAD-LRU invalidation on success.
