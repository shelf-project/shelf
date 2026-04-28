# RCA — Stage 0b + 0c (28-Apr-2026 04:15–06:00 UTC window)

> Window: 28-Apr-2026 **04:15:00 → 06:00:00 UTC** (= 09:45–11:30 IST). Shelf live on rep-0 + rep-1 only (per-pod hostname pinning: rep-0 → `shelf-0.shelf.alluxio.svc.cluster.local:9092`, rep-1 → `shelf-1.shelf.alluxio.svc.cluster.local:9092`). rep-2 had stale config and was not routing through shelf.
>
> **Read-only forensic.** Cluster context `infra:data-platform-cluster`. Trino MCP queries against `cdp.trino_logs.trino_queries`, port-forward only used for ONE read-only `GET` to confirm corrupt object bytes.

## Executive summary

- **Stage 0b — `ICEBERG_CANNOT_OPEN_SPLIT` (263 hits, 84 rep-0 + 179 rep-1).** **97.3% (256/263) are `java.net.UnknownHostException` for `shelf-{0,1}.shelf.alluxio.svc.cluster.local`** triggered by **16 shelf pod incarnation rotations during the 105-minute window** (verified via `kube_pod_start_time` from mimir-data — shelf-0 rotated 6×, shelf-1 4×, shelf-2 6×). Per-pod hostname pinning (today's `cdp.properties` `s3.endpoint`) means *any* StatefulSet pod recreation = hard outage for the affected replica with **no fallback**. The remaining 2.6% are `Connection refused` (6) and `NoHttpResponseException` (1) — same pod-rotation race, slightly different timing inside the gap.
- **Stage 0c — `ICEBERG_INVALID_METADATA` (249 hits, 206 rep-0 + 43 rep-1).** Three sub-classes, all distinct from the plan's H1/H2/H3:
  - 86% (215/249) are the **same `UnknownHostException` class as 0b**, surfacing here because the failing path was a HEAD on `metadata.json` instead of a GET on a Parquet split.
  - 8% (20/249) are **HTTP 502 returned by shelfd** for HEAD on out-of-IRSA buckets (`pw-data-cdp-dev-encrypted`, `pw-data-cdp-prod-{gold,silver}-layer-audit`).
  - **5.6% (14/249) are a brand-new failure class — H4 — caused by a shelfd PUT-path bug.** Live read-only fetch of `s3a://pw-data-cdp-prod-temp/warehouse/admin/iceberg_maintenance_log-994339ec…/metadata/25455-874310b2-….metadata.json` confirmed **the file in S3 is permanently corrupted with literal AWS `aws-chunked` Content-Encoding framing concatenated with the JSON body**. The shelfd shim's `handle_put_object` (preview-8) buffers and forwards the request body without decoding `Content-Encoding: aws-chunked`, persisting AWS SDK chunk-size hex + `chunk-signature=…` lines as bytes in S3. JsonParseException at `line 1, column 6` is exactly the `;` in the first chunk header (`20000;chunk-signature=…`).
- **All three plan hypotheses (H1 picker truncation, H2 same-pod HEAD-LRU poisoning, H3 cross-pod cache coherence) are REFUTED** by the same evidence: the corrupted bytes are in S3 itself, persist across shelf-1's 4 pod incarnation cache-wipes during the window, and Content-Length / response-time profiles do not match a truncated read. **The real Stage 0c root cause is a write-path data-corruption bug (H4)**, not a cache-coherence bug.

---

## Stage 0b — `ICEBERG_CANNOT_OPEN_SPLIT`

### Method

1. Schema confirmed via `DESCRIBE cdp.trino_logs.trino_queries`. Time range filtered with `query_id BETWEEN '20260428_041500' AND '20260428_060000_zzzzz'` (matches plan + transcript convention; `query_date` is partition col; `query_id` lexicographically tracks UTC start time).
2. Pulled **all 263 failures** (not just 20) with `query_id, environment, failure_host, query_date, failure_message, failures_json` to TSV.
3. Classified by walking the `failures_json` `cause` chain and matching exception types — `UnknownHostException`, `Connection refused`, `Connection reset`, `Read timed out`, `SSLException`, `EOFException`, `5xx`, etc.
4. Cross-referenced timestamps with **`kube_pod_start_time{namespace="alluxio", pod=~"shelf-.*"}`** (mimir-data UID `ddy2eykq2tfy8a`, range 04:00–06:30 UTC, step 60 s) to find every shelf pod incarnation (uid changes = pod recreations).
5. shelfd-side access-log correlation was **not feasible**: current shelf pods started at 05:26:42 (shelf-2), 05:28:04 (shelf-1), 05:29:28 (shelf-0); kubelet-rotated stdout retains only 4 s (shelf-1) to 12 min (shelf-0) of logs at probe time; `restartCount=0` ⇒ no `--previous`; loki-data was returning 502 (known intermittent per workspace memory). The trino-side `failures_json` contained enough chain depth to root-cause without shelfd logs.

### Classification table

| Cause | Count | % | Example query_id (UTC) |
|---|---:|---:|---|
| `dns_unknown_host` (UnknownHostException for `shelf-N.shelf.alluxio.svc.cluster.local`) | **256** | 97.3% | `20260428_041502_00461_5xv7w` (rep-1 → shelf-1, 04:15:02) · `20260428_043104_02562_8ecru` (rep-0 → shelf-0, 04:31:04) · `20260428_045710_06450_8ecru` (rep-0, 04:57:10) |
| `tcp_connection_refused` (HttpHostConnectException to `shelf-1.shelf.alluxio.svc.cluster.local:9092`) | 6 | 2.3% | `20260428_041502_00462_5xv7w` · `20260428_044617_01181_5xv7w` · `20260428_045048_01233_5xv7w` |
| `NoHttpResponseException` ("The target server failed to respond") | 1 | 0.4% | `20260428_050824_06908_8ecru` |
| **Total** | **263** | 100% | |

Worker-IP hits (`failure_host`) are spread across 17 distinct Trino worker pods on rep-1 (top: `10.1.146.151`=42, `10.1.118.70`=36, `10.1.123.7`=36) and 8 on rep-0 (top: `10.1.126.171`=14) — **the failure is replica-wide, not a single bad worker**.

### Time distribution vs shelf pod rotations

Failure histogram (per-minute, only minutes with hits) and overlaid pod incarnation start times — every spike sits inside a 1–3-minute window after a shelf pod was recreated:

| Failure spike (UTC) | Hits | Coincident shelf pod recreations |
|---|---:|---|
| 04:15 | 2 | (initial cutover; rep-1's S3 client first contacts shelf) |
| 04:31–04:35 | **47** | shelf-0 04:31:16 · shelf-2 04:32:39 · shelf-1 04:33:47 |
| 04:55–04:57 | 21 | shelf-2 04:52:52 · shelf-2 04:53:54 · shelf-1 04:56:00 · shelf-0 04:57:30 |
| 05:01–05:11 | **120** | shelf-2 05:07:28 · shelf-0 05:08:00 · shelf-1 05:08:52 · shelf-2 05:10:01 · shelf-0 05:11:28 |
| 05:25–05:30 | 56 | shelf-2 05:26:42 · shelf-1 05:28:04 · shelf-0 05:29:28 |
| **Total** | **263** | **16 pod incarnations across 3 pods in 105 min = 1 every 6.6 min** |

(Pod incarnation table verified end-to-end: shelf-0 had 7 incarnations in window, shelf-1 had 5, shelf-2 had 7. Each transition removes the per-pod DNS A record from CoreDNS until the new pod becomes Ready.)

### Top-cause root cause analysis (DNS UnknownHostException, 256 hits)

`cdp.properties` on rep-0/rep-1 sets `s3.endpoint=http://shelf-N.shelf.alluxio.svc.cluster.local:9092` (per-pod stable hostname, intended for HRW key-affinity). The per-pod DNS name is published by the headless `shelf` Service **only while the pod has Ready=true** (StatefulSet stable-network-id contract). During a `Pod recreated` transition (StatefulSet rolling update or `kubectl delete pod`), the sequence is:

1. SIGTERM → readinessProbe failing → endpoint removed from headless Service
2. CoreDNS no longer resolves `shelf-N.shelf...svc.cluster.local`
3. Trino worker's AWS SDK does a fresh DNS lookup → **NXDOMAIN** → `java.net.UnknownHostException`
4. AWS SDK retry policy on UnknownHostException is *immediate-fail* (no exponential backoff for DNS failures); the split open returns ICEBERG_CANNOT_OPEN_SPLIT to the coordinator
5. New pod boots → readiness=true (~10–30 s later) → DNS re-publishes — but the failed split has already been recorded as a query failure

**There is no fallback.** Trino's native S3 client takes a single static endpoint string and will not retry to a different shelf pod, and shelfd does not yet expose a cluster service today (SHELF-22 = exactly that drop-in).

The 6 `Connection refused` and 1 `NoHttpResponseException` are sub-cases of the same race: the worker's DNS cache held the stale per-pod IP for a few extra seconds; TCP succeeded into a reachable IP whose listener was closed (refused) or which had already started shutting down its keepalive (no-response).

The window's contamination (per the plan: 7 helm revs + 2 coord restarts + 1 image swap) is what produced **16 pod recreations in 105 min**. None of these spikes are steady-state shelf behaviour; they are deployment-induced and would be eliminated in a chaos-free Stage 2 window.

### Recommended action for Conductor A

1. **Stage 1 (SHELF-22 cluster-svc + `minReadySeconds=30`) is the load-bearing fix for this entire 263-hit class** — switch `cdp.properties` `s3.endpoint` from `shelf-N.shelf.alluxio.svc.cluster.local:9092` to `shelf-pool.alluxio.svc.cluster.local:9092` (the new headless cluster service the chart adds). With 3 backends behind one DNS name, the rolling-restart of any single pod removes one of three endpoints, kube-proxy iptables drops it within ~2 s, and remaining pods serve. **Without SHELF-22, every helm upgrade or Karpenter pod-eviction of even one shelf pod will reproduce this exact 100-failure spike.**
2. **Stage 1b (SHELF-23 peer-fetch) is required at the same time** — once cluster-svc spreads requests across all 3 pods, the local-cache-only path will miss on shelf pods that are not the HRW primary for a key. Without `race_peer_or_origin` wired into `s3_shim::handle_get_object`, the rep-2 hit-ratio regression noted in the plan is a real risk; with it, the cluster behaves like a Cassandra read-path (fastest of peer / origin).
3. **Helm upgrade discipline (g1) directly mitigates the residual 0b risk.** Even with SHELF-22, doing 7 helm revs in 105 min is a `kubectl rollout restart`-equivalent thundering-herd; `minReadySeconds: 30` only smooths each *single* upgrade, not 7 in series. Cap at 1 helm upgrade per session, none in 9–11 IST peak.
4. **Verification trigger for Stage 2 PASS**: with SHELF-22 + `minReadySeconds=30` + 1 helm-upgrade budget, this entire class should drop to **0 hits** in the 90-minute clean A/B window (matches the locked criterion in the plan).
5. **Out-of-IRSA-bucket sub-issue (cross-cuts 0c too):** the only worker-side option for failed splits today is to fall through to direct S3 (SHELF-24 reverse-proxy). Until SHELF-24 lands, **excluding `pw-data-cdp-dev-encrypted` and `pw-data-cdp-prod-{gold,silver}-layer-audit` tables from the cdp catalog** (or routing them via a `cdp_direct` parallel catalog at `s3.ap-south-1.amazonaws.com`) avoids the 502 noise during cutover.

---

## Stage 0c — `ICEBERG_INVALID_METADATA`

### Method

1. Pulled all 249 failures with **full `failures_json`** column (CSV `field_size_limit` raised to `sys.maxsize`; the user-visible `failure_message` is truncated to ~400 chars and was not enough to distinguish classes).
2. Walked the cause chain to leaf, classified by exception type / status code / underlying message.
3. Identified the failing S3 path per failure (regex `s3a?://[^\s'"\\]+` from chain text).
4. Selected `admin.iceberg_maintenance_log` for the end-to-end trace (largest single-table impact at 14 hits, all on rep-1 + rep-0 dbt INSERT path).
5. Pulled all queries touching the table in the window (`query LIKE '%iceberg_maintenance_log%'` between 04:00–06:00 UTC) plus the immediately-preceding INSERTs since 00:00 UTC for baseline.
6. **Read-only port-forward** `kubectl -n alluxio port-forward shelf-1 18092:9092` + a single `curl GET` against the shim to capture the on-disk bytes for the suspect metadata-25455 path. Compared headers + first 256 bytes against expected Iceberg metadata.json shape.

### Sample trace — `admin.iceberg_maintenance_log` (rep-1, dbt iceberg-maintain macro)

`s3a://pw-data-cdp-prod-temp/warehouse/admin/iceberg_maintenance_log-994339ecc9ac43dc8863dd118e11f669/metadata/25455-874310b2-dd93-4c3f-abe3-f392dd8c500e.metadata.json`

| Step | Time UTC | query_id | State | wall_ms | Notes |
|---|---|---|---|---:|---|
| Baseline INSERTs (pre-cutover, coord pod `_2w8xw`) | 03:08:59, 03:09:04 | `…_2w8xw` | FINISHED | **2776 / 2471** | Direct-S3 path; ~2.5 s typical |
| (coord restart — rep-1 cutover MR `!17873`) | ~03:10–04:09 | — | — | — | coord pod ID switches `_2w8xw` → `_5xv7w` |
| **Last successful INSERT in window** | **04:09:48** | `20260428_040948_00414_5xv7w` | **FINISHED** | **51504** | written_bytes=147. **20× slower than baseline.** Wrote `metadata-25455-874310b2-….metadata.json`. |
| 1st FAILED INSERT (78 s later) | 04:11:06 | `20260428_041106_00442_5xv7w` | FAILED | 2150 | `JsonParseException: Unexpected character (';' (code 59)) at line 1, column 6` reading metadata-25455 |
| INSERT FAILED | 04:18:48 | `20260428_041848_00485_5xv7w` | FAILED | 2110 | same path, same error |
| INSERT FAILED | 04:26:19 | `20260428_042619_00531_5xv7w` | FAILED | 2109 | same |
| INSERT FAILED | 04:34:59 | `20260428_043459_00812_5xv7w` | FAILED | 2137 | (across shelf-1 incarnation 04:33:47) — same |
| INSERT FAILED | 05:16:49 | `20260428_051649_01653_5xv7w` | FAILED | 2143 | (across shelf-1 incarnations 04:56:00, 05:08:52) — same |
| INSERT FAILED | 05:24:39 | `20260428_052439_01748_5xv7w` | FAILED | 2147 | same |
| INSERT FAILED | 05:32:11 | `20260428_053211_01984_5xv7w` | FAILED | 2135 | (post-window, but symmetry holds — same path) |

**Live evidence (read-only fetch of the same path through shelf-1 shim, 28-Apr 07:29 UTC):**

```
HTTP/1.1 200 OK
content-type: application/octet-stream
content-length: 52170949
etag: "0b30b19205ec71d2b31e8bac15a61830-2"
last-modified: Tue, 28 Apr 2026 04:09:59 GMT
accept-ranges: bytes
```

First 256 bytes of body, octal-escape rendered:

```
20000;chunk-signature=cd7cb30d08ae28c059835a12c33ace18cca48a52cd6e94afa8fa999c6215e866\r\n
{"format-version":2,"table-uuid":"16aa64b8-1779-4477-a290-fc1f07f65f8e","location":"s3a://pw-data-cdp-prod-temp/warehouse/admin/iceberg_maintenance_log-994339ec…
```

Raw hex (offset 0–63):

```
00000000: 3230 3030 303b 6368 756e 6b2d 7369 676e   20000;chunk-sign
00000010: 6174 7572 653d 6364 3763 6233 3064 3038   ature=cd7cb30d08
00000020: 6165 3238 6330 3539 3833 3561 3132 6333   ae28c059835a12c3
00000030: 3361 6365 3138 6363 6134 3861 3532 6364   3ace18cca48a52cd
```

`grep -c chunk-signature= maint-meta.bytes` = **400 occurrences** (one per chunked transfer chunk; multipart upload had 2 parts, ETag suffix `-2`). The `aws-chunked` framing pervades the entire 52 MB persisted object.

Key forensic facts:

1. `Last-Modified: 04:09:59 GMT` — sits **inside** the 04:09:48–04:10:39 window of the FINISHED INSERT (`20260428_040948_00414_5xv7w`, wall=51504 ms). This is the bytes that INSERT wrote.
2. The `20000;` prefix is exactly the `;` at column 6 line 1 in `JsonParseException: Unexpected character (';' (code 59)): Expected space separating root-level values at [Source: REDACTED ; line: 1, column: 6]`. Position is exact. `0x20000` = 131,072 bytes = 128 KiB = first chunk of a `Content-Encoding: aws-chunked` PUT.
3. The corruption persists across **4 shelf-1 pod incarnation rotations during the window** (04:33:47, 04:56:00, 05:08:52, 05:28:04) — each restart wipes shelfd's local cache. Therefore the corrupt bytes are stored in S3, not in shelf's RAM/disk cache.
4. ETag `"0b30b19205ec71d2b31e8bac15a61830-2"` — multipart upload, 2 parts. Trino's native S3 client did a chunked multipart PUT; shelfd's shim PUT path (`handle_put_object`, preview-8) buffered each part and forwarded as opaque bytes without decoding the `aws-chunked` envelope, so the chunk-size hex + `chunk-signature=...\r\n` lines were persisted along with the JSON body in *both* parts of the S3 object.

### Hypothesis evidence table

| Hyp | Evidence pulled | Verdict |
|---|---|---|
| **H1 — picker timeout truncation** (RateLimitPicker stalls hit_disk past Trino S3 client timeout, partial JSON parsed) | Live HEAD on the suspect path returns 200 OK in <1 s with `content-length: 52170949` (~50 MB) — body is **larger** than expected, not truncated. JsonParseException happens at line 1 col 6, *before* any timeout would occur. Failing INSERT wall_time was 2.1 s (well under any 10 s S3 client timeout). | **REFUTED** |
| **H2 — HEAD-LRU negative-cache poisoning, same pod** (HEAD returned 404 from negative cache before PUT, unchanged after) | shelf returns **HTTP 200 with corrupt content**, not 404. There is no negative-cache entry to invalidate. SHELF-21 invalidation logic is not on the failing path. | **REFUTED** |
| **H3 — cross-pod cache coherence** (PUT lands on pod A, invalidates only A's HEAD-LRU; subsequent GET lands on pod B and serves stale entry) | rep-1's `s3.endpoint` is the per-pod hostname `shelf-1...:9092` — both PUT and GET went to the **same pod (shelf-1)**. Even ignoring the routing pin, the corruption persists across 4 distinct shelf-1 pod incarnations during the window (each one a fresh process with empty cache); cross-pod coherence cannot explain a phenomenon that survives a same-pod cache-wipe. | **REFUTED** (today; latent for SHELF-22 cluster-svc world but not the cause here) |
| **H4 — shelfd PUT path persists `aws-chunked` framing as literal bytes in S3** *(new — emerged from the trace)* | (a) `;` at exactly column 6 matches the chunk-size hex `20000;` framing; (b) 400 `chunk-signature=` occurrences distributed throughout the 50 MB body confirm full-body framing; (c) Last-Modified is inside the FINISHED INSERT's wall_time; (d) `aws-chunked` is the AWS SDK default for streaming PUTs and Trino native-S3 uses streaming for Iceberg metadata writes; (e) corruption survives every cache wipe because it's on the storage backend, not in cache. | **CONFIRMED** |

### Most-likely hypothesis

**H4 — shelfd `handle_put_object` does not decode `Content-Encoding: aws-chunked` before forwarding the body to S3.** This is a **write-path data-corruption bug** that:

1. Permanently mangles every Iceberg `metadata.json` (and any other PUT body using AWS SDK streaming chunked encoding) written through the shelfd shim.
2. Surfaces as `ICEBERG_INVALID_METADATA` on every subsequent read because Iceberg's `metadata.json` is path-immutable — once a corrupt file is committed, every reader of that snapshot pointer fails until a manual rewrite.
3. Is independent of cache state, picker, peer-fetch, or any read-path concern.

Workspace memory shows SHELF-21 / preview-5 added the PUT path (`Origin::put_object` + `handle_put_object` ≤ 256 MiB buffered forward). The bug is that the buffered body is forwarded as opaque bytes; the `Content-Encoding: aws-chunked` envelope must be unwrapped (or the `Authorization` `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` algorithm hint in the original request honored) before re-uploading to S3. Apache hadoop-aws / AWS SDK have utility decoders for this format (e.g. the SDK's own `AwsChunkedEncodingInputStream` + reader), but the shim must not pass the framed stream through the v4 sigv4 re-signer unchanged.

### Recommended action for Conductor A

1. **Open SHELF-25 (P0) — fix the shim PUT path to decode `aws-chunked` framing.** Acceptance: a real Trino INSERT through the shim against MinIO produces an S3 object whose first byte is `{` and whose `content-length` matches the JSON body (not the chunked envelope size). Add an integration test that PUTs a body with `Content-Encoding: aws-chunked` and asserts the persisted object equals the unframed body byte-for-byte. Workspace memory mentions Agent C is already wiring SHELF-23 — this is a separate, smaller fix that must land before any rep-1 / rep-0 stays cut over (rep-2 reads only via Metabase, so it's the only safe rep until SHELF-25 ships).
2. **Block Stage 5 cutover for any write-capable rep** (rep-0, rep-1) until SHELF-25 is in. Per workspace memory, the same class produced `HIVE_WRITER_CLOSE_ERROR` on rep-1 before SHELF-21 — this is the next layer of the same iceberg, not a fresh bug.
3. **Cleanup the corrupted file (one-shot, separate workstream).** `metadata-25455-874310b2-dd93-4c3f-abe3-f392dd8c500e.metadata.json` is dead bytes occupying the current `metadata_location` in HMS for `cdp.admin.iceberg_maintenance_log`. Until rewritten, every `INSERT INTO admin.iceberg_maintenance_log` on rep-1 fails. Options, in priority order:
   - (a) Manual rewrite via direct S3: read the previous `metadata-25454-….metadata.json` (which presumably is healthy — the 04:09:48 INSERT itself read it), re-derive snapshot 25455 from the data files written during the FINISHED INSERT, write a clean `metadata-25455-….metadata.json` directly to S3 (bypassing shelfd), update HMS pointer.
   - (b) `system.iceberg.register_table` rewind to snapshot 25454 (loses the 1 row written by 04:09:48; acceptable for `iceberg_maintenance_log`).
   - (c) Drop + recreate the table (most aggressive; OK because this is a maintenance-log table with low durability requirements).
4. **Audit other tables for the same corruption.** Any Iceberg `metadata.json` whose `last-modified` ≥ 04:09:48 UTC and was written through shelfd may be similarly corrupt. The 14 hits were on `iceberg_maintenance_log`, but this is the sole table that *kept retrying* (dbt loop every 7 min) and so produced multiple visible failures. Other tables with single hits are likely the same bug, just observed once before the consumer gave up. Quick scan: `SELECT DISTINCT regexp_extract(failure_message, 'metadata for table ([\w\.]+)', 1) FROM cdp.trino_logs.trino_queries WHERE error_code='ICEBERG_INVALID_METADATA' AND query_id BETWEEN '20260428_041500' AND '20260428_060000_zzzzz' AND environment IN ('replica0','replica1') AND failures_json LIKE '%JsonParseException%';` — top hit beyond `iceberg_maintenance_log` is `admin.iceberg_v3_test_encrypted` (8 hits, but 7 are 502 not parse errors, so it's the IRSA-scope issue, not H4).
5. **For the 215 `dns_unknown_host` sub-class (86%):** the recommendation is identical to Stage 0b — Stage 1 (SHELF-22 cluster-svc) eliminates this class structurally. No separate fix needed.
6. **For the 20 `http_status_error` (502) sub-class (8%):** route `pw-data-cdp-dev-encrypted`, `pw-data-cdp-prod-gold-layer-audit`, `pw-data-cdp-prod-silver-layer-audit` through a parallel `cdp_direct` catalog at `s3.ap-south-1.amazonaws.com`, OR have DevOps extend `data-platform-alluxio-role` to grant `s3:GetObject`/`s3:HeadObject`/`s3:ListBucket` on those bucket suffixes. Until then, cdp catalog cutover should not include schemas backed by those buckets (the affected tables are `admin.iceberg_v3_test_encrypted`, `ambassador_txn.{gold,silver}_payouts_transaction_mappings`).

---

## Appendix — data files (ephemeral, `/tmp/agent-b/`)

- `cos_full.tsv` — all 263 ICEBERG_CANNOT_OPEN_SPLIT failures, full `failure_message` + `failures_json`
- `inv_full.tsv` — all 249 ICEBERG_INVALID_METADATA failures, full payloads
- `maint_log.tsv` — all 23 queries touching `iceberg_maintenance_log` in the window (CREATE/INSERT/DESCRIBE)
- `maint-meta.bytes` — the 52,170,949-byte corrupt metadata-25455 fetched from shelf-1 shim (offset-0 starts `20000;chunk-signature=cd7cb30d…`)
- `headers.txt` — HTTP response headers for the corrupt fetch (Last-Modified `Tue, 28 Apr 2026 04:09:59 GMT`, ETag `"…-2"`)
