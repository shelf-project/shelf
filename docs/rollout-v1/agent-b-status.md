# Agent B (Forensics) — Status

Window: 28-Apr-2026 04:15–06:00 UTC. Shelf live on rep-0 + rep-1 only. rep-2 had stale config.

## Milestones

- **2026-04-28 12:45 IST — START.** Read plan + transcript. Confirmed working trino HTTP query pattern from `/tmp/trino_compare.py`. Cluster context = `data-platform-cluster`, shelf-0/1/2 all `Running`, restartCount=0.
- **Constraint observed:** shelf pods rotated AFTER the test window. shelf-2 started 05:26:42 UTC, shelf-1 started 05:28:04 UTC, shelf-0 started 05:29:28 UTC. RestartCount=0 ⇒ no `kubectl logs --previous` available. Only the **last ~30 min** of the 105-min window (05:26–06:00 UTC) has live shelfd logs accessible. The 04:15–05:26 UTC slice is lost from shelfd-side. RCA will be transparent about this gap.

- **Stage 0b complete.** Pulled all 263 ICEBERG_CANNOT_OPEN_SPLIT failures in window. Classified by walking failures_json cause chain.
  - 256/263 (97.3%) `dns_unknown_host` — UnknownHostException for `shelf-{0,1}.shelf.alluxio.svc.cluster.local`
  - 6/263 (2.3%) `tcp_connection_refused` to `shelf-1...:9092`
  - 1/263 (0.4%) `NoHttpResponseException` (single instance)
  - **Time distribution clusters at 04:31-04:35, 04:55-04:57, 05:05-05:11, 05:25-05:30** — every spike correlates with a shelf pod incarnation rotation observed in `kube_pod_start_time` (16 rotations across shelf-0/1/2 in 105 min).

- **Stage 0c sample pulled.** 249 ICEBERG_INVALID_METADATA total. Classification:
  - 215/249 (86%) `dns_unknown_host` — same root cause as Stage 0b, applied to HEAD on metadata.json paths
  - 20/249 (8%) `http_status_error` — shelfd returned **HTTP 502** for HEAD on `pw-data-cdp-dev-encrypted`, `pw-data-cdp-prod-gold-layer-audit`, `pw-data-cdp-prod-silver-layer-audit` (out-of-IRSA buckets)
  - 14/249 (5.6%) `json_parse_error` — JsonParseException at line 1 column 6 for `admin.iceberg_maintenance_log` metadata-25455.

- **Trace complete on iceberg_maintenance_log.** End-to-end: 04:09:48 UTC dbt INSERT (`20260428_040948_00414_5xv7w`) FINISHED with `wall_time_millis=51504` (vs ~2.5s baseline = 20× slower). Last-Modified on `metadata-25455-...metadata.json` = `2026-04-28T04:09:59 GMT` (inside that INSERT). Subsequent INSERTs (04:11:06, 04:18:48, 04:26:19, 04:34:59, 04:48:01, 05:16:49, 05:24:39, 05:32:11) all FAILED with the same JsonParseException. Live read-only fetch of the same path via `kubectl port-forward shelf-1 18092:9092` returned **52,170,949 bytes** with the body starting `20000;chunk-signature=cd7cb30d08ae28c059835a12c33ace18cca48a52cd6e94afa8fa999c6215e866\r\n{"format-version":2,...`. **The file in S3 is permanently corrupted** with `aws-chunked` framing concatenated with the JSON body. ETag `"…-2"` = multipart upload with 2 parts. shelfd's PUT path failed to strip `Content-Encoding: aws-chunked` framing before forwarding to S3.

- **Three plan hypotheses resolved.**
  - H1 picker timeout truncation: REFUTED. Body is 50 MB, not truncated; HEAD response time is sub-second.
  - H2 HEAD-LRU negative-cache poisoning: REFUTED. shelf returns HTTP 200 with corrupt content, not 404. Corruption persists across 4 shelf-1 pod incarnations during the window (cache-wipes), so the corruption is not in shelf's cache.
  - H3 cross-pod cache coherence: REFUTED for the same reason (cache wipes don't clear it).
  - **NEW H4 (validated):** shelfd PUT path does not decode `aws-chunked` Content-Encoding, persisting AWS SDK chunk-size hex + chunk-signature framing as literal bytes in the destination S3 object.

- **Report written.** `/Users/aamir/trino/shelf/docs/rollout-v1/rca-stage0bc.md` — exec summary + Stage 0b classification (97% DNS, 263/263 correlated to 16 pod incarnations) + Stage 0c trace (`metadata-25455-874310b2-…metadata.json` confirmed corrupt in S3 with `aws-chunked` framing, 400 chunk-signature occurrences in 50 MB body) + H1/H2/H3 all REFUTED + new H4 CONFIRMED (shelfd PUT-path data-corruption bug) + actions for Conductor A.

- **No cluster mutations performed.** Single read-only kubectl port-forward to shelf-1:9092 for the GET probe; killed at end of session.
