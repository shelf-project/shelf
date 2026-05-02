# SHELF-S1 — Shim profile via in-process Criterion bench

> Bench evidence superseded by S5 once trinodb/trino#29184 lands; until
> then this bench is the source of truth for per-request hot spots.
> The original SHELF-S1 plan was "tokio-console + cargo flamegraph on
> a single shelf-bench pod under bench load"; that pod is gone (see
> `benchmarks/results/2026-05-01/SUMMARY.md` §"Cluster state restored"),
> so this in-process Criterion harness fills the gap.

## Method

In-process Criterion (`async_tokio` runtime, 4 worker threads) +
`httpmock` S3 origin (loopback ephemeral port). The bench drives the
**production** `s3_shim::router` via `tower::ServiceExt::oneshot`, so:

- TCP / TLS / network RTT are excluded from the measurement.
- Route match, axum extractors, Foyer get/insert, response framing —
  every shim-internal cost is *included*.
- Real `FoyerStore` (4 MiB DRAM per pool + 64 MiB NVMe rowgroup tier)
  and real `S3Origin` (`aws-sdk-s3` against the in-process mock) — no
  stubs.

Run with:

```bash
cargo bench --bench shim_profile -p shelfd
```

Per-iteration mean / p50 / p95 / p99 are derived directly from
`target/criterion/<bench>/new/sample.json` (`time / iters`); criterion's
`time: [low mean high]` console column is a 95 % confidence interval of
the mean, **not** a percentile.

Hardware: macOS 14 / aarch64, M-series, single bench process, no other
load.

## Per-call wall-time breakdown (single-threaded)

| Path                          | mean      | p50       | p95       | p99       |
|-------------------------------|-----------|-----------|-----------|-----------|
| GET DRAM hit                  | 296.64 µs | 211.18 µs | 625.25 µs | 1.469 ms  |
| GET NVMe-leaning hit          | 281.30 µs | 232.67 µs | 622.98 µs | 731.74 µs |
| GET origin miss (mock)        | 224.05 µs | 205.13 µs | 298.99 µs | 475.85 µs |
| HEAD cached (HEAD-LRU hit)    |   5.38 µs |   5.28 µs |   6.76 µs |   7.47 µs |

## Concurrency curve (GET DRAM hit)

| concurrency | mean      | p50       | p95       | p99       |
|-------------|-----------|-----------|-----------|-----------|
|  1          | 195.14 µs | 195.09 µs | 205.67 µs | 228.68 µs |
|  4          | 504.87 µs | 506.06 µs | 531.37 µs | 539.95 µs |
| 16          | 2.369 ms  | 1.874 ms  | 6.473 ms  | 10.053 ms |
| 32          | 4.530 ms  | 4.175 ms  | 6.078 ms  | 13.523 ms |
| 64          | 7.896 ms  | 7.933 ms  | 8.172 ms  |  8.203 ms |

Per-request cost at saturation: at concurrency 64 the wall is 7.90 ms
for 64 in-flight, ≈ **123 µs / request**, versus **195 µs / request**
at concurrency 1 — a 1.6× per-request improvement from runtime-level
parallelism alone, with the absolute throughput climbing from
≈ 5 100 req/s (conc=1) to ≈ 8 100 req/s (conc=64).

## Hot-spot ranking (informs S2, S3)

1. **GET path is ~40× more expensive than HEAD per request**
   (215 µs slope-mean vs 5.4 µs). The HEAD path runs route match →
   `head_lru` lookup → response build, with no Foyer or origin work.
   The GET path adds Foyer `get_or_fetch` + body assembly +
   `Content-Range` framing + `Bytes` allocation. The ~210 µs delta is
   the dominant single-request cost in the shim and is the natural
   target for S2 (HTTP/2 + connection-pool audit) and S3 (SigV4
   signing).

2. **GET DRAM-hit p99 is 7× p50 (1.47 ms vs 211 µs).** Tail variance
   is the second-largest hot spot. The likely sources are tokio task-
   scheduler contention on the 4-worker runtime, allocator returns on
   the 4 KiB `Bytes::from` path, and Foyer's intra-pool S3-FIFO /
   LRU bookkeeping (the warm key may briefly fall to NVMe and back
   under the fill traffic the harness uses to populate the disk
   tier). S2 should look for an HTTP/2-vs-HTTP/1.1 noise floor
   difference; S3 should profile signing under the same harness to
   isolate signing variance from Foyer variance.

3. **Origin-miss path is *cheaper* than DRAM hit on this harness
   (205 µs p50 vs 211 µs p50).** Counter-intuitive but consistent with
   the harness shape: each miss iteration uses a fresh key, so the
   admission policy short-circuits before Foyer ever attempts an
   insert; the warm-key path always does the full Foyer get +
   single-flight check. Implication: the `get_or_fetch` hit path has
   roughly the same critical-section length as the miss path against
   a sub-50 µs origin. Real S3 (10 ms+ RTT) inverts this completely;
   the in-process bench is *not* a substitute for live latency
   numbers (see Caveats).

4. **Concurrency saturates between 16 and 32 in-flight.** 1→4 = 2.6×
   wall (good pipelining), 4→16 = 4.7× wall (still scaling), 16→32 =
   1.9× wall (visible contention), 32→64 = 1.7× wall (further
   contention but the per-request floor at conc=64 is the *lowest*
   observed at any concurrency). The 4-worker runtime is the obvious
   limit; the production shelfd binary uses the tokio default
   (`num_cpus`), so a per-pod 4xlarge (16 vCPU) saturates at higher
   concurrency than this bench.

## Cross-check against S2 (already merged — PR #109)

S2 landed before this report (commit `fb48a52`, "feat(rc8): S2 HTTP/2
+ connection pool audit on shim"). Its motivating finding was a
**5–15 ms per-request shim overhead from HTTP/1.1 handshake +
idle-pool eviction on origin GETs** — that finding is consistent with
this in-process bench but lives at a different scale: the bench
measures shim-internal cost in microseconds, and the missing
millisecond budget S2 plugged sits in the live-network path that the
bench deliberately mocks out. Three observations from the bench that
either re-confirm or refine S2's choices:

- **Origin-miss p99 here (476 µs) is two orders of magnitude below
  S2's reported live overhead (5–15 ms).** The delta is the live TCP
  + TLS handshake + AWS-side TLS termination; S2's `pool_idle_timeout
  = 90s` + `http2_keep_alive_*` defaults are the right place to
  amortise that handshake cost across many requests, exactly because
  the per-request *content* cost (this bench's number) is small enough
  that handshake noise dominates the live wall.

- **The 4 KiB-body framing overhead is real but proportionally
  small** (HEAD = 5 µs vs GET = 215 µs; the ~210 µs gap is body
  assembly + Foyer `get_or_fetch` + axum framing + `Content-Range`
  build). S2 did not touch this path; a focused S2-follow-up that
  flamegraphs the GET body-assembly slice (e.g.
  `cargo bench --bench shim_profile -- --profile-time 30`) would
  surface whether `axum::body::Body::from(bytes)` →
  `http_body_util::collect()` round-tripping or Foyer's
  content-addressed key derivation is the long pole.

- **The concurrency curve does not contradict S2's HTTP/2
  multiplexing benefit on the peer-fetch / membership-resolver
  paths.** Per-request cost *improves* from 195 µs (conc=1) to
  123 µs (conc=64) on the shim's local axum router — the runtime is
  the limit, not socket contention. S2's
  `pool_max_idle_per_host = 8` bump (peer_http) is upstream of the
  shim and should make the conc-curve flatten further on the live
  cluster as cross-pod fetches stop paying handshake on every miss.

## Recommendations for S3 (SigV4 signing — still open)

- **Per-request SigV4 signing is bounded by the origin-miss budget
  (≤ 225 µs total).** The mock origin returns within ~50 µs, so
  signing + axum extractor + Foyer admission together fit in
  ~175 µs. That is consistent with `aws-sdk-sigv4` doing a SHA-256
  over headers + a small HMAC chain; it's hard to imagine more than a
  ~50 µs signing slice. S3 should *measure* signing in isolation
  before assuming it is a hot spot — the bench numbers do not yet
  prove signing dominates.

- **A signing-cache lever (per-bucket / per-method canonical-request
  reuse) would help most in the cache-miss path.** The DRAM-hit and
  HEAD-cached paths do not touch the origin, so signing is not in
  their critical section; a signing cache buys nothing there. S3
  should focus on the high-miss-rate pre-warm scenarios (cold-start,
  KEDA spot-worker rotation) where every byte of Iceberg metadata
  goes through SigV4.

- **Signing variance is *not* the source of the DRAM-hit p99 tail.**
  The DRAM-hit path never signs anything; its 1.47 ms p99 is purely
  internal (Foyer + tokio + allocator). S3 can stay scoped to the
  miss path without worrying about contaminating the hit-path tail.

## Caveats

- **In-process bench excludes network RTT.** Real S3 GETs to
  ap-south-1 from data-platform-cluster cost 10–30 ms; the mock
  origin here costs <50 µs. The miss-vs-hit ratio in this bench
  inverts under real RTT. Use the live shelf-bench cluster (once
  restored) for absolute latency claims.

- **Mock S3 does not exercise SDK signing variance realistically.**
  The mock accepts any signed request without validation; the SDK
  still runs the full signing pipeline on the way out, so signing
  cost is captured. But error paths (401 / 403 retries, throttling)
  are *not* exercised, and any signing optimisation that changes
  retry behaviour needs a separate live test.

- **DRAM-hit numbers are an upper bound on shim throughput.** The
  4 MiB DRAM cap + 8 MiB pre-fill means the warm key may briefly
  fall to NVMe under harness fill traffic. The "NVMe-leaning" bench
  is *not* guaranteed to read from NVMe (Foyer's S3-FIFO / LRU
  promotion is internal); a follow-up bench could pin the tier by
  configuring `dram_bytes = 0` if Foyer permits it.

- **Body collect cost is amortised across 4 KiB.** Larger payloads
  (real Parquet row groups are 8–32 MiB) will shift the dominant
  cost from "shim bookkeeping" to "body memcpy + cache write". The
  hot-spot ranking above is for sub-1 MB objects (matches the
  Iceberg metadata + Parquet footer access pattern that drives most
  of shelf's miss traffic).

- **Criterion's `time: [low mean high]` is a confidence interval,
  not a percentile.** The percentile columns above were computed by
  consuming `sample.json` directly (per-iteration time = `times[i] /
  iters[i]`, sorted, q-th index). HTML reports under
  `target/criterion/` carry the same data with violin plots.

- **No comparison to direct-S3 baseline yet.** This bench measures
  *only* the shim against a mock; it does not show what a Trino
  query saves vs. raw S3. The cluster-side comparison lives in the
  rep-N cutover analyses (rep-1 / rep-2 in `docs/rollout-v1/`); the
  in-process number here is one input into that broader picture, not
  the headline.
