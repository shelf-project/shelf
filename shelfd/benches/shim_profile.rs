//! SHELF-S1 — in-process Criterion profile of the SHELF-22 S3-compat shim
//! accept path.
//!
//! Why this exists: the live shelf-bench cluster has been torn down (see
//! `benchmarks/results/2026-05-01/SUMMARY.md` §"Cluster state restored"),
//! so the original "tokio-console + cargo flamegraph on a single
//! shelf-bench pod under bench load" plan is not currently runnable.
//! Until the cluster comes back, this Criterion harness is the source of
//! truth for per-request hot spots inside the shim — it stands the
//! production `s3_shim::router` up against an in-process `httpmock` S3
//! origin and drives requests through `tower::ServiceExt::oneshot` so
//! every measurement excludes TCP / TLS / network RTT and isolates
//! shim-internal cost (route match → extractor decode → Foyer read →
//! response build).
//!
//! Scenarios:
//! - GET hot: warm cache, single request — dominated by Foyer DRAM read
//!   and response framing.
//! - GET NVMe-leaning: warm cache after a fill that's larger than the
//!   bench DRAM cap; some requests are served from NVMe (Foyer's tier
//!   choice is internal so we can't strictly pin "DRAM" vs "NVMe", see
//!   `agents/out/SHELF-S1/profile-report.md` for caveats).
//! - GET origin miss: each iteration is a fresh content-addressed key;
//!   measures shim overhead on top of a sub-millisecond mocked S3.
//! - HEAD cached: HEAD-LRU hit, no origin round-trip.
//! - Concurrency curve: 1 / 4 / 16 / 32 / 64 in-flight GETs against the
//!   warm key, joined with `futures::future::join_all`.
//!
//! Run with:
//!   cargo bench --bench shim_profile -p shelfd
//!
//! Criterion writes HTML reports under `target/criterion/`; consume the
//! `mean / median / std-dev` columns from stdout to populate
//! `agents/out/SHELF-S1/profile-report.md`.
//!
//! Constraints honoured:
//! - Read-only against `s3_shim.rs` and the production hot path.
//! - No live network or live cluster — `httpmock` is the only external
//!   dependency, and it binds to a loopback ephemeral port.
//! - Real Foyer pools (small DRAM + small NVMe configured for the
//!   bench) — Foyer is not stubbed.
//! - Criterion `async_tokio` runtime matches production's tokio
//!   multi-thread executor.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::future::join_all;
use httpmock::Method::{GET, HEAD};
use httpmock::MockServer;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::{
        AdmissionConfig, EvictionPolicy, MetadataPoolConfig, OriginConfig, PoolsConfig,
        RowGroupPoolConfig,
    },
    head_lru::HeadLru,
    http::{self, ServerState},
    metrics,
    origin::S3Origin,
    router::Router as HrwRouter,
    store::FoyerStore,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tower::ServiceExt;

const BUCKET: &str = "bench-bucket";
/// Warm-cache key reused across the DRAM-hit and concurrency benches.
const HOT_KEY: &str = "hot/object.parquet";
/// Per-iteration payload size. 4 KiB keeps the body memcpy cost low so
/// the shim's own work (route match, header build, Foyer lookup,
/// response assembly) dominates the measurement instead of body
/// allocation.
const PAYLOAD_LEN: usize = 4 * 1024;
/// DRAM budgets for both pools. Sized to hold the warm-cache key with
/// a comfortable margin so DRAM-hit measurements stay deterministic.
const DRAM_BYTES: u64 = 4 * 1024 * 1024;
/// NVMe capacity for the rowgroup pool. Foyer requires a non-zero
/// quota to engage the hybrid tier; 64 MiB is more than enough for
/// the working set the bench produces.
const NVME_BYTES: u64 = 64 * 1024 * 1024;

/// Bench harness shared across every Criterion function. Initialised
/// lazily on first use and held for the rest of the process — Foyer's
/// background tasks need a live tokio runtime, and `metrics::Registry`
/// can only register its global counters once per process.
struct Harness {
    runtime: Runtime,
    state: Arc<ServerState>,
    /// Suffix counter so the origin-miss scenario can mint a fresh
    /// content-addressed key per iteration (Foyer caches by ETag, so
    /// even repeated paths hit cache after the first fetch).
    miss_counter: AtomicU64,
    /// Holders kept alive for the lifetime of the harness — dropping
    /// any of them tears down the bench.
    _server: Arc<MockServer>,
    _nvme_dir: TempDir,
}

static HARNESS: OnceLock<Harness> = OnceLock::new();

fn harness() -> &'static Harness {
    HARNESS.get_or_init(build_harness)
}

fn build_harness() -> Harness {
    // Multi-thread runtime matches production. Worker count is
    // intentionally small so per-iteration scheduling overhead stays
    // representative of a single shelfd pod under modest concurrency.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("bench runtime");

    let nvme_dir = tempfile::tempdir().expect("nvme tempdir");

    // SAFETY: tests + benches share process-global env. The bench is
    // the sole writer here; values are stable for the run.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "bench");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "bench");
        std::env::set_var("AWS_REGION", "us-east-1");
    }

    let payload = vec![0xCDu8; PAYLOAD_LEN];

    let (state, server) = runtime.block_on(async {
        let server = MockServer::start_async().await;

        // HEAD /<bucket>/<key> for any key — returns the metadata the
        // shim's HEAD path stores in the HEAD-LRU.
        server
            .mock_async(|when, then| {
                when.method(HEAD).path_matches(
                    regex::Regex::new(&format!("^/{BUCKET}/.*")).expect("head regex"),
                );
                then.status(200)
                    .header("Content-Length", PAYLOAD_LEN.to_string())
                    .header("ETag", "\"deadbeefcafef00d\"")
                    .header("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
                    .header("Accept-Ranges", "bytes");
            })
            .await;

        // GET /<bucket>/<key> — every range request returns the full
        // 4 KiB payload. The mock does not strictly honour the Range
        // header (it always returns the same bytes), but for the
        // purposes of measuring shim overhead that is fine: the shim
        // forwards whatever the origin returns, and the SDK accepts a
        // 200 in place of a 206 for ranged reads (the response is
        // rebuilt as 206 Partial Content on the way out by the shim
        // when the caller supplied a Range header).
        server
            .mock_async(|when, then| {
                when.method(GET)
                    .path_matches(regex::Regex::new(&format!("^/{BUCKET}/.*")).expect("get regex"));
                then.status(206)
                    .header(
                        "Content-Range",
                        format!("bytes 0-{}/{}", PAYLOAD_LEN - 1, PAYLOAD_LEN),
                    )
                    .header("Content-Length", PAYLOAD_LEN.to_string())
                    .header("ETag", "\"deadbeefcafef00d\"")
                    .header("Accept-Ranges", "bytes")
                    .body(payload.clone());
            })
            .await;

        let origin_cfg = OriginConfig {
            bucket: BUCKET.to_owned(),
            endpoint_url: Some(server.base_url()),
            region: Some("us-east-1".to_owned()),
            max_inflight: 64,
        };
        let pools_cfg = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: DRAM_BYTES,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: DRAM_BYTES,
                nvme_dir: nvme_dir.path().to_path_buf(),
                nvme_bytes: NVME_BYTES,
                eviction_policy: EvictionPolicy::Lru,
                disk_cache: Default::default(),
                compression: Default::default(),
            },
        };
        let admission_cfg = AdmissionConfig {
            size_threshold_bytes: 32 * 1024 * 1024,
            pinned_bypass: true,
        };

        let origin = Arc::new(S3Origin::new(&origin_cfg).await.expect("origin"));
        let store = Arc::new(FoyerStore::open(&pools_cfg).await.expect("store"));
        let router = Arc::new(HrwRouter::new());
        let admission = Arc::new(SizeThresholdPolicy::from_config(&admission_cfg));
        let head_lru = Arc::new(HeadLru::new(10_000));
        let metrics_reg = Arc::new(metrics::Registry::init().expect("metrics"));

        let state = Arc::new(ServerState::with_head_lru_and_pod_id(
            store,
            origin,
            router,
            admission,
            metrics_reg,
            head_lru,
            "shim-profile-bench".to_owned(),
        ));
        state.mark_ready();

        // Phase 1: push roughly 8 MiB of unrelated keys through the
        // shim so the DRAM tier (4 MiB cap) overflows. Foyer's hybrid
        // pool moves evicted DRAM entries down to NVMe; subsequent
        // reads of those keys (the "NVMe-leaning" scenario) exercise
        // the disk path.
        //
        // Done FIRST so the warm hot key (Phase 2) is the freshest
        // entry under LRU and stays DRAM-resident for the duration of
        // the bench. Doing this in the opposite order lets LRU evict
        // the hot key before iteration zero of the DRAM-hit bench, in
        // which case both "dram_hit" and "nvme_leaning_hit" measure
        // the same NVMe-served path.
        let app = http::build_s3_shim_router(state.clone());
        for i in 0..2048 {
            let key = format!("nvme-fill/object-{i}.parquet");
            let req = Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{key}"))
                .header("Range", format!("bytes=0-{}", PAYLOAD_LEN - 1))
                .body(Body::empty())
                .expect("fill request");
            let app_clone = app.clone();
            let resp = app_clone.oneshot(req).await.expect("fill oneshot");
            assert!(
                resp.status() == StatusCode::PARTIAL_CONTENT || resp.status() == StatusCode::OK,
                "fill iteration {i} did not succeed: status = {:?}",
                resp.status()
            );
        }

        // Phase 2: warm the hot key LAST so it occupies the freshest
        // DRAM slot. Subsequent reads in `bench_get_object_dram_hit`
        // and the concurrency curve hit the warm content-addressed
        // key without re-fetching from origin.
        let app = http::build_s3_shim_router(state.clone());
        let warm_req = Request::builder()
            .method("GET")
            .uri(format!("/{BUCKET}/{HOT_KEY}"))
            .header("Range", format!("bytes=0-{}", PAYLOAD_LEN - 1))
            .body(Body::empty())
            .expect("warm request");
        let resp = app.oneshot(warm_req).await.expect("warm oneshot");
        assert!(
            resp.status() == StatusCode::PARTIAL_CONTENT || resp.status() == StatusCode::OK,
            "warm-up call did not succeed: status = {:?}",
            resp.status()
        );

        // Warm the HEAD-LRU so the HEAD bench measures the cached
        // path. Done after the hot-key GET so the HEAD-LRU entry is
        // populated from the same lookup as the GET path.
        let app = http::build_s3_shim_router(state.clone());
        let head_req = Request::builder()
            .method("HEAD")
            .uri(format!("/{BUCKET}/{HOT_KEY}"))
            .body(Body::empty())
            .expect("head warm request");
        let resp = app.oneshot(head_req).await.expect("head warm oneshot");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "head warm-up did not succeed"
        );

        (state, Arc::new(server))
    });

    Harness {
        runtime,
        state,
        miss_counter: AtomicU64::new(0),
        _server: server,
        _nvme_dir: nvme_dir,
    }
}

fn build_get_request(key: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/{BUCKET}/{key}"))
        .header("Range", format!("bytes=0-{}", PAYLOAD_LEN - 1))
        .body(Body::empty())
        .expect("get request")
}

fn build_head_request(key: &str) -> Request<Body> {
    Request::builder()
        .method("HEAD")
        .uri(format!("/{BUCKET}/{key}"))
        .body(Body::empty())
        .expect("head request")
}

fn bench_get_object_dram_hit(c: &mut Criterion) {
    let h = harness();
    c.bench_function("shim::GET dram_hit", |b| {
        b.to_async(&h.runtime).iter(|| async {
            let app = http::build_s3_shim_router(h.state.clone());
            let resp = app
                .oneshot(build_get_request(HOT_KEY))
                .await
                .expect("oneshot");
            // Drain the body so the measurement covers response
            // framing too — without this the body reader might be
            // dropped before the bytes are produced.
            let _ = drain_body(resp).await;
        });
    });
}

fn bench_get_object_nvme_hit(c: &mut Criterion) {
    let h = harness();
    // Pick a key from the fill range. Foyer may still serve this from
    // DRAM (S3-FIFO / LRU promotion is internal); the report flags
    // this as "NVMe-leaning", not "NVMe-only".
    let nvme_key = "nvme-fill/object-7.parquet";
    c.bench_function("shim::GET nvme_leaning_hit", |b| {
        b.to_async(&h.runtime).iter(|| async {
            let app = http::build_s3_shim_router(h.state.clone());
            let resp = app
                .oneshot(build_get_request(nvme_key))
                .await
                .expect("oneshot");
            let _ = drain_body(resp).await;
        });
    });
}

fn bench_get_object_origin_miss(c: &mut Criterion) {
    let h = harness();
    c.bench_function("shim::GET origin_miss", |b| {
        b.to_async(&h.runtime).iter(|| async {
            // Each iteration uses a fresh path. Foyer keys by ETag
            // (which the mock serves as a constant) so this is not a
            // pure miss against Foyer — the shim's HEAD-LRU + Foyer
            // single-flight will short-circuit the second iteration.
            // We still pay the route-match + extractor + path-build
            // cost on every iteration, which is what S2 cares about
            // in the worst-case (a brand-new key, no warm metadata
            // anywhere).
            let id = h.miss_counter.fetch_add(1, Ordering::Relaxed);
            let key = format!("miss/iter-{id}.parquet");
            let app = http::build_s3_shim_router(h.state.clone());
            let resp = app.oneshot(build_get_request(&key)).await.expect("oneshot");
            let _ = drain_body(resp).await;
        });
    });
}

fn bench_head_object(c: &mut Criterion) {
    let h = harness();
    c.bench_function("shim::HEAD cached", |b| {
        b.to_async(&h.runtime).iter(|| async {
            let app = http::build_s3_shim_router(h.state.clone());
            let resp = app
                .oneshot(build_head_request(HOT_KEY))
                .await
                .expect("oneshot");
            let _ = drain_body(resp).await;
        });
    });
}

fn bench_get_object_concurrent(c: &mut Criterion) {
    let h = harness();
    let mut group = c.benchmark_group("shim::GET concurrent_dram_hit");
    for conc in [1usize, 4, 16, 32, 64] {
        group.bench_with_input(BenchmarkId::from_parameter(conc), &conc, |b, &n| {
            b.to_async(&h.runtime).iter(|| async move {
                let mut futs = Vec::with_capacity(n);
                for _ in 0..n {
                    let app = http::build_s3_shim_router(h.state.clone());
                    futs.push(async move {
                        let resp = app
                            .oneshot(build_get_request(HOT_KEY))
                            .await
                            .expect("oneshot");
                        let _ = drain_body(resp).await;
                    });
                }
                join_all(futs).await;
            });
        });
    }
    group.finish();
}

async fn drain_body(resp: axum::http::Response<Body>) -> Bytes {
    use http_body_util::BodyExt;
    let body = resp.into_body();
    body.collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default()
}

criterion_group!(
    benches,
    bench_get_object_dram_hit,
    bench_get_object_nvme_hit,
    bench_get_object_origin_miss,
    bench_head_object,
    bench_get_object_concurrent,
);
criterion_main!(benches);
