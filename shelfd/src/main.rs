//! `shelfd` binary entry point.
//!
//! Ticket ownership:
//! - SHELF-02 — Axum HTTP server, `/healthz`, `/readyz`, `/metrics`,
//!   graceful shutdown, structured logging.
//! - SHELF-06 — full read-through path (see `http::handlers::get_cache`).
//!
//! `main` composes `Config`, `metrics::Registry`, `origin::S3Origin`,
//! `store::FoyerStore`, `router::Router`, `admission::SizeThresholdPolicy`
//! into an `http::ServerState`, then drives `http::serve` under a
//! SIGTERM/SIGINT-triggered `CancellationToken`.

use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use futures::future::BoxFuture;
use shelfd::{
    admission::SizeThresholdPolicy,
    compaction_rewarm::{FileSpec, RewarmFetcher},
    config::Config,
    head_lru::HeadLru,
    http::{self, ServerState},
    membership::{DrainSignal, Resolver, ResolverConfig},
    metrics,
    origin::S3Origin,
    rewarm_poller::iceberg as rewarm_iceberg,
    router::Router,
    store::FoyerStore,
    telemetry::{self, TelemetryGuard},
};
use tokio_util::sync::CancellationToken;

/// **A3 (rc.7)** — bridges the SHELF-45 reactor's `RewarmFetcher`
/// trait onto an `aws_sdk_s3::Client`. Mirrors the integration
/// test's `S3Fetcher` (see `shelfd/tests/it_compaction_rewarm.rs`)
/// so the production reactor wiring matches the surface that test
/// already pins. The `path` carried by `FileSpec` is treated as
/// either an `s3://`-scheme URL or a bare key under
/// `default_bucket`; the latter shape matches what the
/// `S3MetadataSource` returns for tables whose data files live in
/// the same bucket as their metadata.
#[derive(Debug)]
struct S3OriginRewarmFetcher {
    origin: Arc<S3Origin>,
}

impl S3OriginRewarmFetcher {
    fn new(origin: Arc<S3Origin>) -> Self {
        Self { origin }
    }
}

impl RewarmFetcher for S3OriginRewarmFetcher {
    fn fetch_file(&self, file: &FileSpec) -> BoxFuture<'static, shelfd::Result<Bytes>> {
        let origin = self.origin.clone();
        let path = file.path.clone();
        let size = file.size_bytes;
        Box::pin(async move {
            let (bucket, key) = match rewarm_iceberg::split_s3_url(&path) {
                Some(bk) => bk,
                None => {
                    // Bare key — fall through to the configured
                    // origin bucket. The metadata source does emit
                    // `s3://` URLs in practice, so this branch is
                    // a defensive fallback rather than a hot path.
                    let bucket = origin.bucket().to_owned();
                    (bucket, path)
                }
            };
            // `get_range(bucket, key, 0, size)` is the existing
            // SHELF-05 surface. The reactor warms a content-
            // addressed key derived from `(etag, 0, size, 0)`,
            // so the full-file range matches what the reactor
            // expects.
            shelfd::origin::Origin::get_range(origin.as_ref(), &bucket, &key, 0, size).await
        })
    }
}

/// Command-line arguments for `shelfd`. Kept intentionally small; all
/// tunables live in `Config` (see `shelfd::config::Config` +
/// `contracts/config-keys.md`).
#[derive(Debug, Parser)]
#[command(
    name = "shelfd",
    version,
    about = "Shelf cache daemon",
    long_about = "Row-group-granular, Iceberg-native read cache for Trino. \
                  See shelf/BLUEPRINT.md §6.1 and shelf/agents/out/03-plan.md."
)]
struct Args {
    /// Path to the shelfd YAML config file.
    #[arg(long, env = "SHELFD_CONFIG", default_value = "/etc/shelfd/config.yaml")]
    config: std::path::PathBuf,

    /// Override the log level (`RUST_LOG`-compatible filter).
    #[arg(long, env = "SHELFD_LOG", default_value = "info,shelfd=debug")]
    log: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async { run(args).await })
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config =
        Config::from_path(&args.config).map_err(|e| anyhow::anyhow!("config load failed: {e}"))?;

    // SHELF-08: install the subscriber AFTER config is parsed so the
    // OTLP endpoint + pod id can flow into the tracer resource. The
    // guard flushes pending spans on drop at the end of `run`.
    let _telemetry: TelemetryGuard =
        telemetry::init(&args.log, &config.observability, &config.node.id)?;
    if _telemetry.otlp_enabled() {
        tracing::info!(
            endpoint = ?config.observability.otlp_endpoint,
            "otlp tracing exporter enabled",
        );
    }
    tracing::info!(node = %config.node.id, "shelfd starting");

    let metrics = Arc::new(metrics::Registry::init()?);

    // SHELF-50 — flip the decoded-metadata LRU on/off based on the
    // `cache.decodedMeta.enabled` config. The cache itself is a
    // process-wide singleton; we only toggle the runtime gate here.
    // Default off, so this is a no-op until an operator opts in.
    shelfd::decoded_meta::set_enabled(config.decoded_meta.enabled);
    if config.decoded_meta.enabled {
        tracing::info!(
            max_manifest_entries = config.decoded_meta.max_manifest_entries,
            max_footer_entries = config.decoded_meta.max_footer_entries,
            "SHELF-50 decoded-metadata cache enabled",
        );
    }

    let origin = Arc::new(S3Origin::new(&config.origin).await?);

    // SHELF-20: shared lameduck bit. Cloned into `ServerState` so
    // `/stats` can advertise it, into the SIGTERM handler below so
    // we can flip it before shutdown, and — A2 (rc.7) — into the
    // `FoyerStore` admit gate so a draining pod stops paying for
    // S3 GETs and Foyer inserts that are about to be evicted with
    // the pod itself. Constructed *before* `FoyerStore::open` so the
    // `with_drain` builder receives a live clone rather than a
    // throwaway one.
    let drain_signal = DrainSignal::new();

    // **A2 (rc.7)** — `cache.drain.refuse_admits` (default `true`)
    // is the operator-facing rollback flag. Flipping it to `false`
    // is a config-only revert; no rolling restart is required for
    // the gate to disengage. See ADR-0027.
    // **A6 (rc.7)** — wire the cooperative peer-admission gate from
    // `cache.coopAdmission`. Default-off so a stock OSS deploy
    // behaves identically to pre-A6; operators flip
    // `coopAdmission.enabled = true` once the new counters are
    // surfaced on the dashboard. See ADR-0037.
    let coop_gate = shelfd::coop_admission::CoopAdmissionGate::new(config.coop_admission.clone());

    // **B3 (rc.7)** — wire the intermediate-table opt-out gate from
    // `cache.transientAdmission`. Default-off; a stock OSS deploy
    // behaves identically to pre-B3 (the gate is a strict no-op
    // when `enabled = false`). v1 ships without an automatic
    // metadata.json refresher: the gate consults the `overrides`
    // map only, which gives operators an immediate-value lever
    // without the bucket-routing wiring a full refresher would
    // require. A follow-up B3.1 will plug an
    // `S3MetadataReader` impl that mirrors the rewarm-poller's
    // `S3MetadataSource` and consumes a watchedTables list. See
    // ADR-0038.
    let transient_gate = std::sync::Arc::new(shelfd::transient_admission::TransientGate::new(
        config.transient_admission.clone(),
    ));

    let store = Arc::new(
        FoyerStore::open(&config.pools)
            .await?
            .with_drain(drain_signal.clone(), config.drain.refuse_admits)
            .with_coop_admission(coop_gate)
            .with_transient_admission(transient_gate.clone()),
    );
    if config.drain.refuse_admits {
        tracing::info!(
            grace_seconds = config.drain.grace_seconds,
            "A2 drain-aware admission engaged (refuse_admits=true)"
        );
    } else {
        tracing::warn!(
            "A2 drain-aware admission disengaged via cache.drain.refuse_admits=false; \
             rolling-restart admits will continue feeding the cache while the pod terminates"
        );
    }
    if config.coop_admission.enabled {
        tracing::info!(
            replication_factor = config.coop_admission.replication_factor,
            "A6 cooperative peer-admission gate engaged (enabled=true)"
        );
    } else {
        tracing::debug!(
            replication_factor = config.coop_admission.replication_factor,
            "A6 cooperative peer-admission gate disabled (cache.coopAdmission.enabled=false); \
             every peer-fetched byte will admit unchanged"
        );
    }
    if config.transient_admission.enabled {
        tracing::info!(
            transient_threshold = ?config.transient_admission.transient_threshold,
            decision_cache_ttl = ?config.transient_admission.decision_cache_ttl,
            overrides = config.transient_admission.overrides.len(),
            "B3 intermediate-table admission gate engaged (enabled=true)"
        );
    } else {
        tracing::debug!(
            "B3 intermediate-table admission gate disabled \
             (cache.transientAdmission.enabled=false); every Iceberg \
             table admits unchanged"
        );
    }
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&config.admission));
    let head_lru = Arc::new(HeadLru::new(config.head_lru_entries));

    // `shutdown` is the *hard* cancellation token: every long-lived
    // task selects on it. The signal handler only cancels it AFTER
    // the lameduck grace window has elapsed (or a second SIGTERM
    // forces an immediate exit). See `spawn_signal_handler` below.
    let shutdown = CancellationToken::new();

    // Track G-11 — rolling-hit-ratio sampler + cold-start warm-up SLI.
    // Detached: the task exits on shutdown, no graceful join needed.
    let _warm_sampler = shelfd::warm_sampler::spawn(
        shelfd::warm_sampler::DEFAULT_WARM_THRESHOLD_BPS,
        shutdown.clone(),
    );

    // SHELF-24: construct the pin-list loader BEFORE building
    // `ServerState` so the resulting `ReloadHandle` can be threaded
    // through `with_reload_handle`. The loader runs regardless of
    // whether the admin surface is reachable — the handle just lets
    // `POST /admin/reload` short-circuit the timer.
    let reload_handle = match config.pin_list.as_ref() {
        Some(cfg) if cfg.enabled => {
            use shelfd::pinlist::PinListLoader;
            let loader = PinListLoader::new(
                origin.client().clone(),
                cfg.bucket.clone(),
                cfg.key.clone(),
                cfg.refresh_period,
                store.clone(),
            );
            match loader.boot_and_spawn(shutdown.clone()).await {
                Ok((handle, _join)) => {
                    tracing::info!(
                        bucket = %cfg.bucket,
                        key = %cfg.key,
                        refresh_period = ?cfg.refresh_period,
                        "pin-list loader online",
                    );
                    Some(handle)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "pin-list loader failed to start; continuing without");
                    None
                }
            }
        }
        Some(_) => {
            tracing::info!("pin-list loader disabled by config (pin_list.enabled=false)");
            None
        }
        None => {
            tracing::debug!("no pin_list stanza; pin-list loader not spawned");
            None
        }
    };

    // SHELF-23 — peer-fetch HTTP client. Uses the membership stats
    // timeout as a hard request ceiling so a wedged peer never
    // extends the outer Trino request beyond the same window the
    // membership resolver already tolerates. Single shared instance
    // for the lifetime of this process (reqwest::Client is cheap to
    // clone — internal Arc — so threading it through ServerState is
    // the canonical reuse pattern).
    //
    // S2 (rc.8 / ADR-0040) — explicit pool + HTTP/2 keepalive config
    // so peer-fetch doesn't pay handshake cost on every refresh and
    // can multiplex over a single h2c stream when peers negotiate
    // HTTP/2. `pool_max_idle_per_host = 8` is a small bump from the
    // pre-S2 value of 4: peer count is ≤ shelf-pool size (typically
    // 3–6 pods) so 8 idle conns/peer covers the warm path without
    // bloating fd usage. `pool_idle_timeout = 90s` matches S3's
    // server-side keep-alive default and outlives a typical Trino
    // query pause. The `http2_keep_alive_*` knobs are no-ops on the
    // HTTP/1.1 path (peers default to h2c when both sides advertise
    // it) but cost nothing if HTTP/2 isn't negotiated.
    let peer_http = reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .http2_keep_alive_interval(std::time::Duration::from_secs(30))
        .http2_keep_alive_timeout(std::time::Duration::from_secs(60))
        .http2_keep_alive_while_idle(true)
        .tcp_nodelay(true)
        .timeout(config.membership.stats_timeout)
        .build()
        .map_err(|e| anyhow::anyhow!("peer-fetch reqwest::Client build: {e}"))?;
    let peer_fetch_enabled = std::env::var("SHELFD_PEER_FETCH_ENABLED")
        .map(|v| {
            // Accept the same truthy spellings systemd / k8s use:
            // 1 / true / yes / on (case-insensitive). Anything else
            // disables peer-fetch — operator can flip the env var
            // without rebuilding the binary.
            matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(true);

    // SHELF-40 — load the audit-able cost model. `from_config`
    // refuses to register on negative coefficients / unknown
    // regions, which is exactly the "fail loud" gate the design
    // note demands. When `cache.cost.enabled = false` we still
    // build a `CostState` so the `with_cost_state` builder is
    // unconditional — but the `enabled` bit short-circuits every
    // hot-path bump.
    let cost_state = match shelfd::cost::CostState::from_config(&config.cost) {
        Ok(cs) => {
            tracing::info!(
                region = %cs.region(),
                enabled = cs.is_enabled(),
                "SHELF-40 cost state initialised"
            );
            cs
        }
        Err(e) => {
            tracing::error!(error = %e, "SHELF-40 cost state failed to initialise; counter disabled");
            shelfd::cost::CostState::disabled()
        }
    };

    // SHELF-42 — install A/B tag receive-side state from
    // `cache.abTag.*`. The Trino plugin's *forwarding* side is always
    // on; the daemon side is default-off so freshly deployed OSS pods
    // never expose tag-cardinality surface area until the operator
    // opts in via Helm or `SHELFD_AB_TAG=on`.
    let ab_tag_state = shelfd::ab_tag::AbTagState::new(
        config.ab_tag.enabled,
        config.ab_tag.max_distinct_tags,
        config.ab_tag.scrape_window,
    );
    tracing::info!(
        ab_tag_enabled = config.ab_tag.enabled,
        ab_tag_max_distinct_tags = config.ab_tag.max_distinct_tags,
        ab_tag_scrape_window = ?config.ab_tag.scrape_window,
        "SHELF-42 ab_tag receive path configured"
    );

    let mut state = ServerState::with_head_lru_and_pod_id(
        store.clone(),
        origin.clone(),
        router.clone(),
        admission,
        metrics,
        head_lru,
        config.node.id.clone(),
    )
    .with_drain_signal(drain_signal.clone())
    .with_peer_fetch(peer_http, config.membership.stats_port)
    .with_cost_state(cost_state.clone())
    .with_ab_tag(ab_tag_state);
    state.set_peer_fetch_enabled(peer_fetch_enabled);
    tracing::info!(
        peer_fetch_enabled,
        peer_stats_port = config.membership.stats_port,
        "SHELF-23 peer-fetch wired into s3_shim::handle_get_object"
    );

    // SHELF-40 — spawn the rolling-rate updater. No-op when the
    // cost state is disabled. Cancelled on shutdown via the same
    // `shutdown` token the data plane already observes.
    cost_state.spawn_rate_updater(shutdown.clone());

    // A4 (rc.7) — spawn the net dollars-saved accountant. The
    // accountant always publishes the
    // `shelf_pool_amortized_dollars_per_hour` gauge (so dashboards
    // can spot the unset state) but only credits the net counter
    // when `cache.cost.amortized_dollars_per_hour` is a positive,
    // finite value (anti-overclaim guard). See ADR-0028 for the
    // rollout rationale and `shelfd/src/cost.rs` for the model.
    let net_accountant = Arc::new(shelfd::cost::NetCostAccountant::new(
        config.cost.amortized_dollars_per_hour,
    ));
    tracing::info!(
        amortized_micros_per_hour = net_accountant.amortized_micros_per_hour(),
        publishable = net_accountant.is_publishable(),
        "A4 (rc.7) net cost accountant initialised"
    );
    shelfd::cost::spawn_net_accountant(
        net_accountant,
        cost_state.region().to_owned(),
        shutdown.clone(),
    );

    // **A3 (rc.7)** — compaction-rewarm metadata.json poller. Spawns
    // both the SHELF-45 reactor (no other producer wires it in v1.0
    // since the SHELF-37 listener is parked) and the new poller
    // that drives it. Default-OFF via `cache.rewarm.enabled`; even
    // when on, an empty `cache.rewarm.tables` is the explicit
    // no-op. Composability with A1/A2/A4 is implicit: the reactor
    // calls `store.get_or_fetch` which already routes through the
    // unified admit gate (drain → policy → LODC → rate-limit). See
    // ADR-0036.
    if config.rewarm.enabled && !config.rewarm.tables.is_empty() {
        let fetcher: Arc<dyn shelfd::compaction_rewarm::RewarmFetcher> =
            Arc::new(S3OriginRewarmFetcher::new(origin.clone()));
        let rewarm_admission = Arc::new(SizeThresholdPolicy::from_config(&config.admission));
        let reactor = shelfd::compaction_rewarm::CompactionReactor::new(
            config.rewarm.clone(),
            store.clone(),
            fetcher,
            rewarm_admission,
        );
        let (event_tx, _reactor_handle) = reactor.spawn(tokio_util::sync::CancellationToken::new());
        let publisher = shelfd::compaction_rewarm::SnapshotPublisher::new(event_tx);
        let metadata_source: Arc<dyn shelfd::rewarm_poller::MetadataSource> = Arc::new(
            shelfd::rewarm_poller::S3MetadataSource::new(origin.client().clone()),
        );
        let poller = Arc::new(shelfd::rewarm_poller::RewarmPoller::new(
            config.rewarm.clone(),
            metadata_source,
            publisher,
            drain_signal.clone(),
        ));
        let poller_shutdown = shutdown.clone();
        tokio::spawn(async move { poller.run(poller_shutdown).await });
        tracing::info!(
            tables = config.rewarm.tables.len(),
            poll_interval = ?config.rewarm.poll_interval,
            cap_bytes = config.rewarm.max_bytes_per_snapshot,
            "A3 rewarm poller spawned (SHELF-45 reactor wired off the metadata-json source)",
        );
    } else if config.rewarm.enabled {
        tracing::info!(
            "A3 rewarm poller idle: cache.rewarm.enabled=true but cache.rewarm.tables is empty",
        );
    }

    if let Some(handle) = reload_handle {
        state = state.with_reload_handle(handle);
    }
    // SHELF-46 — wire bloom-aware footer admission. Default
    // `enabled: false` in the OSS chart means this branch installs
    // a `None` (s3-shim falls through to the existing path); flip
    // `cache.bloom.enabled=true` in the values file to engage.
    if config.bloom_admission.enabled {
        let runtime_cfg = config.bloom_admission.to_runtime();
        tracing::info!(
            max_index_entries = runtime_cfg.max_index_entries,
            min_footer_bytes = runtime_cfg.min_footer_bytes,
            "SHELF-46 bloom-aware footer admission enabled"
        );
        let bloom = Arc::new(shelfd::parquet_admit::BloomAdmission::new(runtime_cfg));
        state = state.with_bloom_admission(bloom);
    } else {
        tracing::debug!(
            "SHELF-46 bloom-aware footer admission disabled (cache.bloom.enabled=false)"
        );
    }
    let state = Arc::new(state);
    // Phase-0: mark ready as soon as Foyer + S3 client built. The
    // origin head-bucket probe would go here once SHELF-07 lands.
    state.mark_ready();

    // SHELF-20: spawn the membership resolver AFTER `state.mark_ready`
    // so peers' first probe sees `ready=true`. The resolver writes
    // into the same `Arc<Router>` we threaded into `ServerState`, so
    // `is_local_owner` and `/admin/ring` see live updates with no
    // extra plumbing.
    //
    // Resolver lifecycle is "spawn → drive `Router::update` →
    // observe shutdown → exit". The signal handler installed below
    // calls `resolver.begin_drain()` before cancelling `shutdown`,
    // which makes the next `/stats` probe from peers carry
    // `draining: true` and rotates this pod out of their rings.
    let resolver = if config.membership.enabled {
        let resolver_cfg = ResolverConfig {
            headless_service: config.membership.headless_service.clone(),
            stats_port: config.membership.stats_port,
            data_port: config.membership.data_port,
            dns_refresh: config.membership.dns_refresh,
            stats_timeout: config.membership.stats_timeout,
            self_id: config.node.id.clone(),
            drain_grace: config.membership.drain_grace,
            weight_unit_bytes: config.membership.weight_unit_bytes,
        };
        tracing::info!(
            headless = %resolver_cfg.headless_service,
            stats_port = resolver_cfg.stats_port,
            data_port = resolver_cfg.data_port,
            dns_refresh = ?resolver_cfg.dns_refresh,
            drain_grace = ?resolver_cfg.drain_grace,
            "spawning membership resolver",
        );
        match Resolver::spawn(
            resolver_cfg,
            router.clone(),
            drain_signal.clone(),
            shutdown.clone(),
        ) {
            Ok(r) => Some(Arc::new(r)),
            Err(e) => {
                tracing::error!(error = %e, "membership resolver failed to start; shelfd will run with an empty ring");
                None
            }
        }
    } else {
        tracing::info!("membership resolver disabled by config (membership.enabled=false)");
        None
    };

    spawn_signal_handler(shutdown.clone(), drain_signal.clone(), resolver.clone());

    let listen = config.http.listen;
    let request_timeout = config.http.request_timeout;
    tracing::info!(%listen, ?request_timeout, "binding data plane");

    if config.s3_shim.enabled {
        // SHELF-22: dedicated port keeps generic-S3 clients off
        // the native read path so a hot boto3 loop cannot starve
        // Trino splits sharing this daemon's event loop.
        let shim_addr: std::net::SocketAddr = config.s3_shim.bind_address.parse().map_err(|e| {
            anyhow::anyhow!(
                "s3_shim.bind_address='{}' is not a valid SocketAddr: {e}",
                config.s3_shim.bind_address,
            )
        })?;
        state.s3_shim_max_full_object_bytes.store(
            config.s3_shim.max_full_object_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );
        tracing::info!(%shim_addr, "binding s3-compat shim");
        let data_fut = http::serve(listen, state.clone(), request_timeout, shutdown.clone());
        let shim_fut =
            http::serve_s3_shim(shim_addr, state.clone(), request_timeout, shutdown.clone());
        tokio::select! {
            r = data_fut => r?,
            r = shim_fut => r?,
        }
    } else {
        http::serve(listen, state, request_timeout, shutdown.clone()).await?;
    }

    // SHELF-20: data plane has stopped accepting new connections.
    // Wait for the resolver loop to observe shutdown and exit so we
    // don't race the `JoinHandle` drop in `Resolver::Drop`.
    if let Some(r) = resolver.as_ref() {
        if let Err(e) = r.join_once().await {
            tracing::warn!(error = %e, "resolver task did not exit cleanly");
        }
    }

    tracing::info!("shelfd shutdown complete");
    Ok(())
}

/// SIGTERM / SIGINT handler that orchestrates the SHELF-20 lameduck
/// shutdown sequence:
///
/// 1.  Wait for the first signal.
/// 2.  Flip [`DrainSignal::begin`] so the next `/stats` probe carries
///     `draining: true`. Peers' resolvers drop us from their HRW
///     rings within `dns_refresh + max(p99 stats probe latency)`.
/// 3.  Block on `Resolver::wait_drained` (sleeps `drain_grace`, or
///     races against `shutdown` for the hard-kill path).
/// 4.  Cancel `shutdown`. The data plane stops accepting new
///     connections; the `Resolver` loop exits.
/// 5.  If a *second* signal arrives during the grace window, skip
///     the wait and cancel immediately. This is what an operator
///     hitting Ctrl-C twice expects.
///
/// On non-unix builds we degrade to a single Ctrl-C trigger. There is
/// no second-signal escape hatch on Windows because the only relevant
/// production target is linux/amd64+linux/arm64.
fn spawn_signal_handler(
    shutdown: CancellationToken,
    drain: DrainSignal,
    resolver: Option<Arc<Resolver>>,
) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "SIGTERM handler setup failed");
                    return;
                }
            };
            let mut int = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "SIGINT handler setup failed");
                    return;
                }
            };
            tokio::select! {
                _ = term.recv() => tracing::info!("SIGTERM received"),
                _ = int.recv()  => tracing::info!("SIGINT received"),
            }

            drain.begin();
            // **A2 (rc.7)** — surface the drain bit on the operator-
            // facing dashboard. Done *here*, not inside `DrainSignal`,
            // so the signal stays a pure data type with no transitive
            // dependency on the metrics registry. Single point of
            // truth for the SIGTERM path.
            shelfd::metrics::DRAIN_ACTIVE.set(1);
            tracing::info!("drain signal raised; advertising draining=true on /stats");

            if let Some(r) = resolver.as_ref() {
                let grace = r.config().drain_grace;
                tracing::info!(?grace, "entering lameduck grace window");
                tokio::select! {
                    _ = r.wait_drained(&shutdown) => {
                        tracing::info!("lameduck grace elapsed");
                    }
                    _ = term.recv() => {
                        tracing::warn!("second SIGTERM received; skipping grace");
                    }
                    _ = int.recv() => {
                        tracing::warn!("second SIGINT received; skipping grace");
                    }
                }
            } else {
                tracing::info!("no resolver — cancelling shutdown immediately");
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %e, "ctrl_c handler failed");
                return;
            }
            tracing::info!("Ctrl-C received");
            drain.begin();
            shelfd::metrics::DRAIN_ACTIVE.set(1);
            if let Some(r) = resolver.as_ref() {
                r.wait_drained(&shutdown).await;
            }
        }
        shutdown.cancel();
    });
}
