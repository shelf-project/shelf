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

use clap::Parser;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::Config,
    head_lru::HeadLru,
    http::{self, ServerState},
    membership::{DrainSignal, Resolver, ResolverConfig},
    metrics,
    origin::S3Origin,
    router::Router,
    store::FoyerStore,
    telemetry::{self, TelemetryGuard},
};
use tokio_util::sync::CancellationToken;

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
    let origin = Arc::new(S3Origin::new(&config.origin).await?);
    let store = Arc::new(FoyerStore::open(&config.pools).await?);
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&config.admission));
    let head_lru = Arc::new(HeadLru::new(config.head_lru_entries));

    // SHELF-20: shared lameduck bit. Cloned into `ServerState` so
    // `/stats` can advertise it, and into the SIGTERM handler below
    // so we can flip it before shutdown.
    let drain_signal = DrainSignal::new();

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
    // membership resolver already tolerates. `pool_max_idle_per_host
    // = 4` keeps a warm pool to each peer (typical 3-pod cluster)
    // without consuming unnecessary file descriptors.
    let peer_http = reqwest::Client::builder()
        .pool_max_idle_per_host(4)
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
    .with_coalesce_config(config.coalesce.clone());
    tracing::info!(
        coalesce_enabled = config.coalesce.enabled,
        max_gap_bytes = config.coalesce.max_gap_bytes,
        max_coalesced_bytes = config.coalesce.max_coalesced_bytes,
        wait_window_micros = config.coalesce.wait_window_micros,
        consecutive_failures = config.coalesce.consecutive_failures,
        "SHELF-49 coalesced range-GET dispatcher wired into s3_shim"
    );
    state.set_peer_fetch_enabled(peer_fetch_enabled);
    tracing::info!(
        peer_fetch_enabled,
        peer_stats_port = config.membership.stats_port,
        "SHELF-23 peer-fetch wired into s3_shim::handle_get_object"
    );
    if let Some(handle) = reload_handle {
        state = state.with_reload_handle(handle);
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
            if let Some(r) = resolver.as_ref() {
                r.wait_drained(&shutdown).await;
            }
        }
        shutdown.cancel();
    });
}
