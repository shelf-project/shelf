//! shelf-result-cache — SQL result-cache proxy binary.
//!
//! A transparent HTTP proxy that sits in front of Trino and caches query results.
//!
//! # Usage
//!
//! ```bash
//! # Start with default config
//! shelf-result-cache --trino-url http://trino:8080
//!
//! # Start with config file
//! shelf-result-cache --config /etc/shelf/result-cache.yaml
//! ```
//!
//! # Configuration
//!
//! See `Config` struct for all available options. Can be provided via:
//! - Command-line arguments
//! - Environment variables (SHELF_RESULT_CACHE_*)
//! - YAML config file

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use shelf_result_cache::{Config, ResultCacheProxy};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    // Parse config from args/env/file
    let config = parse_config()?;

    info!(
        listen_addr = %config.listen_addr,
        trino_url = %config.trino_url,
        max_cache_bytes = config.max_cache_bytes,
        max_entries = config.max_entries,
        "Starting shelf-result-cache"
    );

    // Create the proxy
    let proxy = ResultCacheProxy::new(config.clone());
    let router = proxy.router();

    // Parse listen address
    let addr: SocketAddr = config
        .listen_addr
        .parse()
        .context("Invalid listen address")?;

    // Start the server
    info!(addr = %addr, "Listening for connections");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}

fn parse_config() -> Result<Config> {
    // TODO: Implement proper config parsing from args/env/file
    // For now, use defaults with environment variable overrides

    let mut config = Config::default();

    if let Ok(trino_url) = std::env::var("SHELF_RESULT_CACHE_TRINO_URL") {
        config.trino_url = trino_url;
    }

    if let Ok(listen_addr) = std::env::var("SHELF_RESULT_CACHE_LISTEN_ADDR") {
        config.listen_addr = listen_addr;
    }

    if let Ok(shelfd_url) = std::env::var("SHELF_RESULT_CACHE_SHELFD_URL") {
        config.shelfd_url = Some(shelfd_url);
    }

    if let Ok(max_cache_bytes) = std::env::var("SHELF_RESULT_CACHE_MAX_BYTES") {
        config.max_cache_bytes = max_cache_bytes
            .parse()
            .context("Invalid SHELF_RESULT_CACHE_MAX_BYTES")?;
    }

    if let Ok(max_entries) = std::env::var("SHELF_RESULT_CACHE_MAX_ENTRIES") {
        config.max_entries = max_entries
            .parse()
            .context("Invalid SHELF_RESULT_CACHE_MAX_ENTRIES")?;
    }

    Ok(config)
}
