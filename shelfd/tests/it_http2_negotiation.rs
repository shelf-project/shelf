//! S2 (rc.8 / ADR-0040) — assert shelfd's `axum::serve` listener
//! negotiates HTTP/2 over cleartext (h2c) when the client sends the
//! HTTP/2 connection preface. Backstops the audit finding that the
//! workspace `axum` `http2` feature + `hyper-util`'s `auto::Builder`
//! already give us h2c "for free" without any source change.
//!
//! Why a dedicated integration test:
//! - `axum::serve` is wired up in two places
//!   (`shelfd::http::serve` data plane + `shelfd::http::serve_s3_shim`
//!   for the S3-compat shim). Both use the same `auto::Builder`
//!   under the hood, so a single test exercising the same
//!   `axum::serve(listener, router)` call site is sufficient
//!   coverage for both.
//! - We cannot rely on a pure unit test because the codec
//!   negotiation happens between hyper's TCP read path and the
//!   client's HTTP/2 prior-knowledge writer; both sides have to
//!   drive real I/O.
//!
//! NOT gated on the `integration` feature: this test does NOT need
//! MinIO or any external service — it spins up a 0-byte axum
//! handler on a loopback port and a `reqwest::Client` with
//! `http2_prior_knowledge()`. Plain `cargo test -p shelfd` runs it.

#![cfg(test)]

use std::net::SocketAddr;
use std::time::Duration;

use axum::{routing::get, Router};
use tokio::net::TcpListener;

/// Spin up a minimal axum::serve listener on an ephemeral loopback
/// port. Returns the bound address so the client can reach it.
async fn spawn_min_axum() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
        "/ping",
        get(|| async { axum::http::StatusCode::NO_CONTENT }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, handle)
}

/// Positive: a client that asserts HTTP/2 prior knowledge gets an
/// HTTP/2 response from `axum::serve`. This is the load-bearing
/// audit assertion — if `axum::serve` ever forces HTTP/1.1 only
/// (e.g. via a future workspace dep change that drops the `http2`
/// feature), this test fails.
#[tokio::test]
async fn axum_serve_negotiates_http2_with_prior_knowledge() {
    let (addr, _server) = spawn_min_axum().await;
    // Give the listener a beat to be ready. `axum::serve` binds in
    // the spawn closure, but the underlying `accept` loop only
    // runs after the await yields once. A 50 ms cushion is enough
    // on every CI we ship to.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest h2c client");

    let url = format!("http://{addr}/ping");
    let resp = client.get(&url).send().await.expect("h2c request");
    assert_eq!(
        resp.version(),
        reqwest::Version::HTTP_2,
        "axum::serve must negotiate HTTP/2 (h2c) when the client \
         sends the HTTP/2 prior-knowledge preface; observed {:?}",
        resp.version()
    );
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}

/// Negative-control: a default `reqwest::Client` (no prior
/// knowledge, no ALPN since this is plaintext) gets HTTP/1.1.
/// Locks in the backward-compat property: existing HTTP/1.1
/// clients (Trino's native S3 client today) continue to work
/// after the S2 audit. If a future change forces HTTP/2 only on
/// the server, this test fails — which is exactly the regression
/// we want to catch.
#[tokio::test]
async fn axum_serve_keeps_http1_for_default_clients() {
    let (addr, _server) = spawn_min_axum().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest default client");

    let url = format!("http://{addr}/ping");
    let resp = client.get(&url).send().await.expect("h1 request");
    assert_eq!(
        resp.version(),
        reqwest::Version::HTTP_11,
        "default reqwest client (no prior knowledge, plaintext, no \
         ALPN) must continue to receive HTTP/1.1 from axum::serve; \
         observed {:?}",
        resp.version()
    );
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}
