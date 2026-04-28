//! Embedded admin UI (feature `ui`).
//!
//! The `ui/dist/` directory — produced by `pnpm --dir shelfd/ui build`
//! — is baked into the binary by `rust-embed` at compile time. The
//! Axum handlers in [`index`] and [`asset`] serve it same-origin at
//! `/ui`, which keeps the SPA on the same port as `/stats`, `/metrics`,
//! and `/admin/*` (no CORS, no second listener).
//!
//! Scope:
//! - Read-only operator dashboard, admin console, and showcase tab
//!   layered on top of the existing HTTP contract shared with
//!   `shelfctl` (see [`crate::control`]). The UI never introduces a
//!   new contract — it consumes what `shelfctl` already depends on, so
//!   the CLI and the browser can never disagree.
//! - Not enabled in the default build. CI stays identical; operators
//!   opt in with `cargo build -p shelfd --features ui` (or the
//!   corresponding Docker ARG).
//!
//! The module is `#[cfg(feature = "ui")]`-gated at the `lib.rs`
//! import, so nothing here compiles when the feature is off.

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

/// Embedded SPA. The folder is evaluated relative to the crate root
/// (i.e. `shelfd/ui/dist/`). A missing directory is a build-time
/// failure when the `ui` feature is enabled — operators either build
/// the UI before `cargo build --features ui`, or drop the feature.
#[derive(RustEmbed)]
#[folder = "ui/dist/"]
struct UiAssets;

/// `GET /ui` — serves `index.html`. Kept as a separate handler (rather
/// than routing `/ui` through [`asset`] with an empty path) so the
/// route table in [`crate::http::build_router`] reads as two explicit
/// lines instead of one cleverly-generic one.
pub async fn index() -> Response {
    serve_path("index.html")
}

/// `GET /ui/*path` — serves a single static asset. Unknown paths
/// return `404`; SPA clients always land on `/ui` first, so we do
/// **not** fall back to `index.html` here — a missing `/ui/foo.js`
/// should surface as a real 404, not a silent HTML redirect that
/// breaks the Content-Type.
pub async fn asset(Path(path): Path<String>) -> Response {
    serve_path(&path)
}

fn serve_path(path: &str) -> Response {
    let file = match UiAssets::get(path) {
        Some(f) => f,
        None => return not_found(),
    };
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let content_type = HeaderValue::from_str(mime.as_ref())
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    // Cache busting: `index.html` changes on every build but keeps a
    // stable URL, so never let intermediaries cache it. Hashed asset
    // filenames from Vite are safe to cache aggressively.
    let cache_control = if path == "index.html" {
        HeaderValue::from_static("no-cache")
    } else {
        HeaderValue::from_static("public, max-age=31536000, immutable")
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache_control)
        .body(Body::from(file.data.into_owned()))
        .unwrap_or_else(|_| not_found())
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}
