//! Compile-time embed of the qx-dashboard static bundle.
//!
//! The Vite build output (`crates/broker/static/dashboard`) is baked
//! into the broker binary via `include_dir!`. Refresh it with:
//!
//!     # in qx-dashboard/
//!     npm run build
//!     cp -R dist/. ../queuex/crates/broker/static/dashboard/
//!     # then rebuild qxd: cargo build --release -p qxd
//!
//! Routes (handled by `server.rs::handle_metrics_connection`):
//!   - GET /                 → index.html
//!   - GET /assets/<file>    → exact match in dist/assets
//!   - GET /favicon.svg etc. → exact match at dist root
//!   - GET /<anything else>  → index.html (SPA history fallback so
//!     react-router-dom routes like /topics, /nodes deep-link work)

use include_dir::{Dir, include_dir};

/// The embedded bundle. Pulled in at compile time. Empty
/// `static/dashboard/` directory short-circuits to a placeholder
/// page via [`render_path`] returning `None` on `/`.
static ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/static/dashboard");

/// `(content_type, body)` for a path under the dashboard, or `None`
/// if the path doesn't resolve to an embedded asset AND isn't a
/// SPA-fallback candidate. The HTTP handler is responsible for
/// turning `None` into a 404 (only happens for unknown `/assets/...`
/// paths — bare URLs and unmatched top-level routes always resolve
/// to `index.html`).
pub fn render_path(path: &str) -> Option<(&'static str, &'static [u8])> {
    // Strip the leading slash and any query string.
    let clean = path.trim_start_matches('/').split('?').next().unwrap_or("");

    // Empty path is the index.
    if clean.is_empty() {
        return index_html();
    }

    // Exact-match lookup first — covers /assets/<hash>.js and
    // top-level files like /favicon.svg, /icons.svg.
    if let Some(file) = ASSETS.get_file(clean) {
        return Some((content_type_for(clean), file.contents()));
    }

    // /assets/* paths that don't exist are real 404s (avoid serving
    // the SPA shell for missing hashed assets — that hides build
    // staleness behind a working-looking page).
    if clean.starts_with("assets/") {
        return None;
    }

    // SPA history fallback: any other unknown path is a client-side
    // route, serve the shell.
    index_html()
}

fn index_html() -> Option<(&'static str, &'static [u8])> {
    ASSETS
        .get_file("index.html")
        .map(|f| ("text/html; charset=utf-8", f.contents()))
}

/// True when no dashboard bundle was embedded. The HTTP handler
/// uses this to keep the legacy `GET /` → Prometheus behaviour for
/// installations that build qxd without the dashboard checked in.
pub fn is_empty() -> bool {
    ASSETS.get_file("index.html").is_none()
}

fn content_type_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "map" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
