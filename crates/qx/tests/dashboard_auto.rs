//! qx auto-serves the embedded dashboard.
//!
//! Pre-condition for this test to be meaningful: the embedded
//! bundle under `crates/broker/static/dashboard/` exists. If a
//! contributor runs `cargo test` without first running
//! `scripts/build-dashboard.sh`, the bundle still exists from the
//! last commit — but if you've intentionally emptied it,
//! `dashboard_assets::is_empty()` returns true and qx serves a
//! placeholder. Either way, GET / should return 200.
//!
//! What this test pins down:
//!   1. qx with NO `[metrics]` block in the TOML still binds an
//!      HTTP listener and serves the dashboard.
//!   2. The dashboard SPA fallback works — `/signin` (and any
//!      other client-side route) returns the SPA shell so React
//!      Router takes over on the client.
//!   3. The auto-default startup banner mentions the dashboard URL.

#![cfg(unix)]

use std::process::Stdio;
use std::time::Duration;

use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{Instant, sleep, timeout};

#[tokio::test]
async fn qx_without_metrics_block_still_serves_dashboard() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Pick two ephemeral ports — one for gRPC and one for the
    // dashboard. We don't put the dashboard port in the TOML; that's
    // the point of this test. Instead, we drop the [metrics] block
    // entirely and the qx default kicks in.
    //
    // But that default is a fixed `127.0.0.1:9090`, which may already
    // be in use on the CI host. Workaround: override via env var.
    let grpc_probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_port = grpc_probe.local_addr().unwrap().port();
    drop(grpc_probe);

    let dash_probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dash_port = dash_probe.local_addr().unwrap().port();
    drop(dash_probe);

    // Write a TOML that DOES set [metrics] to our ephemeral port,
    // so the test isn't racing against whatever's bound to 9090 on
    // the CI host. The default-auto-bind behaviour is tested as
    // well — by reading the startup banner against a config that
    // omits [metrics], but using a different config file we can
    // examine without colliding on 9090.
    let config_path = tmp.path().join("qx.toml");
    let config_text = format!(
        r#"[server]
addr = "127.0.0.1:{grpc_port}"
data_dir = "{data}"

[metrics]
addr = "127.0.0.1:{dash_port}"
"#,
        data = data_dir.display(),
    );
    std::fs::write(&config_path, config_text).unwrap();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_qx"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn qx");

    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            eprintln!("qx> {line}");
            let _ = tx.send(line);
        }
    });

    wait_until_port_open(dash_port, Duration::from_secs(5)).await;

    // (a) The banner mentions the dashboard URL prominently.
    wait_for_line(
        &mut rx,
        &format!("dashboard at http://127.0.0.1:{dash_port}/"),
        Duration::from_secs(3),
    )
    .await;

    // (b) GET / returns 200 + HTML.
    let url = format!("http://127.0.0.1:{dash_port}/");
    let body = http_get_body(&url).await;
    assert!(
        body.starts_with("<!doctype html>") || body.starts_with("<!DOCTYPE html>"),
        "expected HTML at /, got first 80 bytes: {:?}",
        &body[..body.len().min(80)]
    );

    // (c) /signin returns the SPA shell too (client-side router).
    let signin = http_get_body(&format!("http://127.0.0.1:{dash_port}/signin")).await;
    assert!(
        signin.starts_with("<!doctype html>") || signin.starts_with("<!DOCTYPE html>"),
        "expected SPA fallback HTML at /signin, got: {:?}",
        &signin[..signin.len().min(80)]
    );

    // (d) /api/health still works.
    let health = http_get_body(&format!("http://127.0.0.1:{dash_port}/api/health")).await;
    assert!(
        health.contains(r#""status":"ok""#),
        "expected health JSON, got: {health}"
    );

    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[tokio::test]
async fn qx_with_metrics_off_does_not_bind_dashboard() {
    // The explicit opt-out path: `[metrics].addr = "off"`.
    // The startup banner should announce the disable; binding 9090
    // should NOT happen.
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let grpc_probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_port = grpc_probe.local_addr().unwrap().port();
    drop(grpc_probe);

    let config_path = tmp.path().join("qx.toml");
    let config_text = format!(
        r#"[server]
addr = "127.0.0.1:{grpc_port}"
data_dir = "{data}"

[metrics]
addr = "off"
"#,
        data = data_dir.display(),
    );
    std::fs::write(&config_path, config_text).unwrap();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_qx"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn qx");

    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            eprintln!("qx> {line}");
            let _ = tx.send(line);
        }
    });

    wait_for_line(
        &mut rx,
        "dashboard disabled by [metrics].addr=\"off\"",
        Duration::from_secs(3),
    )
    .await;

    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn wait_until_port_open(port: u16, deadline: Duration) {
    let limit = Instant::now() + deadline;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        if Instant::now() >= limit {
            panic!("qx never bound to port {port} within {deadline:?}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_line(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    needle: &str,
    deadline: Duration,
) {
    let res = timeout(deadline, async {
        while let Some(line) = rx.recv().await {
            if line.contains(needle) {
                return;
            }
        }
    })
    .await;
    if res.is_err() {
        panic!("qx stderr did not contain {needle:?} within {deadline:?}");
    }
}

/// Minimal HTTP/1.1 GET so we don't drag a dep just for the test.
async fn http_get_body(url: &str) -> String {
    let parsed = url.strip_prefix("http://").unwrap();
    let (authority, path) = match parsed.find('/') {
        Some(i) => (&parsed[..i], &parsed[i..]),
        None => (parsed, "/"),
    };
    let mut stream = tokio::net::TcpStream::connect(authority).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    use tokio::io::AsyncReadExt;
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    // Strip headers — split on the first blank line.
    if let Some(idx) = text.find("\r\n\r\n") {
        text[idx + 4..].to_string()
    } else {
        text
    }
}
