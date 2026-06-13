//! Process-level integration test for qxd's SIGHUP reload path.
//!
//! Spawns the qxd binary against a TOML config, sends SIGHUP, and
//! checks that:
//!
//!   1. Editing the `[auth].token` and HUPing rotates the live
//!      bearer token — old clients are rejected, new clients pass.
//!   2. Editing nothing (or only an unrelated comment) and HUPing
//!      reports "no [workflow]/[auth] changes" — the diff cache
//!      short-circuits before any broker mutation.
//!
//! SIGHUP only exists on unix.

#![cfg(unix)]

use std::process::Stdio;
use std::time::Duration;

use qx_broker::Client;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{Instant, sleep, timeout};

#[tokio::test]
async fn sighup_rotates_auth_token_and_skips_no_op_reload() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Pick an unused port up-front.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let config_path = tmp.path().join("qxd.toml");
    write_config_with_token(&config_path, &data_dir, port, "before").await;

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_qx"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to spawn qxd");

    let stderr = child.stderr.take().expect("piped stderr");
    let (stderr_tx, mut stderr_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            eprintln!("qxd> {line}");
            let _ = stderr_tx.send(line);
        }
    });

    wait_until_port_open(port, Duration::from_secs(5)).await;

    // (a) Original token works.
    let url = format!("http://127.0.0.1:{port}");
    let mut before_client = Client::connect_with_token(url.clone(), "before".into())
        .await
        .unwrap();
    before_client.open_topic("t1", 1).await.unwrap();

    // (b) Edit the TOML to rotate the token and HUP.
    write_config_with_token(&config_path, &data_dir, port, "after").await;
    send_sighup(child.id().expect("pid")).await;
    wait_for_stderr(
        &mut stderr_rx,
        "auth token rotated",
        Duration::from_secs(3),
    )
    .await;

    // The old token now fails on a *new* connection.
    let mut stale_client = Client::connect_with_token(url.clone(), "before".into())
        .await
        .unwrap();
    let err = stale_client.open_topic("t2", 1).await.unwrap_err();
    let err_msg = format!("{err:?}");
    assert!(
        err_msg.contains("Unauthenticated"),
        "expected unauthenticated after rotation, got {err_msg}"
    );

    // The new token is accepted.
    let mut new_client = Client::connect_with_token(url.clone(), "after".into())
        .await
        .unwrap();
    new_client.open_topic("t3", 1).await.unwrap();

    // (c) HUP without changing anything → diff cache short-circuits.
    send_sighup(child.id().expect("pid")).await;
    wait_for_stderr(
        &mut stderr_rx,
        "no [workflow]/[auth]/[tls]/[cluster.voters]/[keyx] changes; skipping recompute",
        Duration::from_secs(3),
    )
    .await;

    // Tear down.
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn write_config_with_token(
    path: &std::path::Path,
    data_dir: &std::path::Path,
    port: u16,
    token: &str,
) {
    let text = format!(
        r#"[server]
addr = "127.0.0.1:{port}"
data_dir = "{data}"

# Disable the auto-bound 9090 dashboard listener for this test —
# we're not exercising it here and the default would collide on
# CI hosts running multiple subprocess tests in parallel.
[metrics]
addr = "off"

[auth]
token = "{token}"
"#,
        data = data_dir.display(),
    );
    let mut f = tokio::fs::File::create(path).await.unwrap();
    f.write_all(text.as_bytes()).await.unwrap();
    f.flush().await.unwrap();
}

async fn send_sighup(pid: u32) {
    // tokio's process API doesn't expose kill-with-signal directly
    // and we don't want a libc dep just for the test, so shell out
    // to /bin/kill -HUP.
    let status = tokio::process::Command::new("kill")
        .arg("-HUP")
        .arg(pid.to_string())
        .status()
        .await
        .expect("spawn kill");
    assert!(status.success(), "kill -HUP failed: {status:?}");
}

async fn wait_for_stderr(
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
        panic!("qxd stderr did not contain {needle:?} within {deadline:?}");
    }
}

async fn wait_until_port_open(port: u16, deadline: Duration) {
    let limit = Instant::now() + deadline;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        if Instant::now() >= limit {
            panic!("qxd never bound to port {port} within {deadline:?}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}
