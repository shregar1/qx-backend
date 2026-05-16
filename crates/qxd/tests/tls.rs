//! Process-level integration test: spawn the `qxd` binary with a TOML config
//! that enables TLS, then connect a `qx-broker::Client` with TLS and run a
//! send/stream round-trip. Verifies the full external-process producer →
//! qxd-on-the-wire → broker TLS path.

use std::process::Stdio;
use std::time::Duration;

use qx_broker::Client;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn qxd_subprocess_serves_tls_and_round_trips() {
    // 1. Generate a self-signed cert good for "localhost".
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    // 2. Write cert + key + config files.
    let tmp = TempDir::new().unwrap();
    let cert_path = tmp.path().join("cert.pem");
    let key_path = tmp.path().join("key.pem");
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    write_file(&cert_path, cert_pem.as_bytes()).await;
    write_file(&key_path, key_pem.as_bytes()).await;

    // Pick an unused port up-front by binding then releasing — qxd will rebind.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let config_path = tmp.path().join("qxd.toml");
    let config_text = format!(
        r#"[server]
addr = "127.0.0.1:{port}"
data_dir = "{data}"

[tls]
cert_file = "{cert}"
key_file = "{key}"
"#,
        data = data_dir.display(),
        cert = cert_path.display(),
        key = key_path.display(),
    );
    write_file(&config_path, config_text.as_bytes()).await;

    // 3. Spawn qxd as a subprocess.
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_qxd"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to spawn qxd");

    // 4. Wait until qxd is accepting TCP connections on its port (max ~5s).
    let url = format!("https://localhost:{port}");
    wait_until_port_open(port, Duration::from_secs(5)).await;

    // 5. Connect with TLS using the same cert as the trust anchor.
    let mut client = Client::connect_tls(url, cert_pem.into_bytes(), "localhost".into())
        .await
        .expect("TLS connect failed");

    // 6. Round-trip: open a topic, send, stream it back.
    let count = client.open_topic("orders", 4).await.unwrap();
    assert_eq!(count, 4);
    let (partition, _id) = client
        .send("orders", b"user-42", b"hello-over-tls")
        .await
        .unwrap();
    let batch = client
        .stream_from_group("orders", "g1", partition, 10)
        .await
        .unwrap();
    let payloads: Vec<_> = batch.iter().map(|r| r.payload.clone()).collect();
    assert_eq!(payloads, vec![b"hello-over-tls".to_vec()]);

    // 7. Tear down qxd.
    drop(client);
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn write_file(path: &std::path::Path, contents: &[u8]) {
    let mut f = tokio::fs::File::create(path).await.unwrap();
    f.write_all(contents).await.unwrap();
    f.flush().await.unwrap();
}

async fn wait_until_port_open(port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("qxd never bound to port {port} within {timeout:?}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}
