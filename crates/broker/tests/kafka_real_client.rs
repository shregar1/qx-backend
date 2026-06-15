//! Real Kafka client validation: spawn `kcat` (librdkafka 2.x) against a
//! live qx broker and verify metadata, produce, consume, and admin paths
//! all work over the wire.

use std::time::Duration;

use qx_broker::{Broker, KafkaConfig};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;

fn kcat_available() -> bool {
    std::process::Command::new("kcat")
        .arg("-V")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

async fn boot_full(
    dir: &TempDir,
) -> (
    std::sync::Arc<Broker>,
    std::net::SocketAddr,
    std::net::SocketAddr,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let broker = Broker::open(dir.path());

    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let grpc_handle = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_with_listener(grpc_listener).await.unwrap();
        })
    };

    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let kafka_handle = {
        let broker = broker.clone();
        let cfg = KafkaConfig {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: kafka_addr.port() as i32,
            cluster_id: "qx-real".to_string(),
            node_id: 0,
            ..Default::default()
        };
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    (broker, grpc_addr, kafka_addr, vec![grpc_handle, kafka_handle])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kcat_metadata_list() {
    if !kcat_available() {
        println!("kcat not installed; skipping");
        return;
    }
    let dir = TempDir::new().unwrap();
    let (broker, _, addr, handles) = boot_full(&dir).await;

    let out = Command::new("kcat")
        .arg("-L")
        .arg("-b")
        .arg(format!("127.0.0.1:{}", addr.port()))
        .output()
        .await
        .expect("kcat -L spawn");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    broker.shutdown().await;
    for h in handles {
        let _ = h.await;
    }

    assert!(out.status.success(), "kcat -L failed\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("broker") || stdout.contains("Metadata"),
        "kcat -L output didn't look like metadata: {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kcat_produce() {
    if !kcat_available() {
        println!("kcat not installed; skipping");
        return;
    }
    let dir = TempDir::new().unwrap();
    let (broker, grpc_addr, kafka_addr, handles) = boot_full(&dir).await;

    let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    grpc.open_topic("rc-events", 1).await.unwrap();

    let bootstrap = format!("127.0.0.1:{}", kafka_addr.port());

    let mut producer = Command::new("kcat")
        .arg("-P")
        .arg("-b").arg(&bootstrap)
        .arg("-t").arg("rc-events")
        .arg("-p").arg("0")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn kcat -P");
    let mut stdin = producer.stdin.take().unwrap();
    stdin.write_all(b"hello-from-kcat\n").await.unwrap();
    stdin.write_all(b"second-message\n").await.unwrap();
    drop(stdin);
    let prod_out =
        tokio::time::timeout(Duration::from_secs(15), producer.wait_with_output())
            .await
            .expect("kcat -P timeout")
            .unwrap();
    let prod_stderr = String::from_utf8_lossy(&prod_out.stderr);

    let recs_before_shutdown = broker.topic_records("rc-events", 64).unwrap();

    broker.shutdown().await;
    for h in handles {
        let _ = h.await;
    }

    assert!(
        prod_out.status.success(),
        "kcat -P failed:\n{prod_stderr}"
    );
    assert!(
        recs_before_shutdown.iter().any(|r| r == b"hello-from-kcat"),
        "produce claimed success but topic doesn't have the record: {:?}",
        recs_before_shutdown
            .iter()
            .map(|r| String::from_utf8_lossy(r).to_string())
            .collect::<Vec<_>>()
    );
}

/// `kcat -C` consumes records produced via `kcat -P`. Captures librdkafka
/// debug output to a tempfile so future regressions are diagnosable
/// (a Fetch response librdkafka can't parse manifests as a silent hang).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kcat_consume() {
    if !kcat_available() {
        println!("kcat not installed; skipping");
        return;
    }
    let dir = TempDir::new().unwrap();
    let (broker, grpc_addr, kafka_addr, handles) = boot_full(&dir).await;

    let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    grpc.open_topic("rc-events", 1).await.unwrap();

    let bootstrap = format!("127.0.0.1:{}", kafka_addr.port());

    // Push records via kcat -P first (we already know this works).
    let mut producer = Command::new("kcat")
        .arg("-P")
        .arg("-b").arg(&bootstrap)
        .arg("-t").arg("rc-events")
        .arg("-p").arg("0")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn kcat -P");
    let mut stdin = producer.stdin.take().unwrap();
    stdin.write_all(b"hello-from-kcat\n").await.unwrap();
    drop(stdin);
    let _ = tokio::time::timeout(Duration::from_secs(15), producer.wait_with_output())
        .await
        .expect("produce timeout")
        .unwrap();

    // Pipe stderr to a real file so we can inspect it after timing kcat out.
    let stderr_path = std::env::temp_dir().join(format!(
        "kcat-debug-{}.log",
        std::process::id()
    ));
    let stderr_file = std::fs::File::create(&stderr_path).unwrap();
    let stdout_path = std::env::temp_dir().join(format!(
        "kcat-out-{}.log",
        std::process::id()
    ));
    let stdout_file = std::fs::File::create(&stdout_path).unwrap();

    let mut consumer = Command::new("kcat")
        .arg("-C")
        .arg("-b").arg(&bootstrap)
        .arg("-t").arg("rc-events")
        .arg("-p").arg("0")
        .arg("-o").arg("0")
        .arg("-e")
        .arg("-d").arg("fetch,protocol,topic,broker,msg")
        .stdout(stdout_file)
        .stderr(stderr_file)
        .spawn()
        .expect("spawn kcat -C");

    // Wait up to 6s, then force-kill if still running.
    let result = tokio::time::timeout(Duration::from_secs(6), consumer.wait()).await;
    let timed_out = match result {
        Ok(_) => false,
        Err(_) => {
            let _ = consumer.kill().await;
            true
        }
    };

    broker.shutdown().await;
    for h in handles {
        let _ = h.await;
    }

    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
    let stdout = std::fs::read_to_string(&stdout_path).unwrap_or_default();

    eprintln!("=== kcat -C stdout (timed_out={timed_out}) ===");
    eprintln!("{stdout}");
    eprintln!("=== kcat -C stderr (tail 3000 chars) ===");
    let start = stderr.len().saturating_sub(3000);
    eprintln!("{}", &stderr[start..]);

    if timed_out {
        panic!("kcat -C hung — see librdkafka debug log printed above");
    }
    assert!(stdout.contains("hello-from-kcat"), "didn't consume record");
}
