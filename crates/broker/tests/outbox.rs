//! End-to-end test for the workflow → topic outbox.
//!
//! Boots a broker, opens a target topic, starts the outbox dispatcher,
//! completes a workflow task with two emits, and verifies both payloads
//! land in the target topic.

use std::time::Duration;

use qx_broker::Broker;
use tempfile::TempDir;
use tokio::net::TcpListener;

async fn boot_broker(dir: &TempDir) -> (std::sync::Arc<Broker>, std::net::SocketAddr) {
    let broker = Broker::open(dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_with_listener(listener).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    (broker, addr)
}

async fn wait_until<F>(mut f: F, deadline: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    f()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_emits_land_in_target_topic() {
    let dir = TempDir::new().unwrap();
    let (broker, grpc_addr) = boot_broker(&dir).await;
    let resumed = broker.start_background().unwrap();
    assert_eq!(resumed, 0, "fresh broker should have zero persisted triggers");

    let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    grpc.open_topic("downstream", 4).await.unwrap();

    // Create a task in workflow "wf1" via gRPC, then complete it with
    // two emits using the broker's public Rust API.
    let task_id = grpc
        .create_root_task("wf1", b"input-payload".to_vec())
        .await
        .unwrap();

    broker
        .complete_task_with_emits(
            "wf1",
            task_id,
            b"out".to_vec(),
            &[("downstream", b"emit-A"), ("downstream", b"emit-B")],
        )
        .unwrap();

    let ok = wait_until(
        || {
            let payloads = broker.topic_records("downstream", 64).unwrap_or_default();
            payloads.iter().any(|p| p == b"emit-A")
                && payloads.iter().any(|p| p == b"emit-B")
        },
        Duration::from_secs(5),
    )
    .await;
    let payloads = broker.topic_records("downstream", 64).unwrap_or_default();
    assert!(
        ok,
        "outbox didn't deliver both emits; got {:?}",
        payloads
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivered_emits_dont_resend_on_redrain() {
    let dir = TempDir::new().unwrap();
    let (broker, grpc_addr) = boot_broker(&dir).await;
    broker.start_background().unwrap();

    let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    grpc.open_topic("out", 1).await.unwrap();

    let task_id = grpc.create_root_task("wf2", b"x".to_vec()).await.unwrap();
    broker
        .complete_task_with_emits("wf2", task_id, b"done".to_vec(), &[("out", b"once")])
        .unwrap();

    // Wait for delivery.
    let _ = wait_until(
        || {
            broker
                .topic_records("out", 64)
                .map(|v| v.contains(&b"once".to_vec()))
                .unwrap_or(false)
        },
        Duration::from_secs(3),
    )
    .await;

    // Let the dispatcher run a few more cycles; it should not re-deliver.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Drain the topic once more — should still show exactly one "once".
    let payloads = broker.topic_records("out", 64).unwrap();
    let once_count = payloads.iter().filter(|p| *p == b"once").count();
    assert_eq!(
        once_count, 1,
        "outbox dispatcher should not re-deliver after marking; got {:?}",
        payloads
    );
}
