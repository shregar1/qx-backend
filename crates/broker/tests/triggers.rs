//! End-to-end tests for topic-to-workflow triggers, including a
//! broker-restart test that exercises the durable offset story now that
//! `Broker::shutdown` cleanly releases background tasks.

use std::time::Duration;

use qx_broker::{Broker, TriggerConfig};
use tempfile::TempDir;
use tokio::net::TcpListener;

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

async fn boot_broker(
    dir: &TempDir,
) -> (
    std::sync::Arc<Broker>,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let broker = Broker::open(dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let grpc = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_with_listener(listener).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    (broker, addr, grpc)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_creates_one_workflow_task_per_record() {
    let dir = TempDir::new().unwrap();
    let (broker, grpc_addr, grpc_handle) = boot_broker(&dir).await;

    let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    grpc.open_topic("orders", 1).await.unwrap();

    let mut cfg = TriggerConfig::new("orders-to-process", "orders", "process_order");
    cfg.poll_interval_ms = 25;
    broker.register_trigger(cfg).unwrap();

    for payload in [b"o1".as_ref(), b"o2", b"o3"] {
        grpc.send("orders", b"k", payload).await.unwrap();
    }

    let ok = wait_until(
        || {
            broker
                .workflow_payloads("process_order", 1024)
                .map(|p| p.len() == 3)
                .unwrap_or(false)
        },
        Duration::from_secs(3),
    )
    .await;
    let payloads = broker.workflow_payloads("process_order", 1024).unwrap();
    assert!(ok, "expected 3 tasks, found {}", payloads.len());

    assert!(payloads.contains(&b"o1".to_vec()));
    assert!(payloads.contains(&b"o2".to_vec()));
    assert!(payloads.contains(&b"o3".to_vec()));

    // list_triggers should round-trip what we registered.
    let triggers = broker.list_triggers().unwrap();
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0].name, "orders-to-process");
    assert_eq!(triggers[0].topic, "orders");
    assert_eq!(triggers[0].workflow, "process_order");

    broker.shutdown().await;
    let _ = grpc_handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_offsets_survive_broker_restart() {
    let dir = TempDir::new().unwrap();

    // ---- First boot: produce 2 records, let trigger process them ----
    {
        let (broker, grpc_addr, grpc_handle) = boot_broker(&dir).await;
        broker.start_background().unwrap();
        let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
            .await
            .unwrap();
        grpc.open_topic("orders", 1).await.unwrap();

        let mut cfg = TriggerConfig::new("t1", "orders", "wf1");
        cfg.poll_interval_ms = 25;
        broker.register_trigger(cfg).unwrap();

        grpc.send("orders", b"k", b"a").await.unwrap();
        grpc.send("orders", b"k", b"b").await.unwrap();

        let ok = wait_until(
            || {
                broker
                    .workflow_payloads("wf1", 1024)
                    .map(|p| p.len() == 2)
                    .unwrap_or(false)
            },
            Duration::from_secs(3),
        )
        .await;
        assert!(ok, "expected 2 tasks after first boot");

        broker.shutdown().await;
        drop(grpc);
        drop(broker);
        let _ = grpc_handle.await;
    }

    // ---- Second boot: resume triggers, send 1 more, verify no replay ----
    {
        let (broker, grpc_addr, grpc_handle) = boot_broker(&dir).await;
        let resumed = broker.start_background().unwrap();
        assert_eq!(resumed, 1, "trigger should be persisted across restart");

        let mut grpc = qx_broker::Client::connect(format!("http://{grpc_addr}"))
            .await
            .unwrap();
        grpc.open_topic("orders", 1).await.unwrap();
        grpc.send("orders", b"k", b"c").await.unwrap();

        let ok = wait_until(
            || {
                broker
                    .workflow_payloads("wf1", 1024)
                    .map(|p| p.len() == 3)
                    .unwrap_or(false)
            },
            Duration::from_secs(3),
        )
        .await;
        let payloads = broker.workflow_payloads("wf1", 1024).unwrap();
        assert!(
            ok,
            "expected exactly 3 tasks (no replay of a/b), found {}: {:?}",
            payloads.len(),
            payloads
        );

        broker.shutdown().await;
        let _ = grpc_handle.await;
    }
}

