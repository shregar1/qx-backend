//! Cross-cluster mirroring test: two independent broker processes, mirror
//! moves records from A → B, consumer on B reads the mirrored stream.

use std::sync::Arc;
use std::time::Duration;

use qx_broker::{Broker, Client};
use qx_mirror::{Mirror, MirrorConfig};
use tempfile::TempDir;

async fn start_broker() -> (TempDir, Arc<Broker>, String) {
    let tmp = TempDir::new().unwrap();
    let broker = Broker::open(tmp.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = broker.clone();
    tokio::spawn(async move {
        b.serve_with_listener(listener).await.ok();
    });
    (tmp, broker, format!("http://{addr}"))
}

#[tokio::test]
async fn mirror_copies_records_from_source_to_dest() {
    let (_tmp_a, _broker_a, url_a) = start_broker().await;
    let (_tmp_b, _broker_b, url_b) = start_broker().await;

    // Producer writes to source
    let mut producer = Client::connect(url_a.clone()).await.unwrap();
    producer.open_topic("orders", 2).await.unwrap();
    for i in 0..5u32 {
        producer
            .send("orders", format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .await
            .unwrap();
    }

    // Mirror copies source → dest
    let mut mirror = Mirror::open(MirrorConfig {
        source_addr: url_a,
        source_topic: "orders".into(),
        source_partition_count: 2,
        source_group: "mirror-group".into(),
        dest_addr: url_b.clone(),
        dest_topic: "orders-mirror".into(),
        dest_partition_count: 2,
        batch: 10,
        idle_poll: Duration::from_millis(20),
    })
    .await
    .unwrap();
    let moved = mirror.pump_until(5).await.unwrap();
    assert_eq!(moved, 5);

    // Consumer reads from dest
    let mut consumer = Client::connect(url_b).await.unwrap();
    let mut all_payloads: Vec<Vec<u8>> = Vec::new();
    for p in 0..2u32 {
        let batch = consumer
            .stream_from_group("orders-mirror", "downstream", p, 100)
            .await
            .unwrap();
        for r in batch {
            all_payloads.push(r.payload.to_vec());
        }
    }
    all_payloads.sort();
    let expected: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("v{i}").into_bytes())
        .collect::<Vec<_>>()
        .into_iter()
        .map(|v| v.to_vec())
        .collect();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    assert_eq!(all_payloads, expected_sorted);
}

#[tokio::test]
async fn mirror_advances_source_offsets() {
    let (_tmp_a, _broker_a, url_a) = start_broker().await;
    let (_tmp_b, _broker_b, url_b) = start_broker().await;

    let mut producer = Client::connect(url_a.clone()).await.unwrap();
    producer.open_topic("t", 1).await.unwrap();
    producer.send("t", b"k", b"once").await.unwrap();

    let mut mirror = Mirror::open(MirrorConfig {
        source_addr: url_a.clone(),
        source_topic: "t".into(),
        source_partition_count: 1,
        source_group: "g".into(),
        dest_addr: url_b,
        dest_topic: "t".into(),
        dest_partition_count: 1,
        batch: 10,
        idle_poll: Duration::from_millis(20),
    })
    .await
    .unwrap();
    mirror.pump_until(1).await.unwrap();

    // Re-running pump should be a noop — offsets have been committed
    let zero = mirror.pump_once().await.unwrap();
    assert_eq!(zero, 0);
}
