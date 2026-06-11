//! End-to-end gRPC tests: spin a real tonic server on an ephemeral port,
//! connect a real client, exercise the wire protocol.

use std::sync::Arc;

use qx_broker::{Broker, BrokerConfig, Client, TlsConfig};
use tempfile::TempDir;

async fn start_server() -> (TempDir, Arc<Broker>, String) {
    let tmp = TempDir::new().unwrap();
    let broker = Broker::open(tmp.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bclone = broker.clone();
    tokio::spawn(async move {
        bclone.serve_with_listener(listener).await.ok();
    });
    (tmp, broker, format!("http://{addr}"))
}

#[tokio::test]
async fn client_can_connect() {
    let (_tmp, _broker, url) = start_server().await;
    let _client = Client::connect(url).await.unwrap();
}

#[tokio::test]
async fn open_topic_returns_configured_partition_count() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let count = client.open_topic("my-topic", 4).await.unwrap();
    assert_eq!(count, 4);
}

#[tokio::test]
async fn send_then_receive_over_grpc() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    let (partition, _id) = client.send("t", b"key", b"hello").await.unwrap();
    let batch = client.receive("t", 10).await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(&batch[0].payload[..], b"hello");
    assert_eq!(batch[0].partition, partition);
}

#[tokio::test]
async fn ack_removes_message_over_grpc() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    client.send("t", b"key", b"hello").await.unwrap();
    let batch = client.receive("t", 10).await.unwrap();
    let msg = batch.into_iter().next().unwrap();
    client.ack("t", msg.receipt).await.unwrap();
    let stats = client.stats("t").await.unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.inflight, 0);
}

#[tokio::test]
async fn nack_returns_message_over_grpc() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    client.send("t", b"key", b"hello").await.unwrap();
    let batch = client.receive("t", 10).await.unwrap();
    let msg = batch.into_iter().next().unwrap();
    let partition = msg.partition;
    client.nack("t", msg.receipt).await.unwrap();
    let batch = client.receive("t", 10).await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].attempts, 2);
    assert_eq!(batch[0].partition, partition);
}

#[tokio::test]
async fn send_batch_over_grpc() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    let items = vec![
        (
            bytes::Bytes::from_static(b"k1"),
            bytes::Bytes::from_static(b"v1"),
        ),
        (
            bytes::Bytes::from_static(b"k2"),
            bytes::Bytes::from_static(b"v2"),
        ),
        (
            bytes::Bytes::from_static(b"k3"),
            bytes::Bytes::from_static(b"v3"),
        ),
    ];
    let results = client.send_batch("t", items).await.unwrap();
    assert_eq!(results.len(), 3);
    let batch = client.receive("t", 10).await.unwrap();
    assert_eq!(batch.len(), 3);
}

#[tokio::test]
async fn send_to_unknown_topic_returns_not_found() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let result = client.send("nonexistent", b"k", b"v").await;
    assert!(result.is_err());
}

async fn start_server_with_token(token: &str) -> (TempDir, Arc<Broker>, String) {
    let tmp = TempDir::new().unwrap();
    let broker = Broker::open_with_config(
        tmp.path(),
        BrokerConfig {
            auth_token: Some(token.to_string()),
            ..Default::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bclone = broker.clone();
    tokio::spawn(async move {
        bclone.serve_with_listener(listener).await.ok();
    });
    (tmp, broker, format!("http://{addr}"))
}

#[tokio::test]
async fn join_group_single_member_gets_all_partitions() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    let (member_id, generation, assignment) = client.join_group("t", "g").await.unwrap();
    assert_eq!(member_id, 0);
    assert_eq!(generation, 1);
    assert_eq!(assignment, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn second_member_triggers_rebalance() {
    let (_tmp, _broker, url) = start_server().await;
    let mut a = Client::connect(url.clone()).await.unwrap();
    a.open_topic("t", 4).await.unwrap();
    let (id_a, _, before) = a.join_group("t", "g").await.unwrap();
    assert_eq!(before, vec![0, 1, 2, 3]);

    let mut b = Client::connect(url).await.unwrap();
    let (_, _, b_assignment) = b.join_group("t", "g").await.unwrap();

    // Member A's view via heartbeat should reflect the new assignment
    let (_, a_assignment) = a.heartbeat("t", "g", id_a).await.unwrap();
    assert_eq!(a_assignment.len() + b_assignment.len(), 4);
    assert!(!a_assignment.is_empty());
    assert!(!b_assignment.is_empty());
}

#[tokio::test]
async fn leave_group_reassigns_partitions() {
    let (_tmp, _broker, url) = start_server().await;
    let mut a = Client::connect(url.clone()).await.unwrap();
    a.open_topic("t", 4).await.unwrap();
    let (id_a, _, _) = a.join_group("t", "g").await.unwrap();
    let mut b = Client::connect(url).await.unwrap();
    let (id_b, _, _) = b.join_group("t", "g").await.unwrap();

    a.leave_group("t", "g", id_a).await.unwrap();
    let (_, b_after) = b.heartbeat("t", "g", id_b).await.unwrap();
    assert_eq!(b_after, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn stream_for_member_only_returns_assigned_partitions() {
    let (_tmp, _broker, url) = start_server().await;
    let mut producer = Client::connect(url.clone()).await.unwrap();
    producer.open_topic("t", 4).await.unwrap();
    // Send to every partition so all of them have data
    for p in 0..4u32 {
        producer.send_to_partition("t", p, b"x").await.ok();
    }

    let mut a = Client::connect(url.clone()).await.unwrap();
    let (id_a, _, _) = a.join_group("t", "g").await.unwrap();
    let mut b = Client::connect(url).await.unwrap();
    let (_id_b, _, _) = b.join_group("t", "g").await.unwrap();

    let (_, assignment_a) = a.heartbeat("t", "g", id_a).await.unwrap();
    let batch = a.stream_for_member("t", "g", id_a, 100).await.unwrap();
    // Every record returned must come from a partition we own
    for record in &batch {
        assert!(
            assignment_a.contains(&record.partition),
            "got partition {} but assignment is {assignment_a:?}",
            record.partition
        );
    }
}

#[tokio::test]
async fn consumer_group_streams_and_commits_offset() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();

    // Send to partition 0 specifically by reusing a key that hashes there.
    // Easier: discover the partition via send and use it.
    let (p, _) = client.send("t", b"k", b"v1").await.unwrap();
    client.send("t", b"k", b"v2").await.unwrap();
    client.send("t", b"k", b"v3").await.unwrap();

    let batch = client.stream_from_group("t", "g", p, 10).await.unwrap();
    assert_eq!(batch.len(), 3);
    let last = batch.last().unwrap().id;

    client
        .commit_group_offset("t", "g", p, last + 1)
        .await
        .unwrap();
    let off = client.get_group_offset("t", "g", p).await.unwrap();
    assert_eq!(off, Some(last + 1));

    let second = client.stream_from_group("t", "g", p, 10).await.unwrap();
    assert!(second.is_empty(), "after commit, no new messages");
}

#[tokio::test]
async fn consumer_group_offsets_are_per_group() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    let (p, _) = client.send("t", b"k", b"v1").await.unwrap();
    client.send("t", b"k", b"v2").await.unwrap();
    client.commit_group_offset("t", "alpha", p, 1).await.unwrap();

    let alpha = client.stream_from_group("t", "alpha", p, 10).await.unwrap();
    let beta = client.stream_from_group("t", "beta", p, 10).await.unwrap();
    assert_eq!(alpha.len(), 1);
    assert_eq!(beta.len(), 2);
}

#[tokio::test]
async fn workflow_create_root_and_get_returns_pending_task() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let id = client
        .create_root_task("deploy", b"plan".to_vec())
        .await
        .unwrap();
    let task = client.get_task("deploy", id).await.unwrap();
    assert_eq!(task.id, id);
    assert_eq!(task.status, 0); // Pending
    assert_eq!(&task.payload[..], b"plan");
    assert!(task.parent_id.is_none());
}

#[tokio::test]
async fn workflow_child_links_to_parent() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let root = client
        .create_root_task("deploy", b"root".to_vec())
        .await
        .unwrap();
    let child = client
        .create_child_task("deploy", root, b"step".to_vec())
        .await
        .unwrap();
    let kids = client.list_children("deploy", root).await.unwrap();
    assert_eq!(kids, vec![child]);
}

#[tokio::test]
async fn workflow_complete_and_fail_transition_status() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let a = client
        .create_root_task("wf", b"a".to_vec())
        .await
        .unwrap();
    let b = client
        .create_root_task("wf", b"b".to_vec())
        .await
        .unwrap();
    client.complete_task("wf", a, b"ok".to_vec()).await.unwrap();
    client.fail_task("wf", b, "boom".into()).await.unwrap();

    let ta = client.get_task("wf", a).await.unwrap();
    let tb = client.get_task("wf", b).await.unwrap();
    assert_eq!(ta.status, 2); // Done
    assert_eq!(&ta.result[..], b"ok");
    assert_eq!(tb.status, 3); // Failed
    assert_eq!(tb.error, "boom");
}

#[tokio::test]
async fn workflow_cancel_propagates_to_descendants() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let root = client
        .create_root_task("wf", b"root".to_vec())
        .await
        .unwrap();
    let _ = client
        .create_child_task("wf", root, b"a".to_vec())
        .await
        .unwrap();
    let _ = client
        .create_child_task("wf", root, b"b".to_vec())
        .await
        .unwrap();

    let cancelled = client.cancel_task("wf", root).await.unwrap();
    assert_eq!(cancelled, 3);
    let r = client.get_task("wf", root).await.unwrap();
    assert_eq!(r.status, 4); // Cancelled
}

#[tokio::test]
async fn workflow_get_unknown_returns_not_found() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let result = client.get_task("wf", 999).await;
    assert!(matches!(
        result,
        Err(qx_broker::BrokerError::Rpc(s)) if s.code() == tonic::Code::NotFound
    ));
}

async fn start_server_with_tls() -> (TempDir, Arc<Broker>, String, Vec<u8>) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let tmp = TempDir::new().unwrap();
    let broker = Broker::open_with_config(
        tmp.path(),
        BrokerConfig {
            tls: Some(TlsConfig {
                cert_pem: cert_pem.clone().into_bytes(),
                key_pem: key_pem.into_bytes(),
                client_ca_pem: None,
            }),
            ..Default::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bclone = broker.clone();
    tokio::spawn(async move {
        bclone.serve_with_listener(listener).await.ok();
    });
    (tmp, broker, format!("https://localhost:{}", addr.port()), cert_pem.into_bytes())
}

#[tokio::test]
async fn tls_client_can_connect_with_ca_cert() {
    let (_tmp, _broker, url, ca_pem) = start_server_with_tls().await;
    let mut client = Client::connect_tls(url, ca_pem, "localhost".into())
        .await
        .unwrap();
    let count = client.open_topic("t", 4).await.unwrap();
    assert_eq!(count, 4);
}

#[tokio::test]
async fn auth_required_when_broker_has_token() {
    let (_tmp, _broker, url) = start_server_with_token("s3cret").await;
    let mut client = Client::connect(url).await.unwrap();
    let err = client.open_topic("t", 4).await.unwrap_err();
    match err {
        qx_broker::BrokerError::Rpc(status) => {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }
        other => panic!("expected unauthenticated error, got {other:?}"),
    }
}

#[tokio::test]
async fn auth_passes_with_correct_token() {
    let (_tmp, _broker, url) = start_server_with_token("s3cret").await;
    let mut client = Client::connect_with_token(url, "s3cret".into()).await.unwrap();
    let count = client.open_topic("t", 4).await.unwrap();
    assert_eq!(count, 4);
}

#[tokio::test]
async fn auth_rejects_wrong_token() {
    let (_tmp, _broker, url) = start_server_with_token("s3cret").await;
    let mut client = Client::connect_with_token(url, "wrong".into()).await.unwrap();
    let err = client.open_topic("t", 4).await.unwrap_err();
    match err {
        qx_broker::BrokerError::Rpc(status) => {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }
        other => panic!("expected unauthenticated error, got {other:?}"),
    }
}

#[tokio::test]
async fn metrics_endpoint_exposes_topic_gauges() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (_tmp, broker, grpc_url) = start_server().await;
    let mut client = Client::connect(grpc_url).await.unwrap();
    client.open_topic("metrics-topic", 4).await.unwrap();
    client
        .send("metrics-topic", b"k", b"v")
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_addr = listener.local_addr().unwrap();
    let bclone = broker.clone();
    tokio::spawn(async move {
        bclone.serve_metrics_with_listener(listener).await.ok();
    });

    let mut stream = tokio::net::TcpStream::connect(metrics_addr).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap();
    let response = String::from_utf8_lossy(&response).into_owned();

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("qx_topic_pending"));
    assert!(response.contains("topic=\"metrics-topic\""));
    assert!(response.contains("qx_topic_partitions{topic=\"metrics-topic\"} 4"));
}

#[tokio::test]
async fn receive_stream_delivers_messages_as_they_arrive() {
    use std::time::Duration;

    let (_tmp, _broker, url) = start_server().await;
    let mut producer = Client::connect(url.clone()).await.unwrap();
    let mut consumer = Client::connect(url).await.unwrap();
    producer.open_topic("t", 4).await.unwrap();

    let mut stream = consumer
        .receive_stream("t", 10, Duration::from_millis(20))
        .await
        .unwrap();

    // Send after the stream is opened
    tokio::spawn(async move {
        for i in 0..3u32 {
            producer
                .send("t", format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .await
                .unwrap();
        }
    });

    let mut payloads = Vec::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .expect("stream timeout")
            .expect("stream rpc error")
            .expect("stream ended early");
        payloads.push(msg.payload);
    }
    payloads.sort();
    assert_eq!(payloads, vec![b"v0".to_vec(), b"v1".to_vec(), b"v2".to_vec()]);
}

#[tokio::test]
async fn schema_register_and_retrieve_over_grpc() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    let v1 = client
        .register_schema("orders.value", b"{\"name\":\"order_v1\"}".to_vec())
        .await
        .unwrap();
    assert_eq!(v1.version, 1);

    let v2 = client
        .register_schema("orders.value", b"{\"name\":\"order_v2\"}".to_vec())
        .await
        .unwrap();
    assert_eq!(v2.version, 2);

    let latest = client.get_latest_schema("orders.value").await.unwrap();
    assert_eq!(latest.bytes, b"{\"name\":\"order_v2\"}");

    let by_id = client.get_schema_by_id(v1.id).await.unwrap();
    assert_eq!(by_id.bytes, b"{\"name\":\"order_v1\"}");

    let versions = client.list_schema_versions("orders.value").await.unwrap();
    assert_eq!(versions, vec![1, 2]);

    let subjects = client.list_subjects().await.unwrap();
    assert_eq!(subjects, vec!["orders.value".to_string()]);
}

#[tokio::test]
async fn stats_aggregates_across_partitions() {
    let (_tmp, _broker, url) = start_server().await;
    let mut client = Client::connect(url).await.unwrap();
    client.open_topic("t", 4).await.unwrap();
    for i in 0..3 {
        client
            .send("t", format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .await
            .unwrap();
    }
    let stats = client.stats("t").await.unwrap();
    assert_eq!(stats.pending, 3);
    assert_eq!(stats.partition_count, 4);
}
