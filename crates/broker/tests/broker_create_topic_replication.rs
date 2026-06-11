//! End-to-end test for replicated topic creation.
//!
//! When a broker is in cluster mode, calling `Broker::open_topic_async`
//! (or going through the gRPC `OpenTopic` handler) routes a
//! `ReplicatedCommand::CreateTopic` through the installed
//! `RaftReplicator`. Every replica's apply callback then materialises
//! the topic locally via `TopicResolver::create_topic`. After the
//! call returns, every broker can serve that topic, including
//! brokers that the client never directly contacted.
//!
//! Without CreateTopic replication, a client would have to call
//! `OpenTopic` against every broker before producing — which doesn't
//! match Kafka semantics (CreateTopic via the controller propagates).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use qx_broker::{Broker, BrokerConfig, ClusterConfig};
use tempfile::TempDir;

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "auto-replicated";
const PARTITION_COUNT: u32 = 4;

struct Node {
    _dir: TempDir,
    broker: Arc<Broker>,
}

async fn bootstrap_three_brokers() -> Vec<Node> {
    let mut listeners = Vec::new();
    let mut addrs: Vec<(u64, String)> = Vec::new();
    for id in 0..NUM_BROKERS {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push((id, l.local_addr().unwrap().to_string()));
        listeners.push(l);
    }
    drop(listeners);

    let mut nodes = Vec::new();
    for id in 0..NUM_BROKERS {
        let dir = TempDir::new().unwrap();
        let cfg = BrokerConfig {
            cluster: Some(ClusterConfig {
                node_id: id,
                voters: addrs.clone(),
                raft_tls: None,
                client_addrs: Vec::new(),
            }),
            ..Default::default()
        };
        let broker = Broker::open_with_config(dir.path(), cfg);
        nodes.push(Node { _dir: dir, broker });
    }

    let futures: Vec<_> = nodes
        .iter()
        .map(|n| {
            let b = n.broker.clone();
            async move { b.start_cluster().await }
        })
        .collect();
    futures::future::try_join_all(futures)
        .await
        .expect("start_cluster on all brokers");

    nodes
}

async fn wait_until_topic_known_on(broker: &Broker, name: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if broker.lookup_topic(name).is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("topic '{name}' never reached this broker");
}

async fn find_leader_idx(nodes: &[Node]) -> usize {
    // The probe send requires the topic to exist. Use a per-iteration
    // CreateTopic-or-Produce probe: the first broker whose
    // `open_topic_async` returns Ok is the leader (or can reach it via
    // Raft).
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for (idx, n) in nodes.iter().enumerate() {
            // open_topic_async is idempotent — the cluster-apply path
            // ensures every broker creates the topic. It only
            // succeeds when the cluster has a leader.
            if n.broker.open_topic_async("__probe__", 1).await.is_ok() {
                return idx;
            }
        }
        if Instant::now() >= deadline {
            panic!("no broker accepted CreateTopic probe within 15s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn open_topic_on_one_broker_replicates_to_every_broker() {
    let nodes = bootstrap_three_brokers().await;
    let leader_idx = find_leader_idx(&nodes).await;

    // Sanity: the probe-topic from `find_leader_idx` should already
    // be visible on every broker.
    for n in &nodes {
        wait_until_topic_known_on(&n.broker, "__probe__", Duration::from_secs(5)).await;
    }

    // Now create the real test topic on the leader only. Every
    // follower's apply callback must materialise it locally.
    let count = nodes[leader_idx]
        .broker
        .open_topic_async(TOPIC, PARTITION_COUNT)
        .await
        .expect("open_topic_async on leader");
    assert_eq!(count, PARTITION_COUNT);

    for n in &nodes {
        wait_until_topic_known_on(&n.broker, TOPIC, Duration::from_secs(5)).await;
        let t = n.broker.lookup_topic(TOPIC).expect("topic visible");
        assert_eq!(t.partition_count(), PARTITION_COUNT);
    }

    // Producing on the leader now replicates a Produce envelope.
    // Every broker — including those that never received OpenTopic —
    // observes the record on their LOCAL Topic.
    nodes[leader_idx]
        .broker
        .produce_for_test(TOPIC, 0, vec![Bytes::from_static(b"cross-replicated")])
        .await
        .expect("produce_for_test on leader");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut converged = true;
        for n in &nodes {
            let t = n.broker.lookup_topic(TOPIC).expect("topic still open");
            if !matches!(t.read_record(0, 0), Ok(Some(_))) {
                converged = false;
                break;
            }
        }
        if converged {
            break;
        }
        if Instant::now() >= deadline {
            panic!("replicated produce never reached every broker");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for (idx, n) in nodes.iter().enumerate() {
        let t = n.broker.lookup_topic(TOPIC).expect("topic open");
        let payload = t
            .read_record(0, 0)
            .expect("read_record")
            .unwrap_or_else(|| panic!("broker {idx} missing replicated record"));
        assert_eq!(&payload[..], b"cross-replicated");
    }
}
