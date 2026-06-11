//! Multi-broker integration test using the real gRPC Raft transport.
//!
//! Three brokers run in the same process but each owns:
//!   - its own data directory (independent `Topic` state),
//!   - its own gRPC listener for inter-broker Raft RPCs,
//!   - its own `RaftReplicator` wired to a single-node `GrpcCluster`
//!     view that knows the addresses of every peer.
//!
//! Bootstrap calls `GrpcCluster::initialize_membership(3)` on broker 0
//! after every node has started its Raft service. Openraft elects a
//! leader, the test then locates that leader, drives a `Produce`
//! through it, and verifies every broker's `Topic` converges.
//!
//! This is the proof that the broker → Replicator → GrpcCluster path
//! works without `Cluster::bootstrap_with_appliers`'s in-process
//! shortcut. Each broker actually talks to its peers over tonic.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use qx_broker::replication::{RaftReplicator, fresh_pending_map};
use qx_broker::{Broker, BrokerConfig};
use qx_raft::{ApplyCallback, GrpcCluster, NodeId, RaftCluster};
use tempfile::TempDir;

const NUM_BROKERS: u32 = 3;
const TOPIC: &str = "orders";
const PARTITION_COUNT: u32 = 4;

struct BrokerNode {
    _dir: TempDir,
    broker: Arc<Broker>,
    cluster: Arc<GrpcCluster>,
}

/// Spin up `NUM_BROKERS` brokers. Each broker:
///   1. opens its own data dir and a topic,
///   2. binds a TCP listener for Raft RPCs and records its addr,
///   3. starts a single-node `GrpcCluster` view with the full peer map,
///   4. installs the matching `RaftReplicator`.
///
/// After every broker has started, broker 0 calls
/// `initialize_membership(NUM_BROKERS)` to commit the initial cluster
/// membership entry. Openraft replicates that entry to the others and
/// elects a leader.
async fn bootstrap_three_broker_grpc_cluster() -> Vec<BrokerNode> {
    // Step 1: open brokers + topics, bind Raft listeners.
    let mut dirs = Vec::new();
    let mut brokers = Vec::new();
    let mut listeners = Vec::new();
    let mut raft_dirs = Vec::new();
    let mut peer_addrs: HashMap<NodeId, String> = HashMap::new();

    for id in 0..NUM_BROKERS {
        let broker_dir = TempDir::new().expect("tempdir broker");
        let raft_dir = TempDir::new().expect("tempdir raft");
        let broker = Broker::open_with_config(broker_dir.path(), BrokerConfig::default());
        broker
            .open_topic_blocking(TOPIC, PARTITION_COUNT)
            .expect("open topic");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind raft listener");
        let addr = listener.local_addr().expect("local_addr");
        peer_addrs.insert(id as NodeId, format!("http://{addr}"));
        dirs.push(broker_dir);
        raft_dirs.push(raft_dir);
        brokers.push(broker);
        listeners.push(listener);
    }

    // Step 2: build appliers (one per broker, capturing that broker's
    // Topic + pending map) and start the per-broker GrpcCluster view.
    let pendings: Vec<_> = (0..NUM_BROKERS).map(|_| fresh_pending_map()).collect();
    let mut nodes: Vec<BrokerNode> = Vec::with_capacity(NUM_BROKERS as usize);

    let listeners: Vec<_> = listeners.into_iter().collect();
    let raft_dirs: Vec<_> = raft_dirs.into_iter().collect();
    let dirs: Vec<_> = dirs.into_iter().collect();

    for (idx, (((listener, raft_dir), broker_dir), broker)) in listeners
        .into_iter()
        .zip(raft_dirs.into_iter())
        .zip(dirs.into_iter())
        .zip(brokers.into_iter())
        .enumerate()
    {
        let id = idx as NodeId;
        let topic = broker.lookup_topic(TOPIC).expect("topic opened");
        let applier: ApplyCallback =
            RaftReplicator::applier_for(id, topic, pendings[idx].clone());

        // start_node fires up this broker's Raft instance and the
        // tonic server that peers will dial. Membership is NOT
        // initialized here — that happens once, on broker 0.
        let cluster = GrpcCluster::start_node(
            id,
            listener,
            peer_addrs.clone(),
            raft_dir.path(),
            Some(applier),
            None,
        )
        .await
        .expect("start_node");
        let cluster: Arc<GrpcCluster> = Arc::new(cluster);

        // Install the replicator bound to this broker's cluster view
        // + node id + matching pending map.
        let replicator = Arc::new(RaftReplicator::with_pending(
            cluster.clone() as Arc<dyn RaftCluster>,
            id,
            pendings[idx].clone(),
        ));
        broker
            .install_replicator(replicator)
            .expect("install_replicator");

        nodes.push(BrokerNode {
            _dir: broker_dir,
            broker,
            cluster,
        });
        // raft_dir must stay alive — drop it after the test finishes.
        // We leak it on purpose into the NodeBroker by holding the
        // broker dir; the raft dir is implicitly tied to the cluster.
        // Use mem::forget to keep raft_dir alive.
        std::mem::forget(raft_dir);
    }

    // Step 3: initialise membership on broker 0. The membership entry
    // propagates to the other two via Raft, an election fires, and
    // the cluster is live.
    nodes[0]
        .cluster
        .initialize_membership(NUM_BROKERS)
        .await
        .expect("initialize_membership");

    // Wait until every broker can see a leader. The leader might be
    // any of the three — openraft picks one based on election timers.
    for n in &nodes {
        n.cluster
            .wait_for_known_leader(Duration::from_secs(10))
            .await
            .expect("wait_for_known_leader");
    }

    nodes
}

async fn find_leader_node(nodes: &[BrokerNode]) -> usize {
    // Every node tracks the current leader in its metrics. They should
    // converge fast once the membership entry propagates.
    for attempt in 0..50 {
        for n in nodes {
            if let Some(l) = n.cluster.leader().await {
                return l as usize;
            }
        }
        tokio::time::sleep(Duration::from_millis(50 + attempt * 10)).await;
    }
    panic!("no leader elected within deadline");
}

async fn wait_for_apply_on_all(
    nodes: &[BrokerNode],
    partition: u32,
    expected_offset: u64,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_ok = true;
        for (idx, n) in nodes.iter().enumerate() {
            let topic = n.broker.lookup_topic(TOPIC).expect("topic still open");
            // partition_count was set during open_topic
            if topic.read_record(partition, expected_offset).is_err()
                || topic
                    .read_record(partition, expected_offset)
                    .unwrap()
                    .is_none()
            {
                all_ok = false;
                let _ = idx; // for breakpoint diagnostics
                break;
            }
        }
        if all_ok {
            return;
        }
        if Instant::now() >= deadline {
            panic!("apply did not reach all replicas in {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn produce_via_grpc_cluster_replicates_to_every_broker() {
    let nodes = bootstrap_three_broker_grpc_cluster().await;
    let leader = find_leader_node(&nodes).await;

    let payloads = vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")];
    let ids = nodes[leader]
        .broker
        .produce_for_test(TOPIC, 1, payloads.clone())
        .await
        .expect("produce_for_test");
    assert_eq!(ids, vec![0u64, 1]);

    wait_for_apply_on_all(&nodes, 1, 1, Duration::from_secs(10)).await;

    for (idx, n) in nodes.iter().enumerate() {
        let topic = n.broker.lookup_topic(TOPIC).expect("topic still open");
        for (offset, expected) in payloads.iter().enumerate() {
            let actual = topic
                .read_record(1, offset as u64)
                .expect("read_record")
                .unwrap_or_else(|| panic!("broker {idx} missing offset {offset}"));
            assert_eq!(&actual[..], &expected[..]);
        }
    }
}

#[tokio::test]
async fn produce_via_grpc_cluster_survives_leader_shutdown() {
    let nodes = bootstrap_three_broker_grpc_cluster().await;
    let leader = find_leader_node(&nodes).await;

    let payload = Bytes::from_static(b"durable");
    nodes[leader]
        .broker
        .produce_for_test(TOPIC, 0, vec![payload.clone()])
        .await
        .expect("produce_for_test");
    wait_for_apply_on_all(&nodes, 0, 0, Duration::from_secs(10)).await;

    // Shut down the leader's Raft node. Use a tiny scope so the
    // result drops before we move on; openraft handles tracking
    // through metrics.
    nodes[leader]
        .cluster
        .shutdown_node(leader as NodeId)
        .await
        .expect("shutdown_node");

    // Survivors still serve the record from their own local Topic.
    let mut survivors = 0;
    for (idx, n) in nodes.iter().enumerate() {
        if idx == leader {
            continue;
        }
        let topic = n.broker.lookup_topic(TOPIC).expect("topic still open");
        let actual = topic
            .read_record(0, 0)
            .expect("read_record")
            .expect("payload present");
        assert_eq!(&actual[..], &payload[..]);
        survivors += 1;
    }
    assert_eq!(survivors, (NUM_BROKERS - 1) as usize);
}
