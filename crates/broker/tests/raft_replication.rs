//! Integration tests for [`qx_broker::replication::RaftReplicator`].
//!
//! Each test spins up an in-process 3-node Raft cluster, gives each
//! node its own on-disk `qx_topic::Topic`, then drives Produce through
//! the `Replicator` trait. The tests verify:
//!
//!   - `replicate_produce` returns the same record ids on the
//!     proposer side that the local apply assigned.
//!   - Every replica's `Topic` independently observes the same
//!     payloads at the same offsets (Raft ordering → deterministic
//!     state-machine convergence).
//!   - After the Raft leader is shut down, the previously replicated
//!     records are still readable from the surviving followers'
//!     `Topic`s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use qx_broker::replication::{
    ProduceCommand, RaftReplicator, Replicator, fresh_pending_map,
};
use qx_queue::QueueConfig;
use qx_raft::{ApplyCallback, Cluster, NodeId};
use qx_storage::PartitionConfig;
use qx_topic::{Topic, TopicConfig};
use tempfile::TempDir;

const PARTITION_COUNT: u32 = 4;
const NUM_NODES: u32 = 3;

fn make_topic(dir: &std::path::Path) -> Topic {
    Topic::open(
        dir,
        TopicConfig {
            partition_count: PARTITION_COUNT,
            queue: QueueConfig {
                partition: PartitionConfig::default(),
                visibility_timeout: Duration::from_secs(30),
                max_attempts: 5,
                durable_send: false,
                eventual_commits: true,
                track_message_state: true,
            },
        },
    )
    .expect("open topic")
}

struct ClusterHarness {
    cluster: Arc<Cluster>,
    nodes: Vec<NodeHandle>,
    /// One per node; only `proposer_node_id`'s map is wired to a
    /// `RaftReplicator`. The rest exist only because every node's
    /// applier needs *some* pending map handle — followers never
    /// signal back so theirs stays empty.
    pendings: Vec<qx_broker::replication::PendingMap>,
}

struct NodeHandle {
    _dir: TempDir,
    topic: Arc<Topic>,
}

async fn bootstrap_harness() -> ClusterHarness {
    let nodes: Vec<NodeHandle> = (0..NUM_NODES)
        .map(|_| {
            let dir = tempfile::tempdir().expect("tempdir");
            let topic = Arc::new(make_topic(dir.path()));
            NodeHandle { _dir: dir, topic }
        })
        .collect();

    let pendings: Vec<_> = (0..NUM_NODES).map(|_| fresh_pending_map()).collect();
    let appliers: Vec<ApplyCallback> = nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| {
            RaftReplicator::applier_for(
                idx as NodeId,
                node.topic.clone(),
                pendings[idx].clone(),
            )
        })
        .collect();

    let cluster = Cluster::bootstrap_with_appliers(NUM_NODES, appliers)
        .await
        .expect("bootstrap cluster");

    ClusterHarness {
        cluster: Arc::new(cluster),
        nodes,
        pendings,
    }
}

fn make_replicator(h: &ClusterHarness, proposer: NodeId) -> RaftReplicator {
    RaftReplicator::with_pending(
        h.cluster.clone(),
        proposer,
        h.pendings[proposer as usize].clone(),
    )
}

/// Wait until every node has applied at least `expected_log_len`
/// entries to its replicated state log. The cluster surfaces this as
/// `state_len`; convergence on every node implies every replica's
/// `Topic` has observed every Produce up to that point.
async fn wait_for_apply_on_all_nodes(
    cluster: &Cluster,
    expected_log_len: u64,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_caught_up = true;
        for n in 0..cluster.size() as NodeId {
            let len = cluster.state_len(n).await.expect("state_len");
            if len < expected_log_len {
                all_caught_up = false;
                break;
            }
        }
        if all_caught_up {
            return;
        }
        if Instant::now() >= deadline {
            let lengths: Vec<_> =
                futures::future::join_all((0..cluster.size() as NodeId).map(|n| async move {
                    cluster.state_len(n).await.unwrap_or(0)
                }))
                .await;
            panic!(
                "timed out waiting for all replicas to apply >= {expected_log_len} entries: {lengths:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn replicate_produce_lands_on_every_replica() {
    let h = bootstrap_harness().await;
    let proposer = 0u64;
    let replicator = make_replicator(&h, proposer);

    let cmd = ProduceCommand {
        topic: "orders".into(),
        partition: 1,
        payloads: vec![
            bytes::Bytes::from_static(b"alpha"),
            bytes::Bytes::from_static(b"beta"),
            bytes::Bytes::from_static(b"gamma"),
        ],
    };
    let ids = replicator
        .replicate_produce(h.nodes[proposer as usize].topic.clone(), cmd.clone())
        .await
        .expect("replicate_produce");
    // First produce to partition 1 on a fresh topic — ids must be 0, 1, 2.
    assert_eq!(ids, vec![0u64, 1, 2]);

    wait_for_apply_on_all_nodes(&h.cluster, 1, Duration::from_secs(5)).await;

    // Every replica's local Topic should hold the same three records
    // at the same offsets on partition 1.
    for (idx, node) in h.nodes.iter().enumerate() {
        let payloads: Vec<bytes::Bytes> = (0..3u64)
            .map(|i| {
                node.topic
                    .read_record(1, i)
                    .expect("read_record")
                    .unwrap_or_else(|| panic!("node {idx} missing offset {i}"))
            })
            .collect();
        assert_eq!(
            payloads,
            cmd.payloads,
            "node {idx} did not converge on payloads"
        );
    }
}

#[tokio::test]
async fn replicated_records_survive_leader_shutdown() {
    let h = bootstrap_harness().await;
    let proposer = 0u64;
    let replicator = make_replicator(&h, proposer);

    let payloads = vec![b"first".to_vec(), b"second".to_vec()];
    let ids = replicator
        .replicate_produce(
            h.nodes[proposer as usize].topic.clone(),
            ProduceCommand {
                topic: "orders".into(),
                partition: 0,
                payloads: payloads.iter().cloned().map(bytes::Bytes::from).collect(),
            },
        )
        .await
        .expect("replicate_produce");
    assert_eq!(ids, vec![0u64, 1]);

    wait_for_apply_on_all_nodes(&h.cluster, 1, Duration::from_secs(5)).await;

    // Pick the current leader and shut it down. The two remaining
    // nodes still have quorum and — crucially — already hold the
    // replicated records.
    let leader = h.cluster.leader().await.expect("leader elected");
    h.cluster
        .shutdown_node(leader)
        .await
        .expect("shutdown leader");

    // Verify the survivors' topics still serve the records.
    let mut survivors_with_records = 0usize;
    for (idx, node) in h.nodes.iter().enumerate() {
        if idx as NodeId == leader {
            continue;
        }
        for (offset, expected) in payloads.iter().enumerate() {
            let actual = node
                .topic
                .read_record(0, offset as u64)
                .expect("read_record")
                .unwrap_or_else(|| {
                    panic!(
                        "follower {idx} missing record at offset {offset} after leader {leader} shutdown"
                    )
                });
            assert_eq!(&actual, expected, "follower {idx} payload diverged");
        }
        survivors_with_records += 1;
    }
    assert_eq!(
        survivors_with_records,
        (NUM_NODES - 1) as usize,
        "every non-leader replica must still serve the records"
    );
}

#[tokio::test]
async fn many_replicate_produce_calls_assign_increasing_ids() {
    let h = bootstrap_harness().await;
    let replicator = make_replicator(&h, 0);

    let mut last_id = None;
    for i in 0..10u32 {
        let cmd = ProduceCommand {
            topic: "orders".into(),
            partition: 2,
            payloads: vec![bytes::Bytes::from(format!("msg-{i}").into_bytes())],
        };
        let ids = replicator
            .replicate_produce(h.nodes[0].topic.clone(), cmd)
            .await
            .expect("replicate_produce");
        assert_eq!(ids.len(), 1);
        if let Some(prev) = last_id {
            assert!(ids[0] > prev, "ids must increase monotonically");
        }
        last_id = Some(ids[0]);
    }
    assert_eq!(last_id, Some(9u64));

    wait_for_apply_on_all_nodes(&h.cluster, 10, Duration::from_secs(5)).await;

    // All ten records reach every replica in order.
    for (idx, node) in h.nodes.iter().enumerate() {
        for i in 0..10u64 {
            let payload = node
                .topic
                .read_record(2, i)
                .unwrap()
                .unwrap_or_else(|| panic!("node {idx} missing offset {i}"));
            assert_eq!(payload, format!("msg-{i}").into_bytes());
        }
    }
}
