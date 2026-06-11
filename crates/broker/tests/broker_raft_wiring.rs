//! End-to-end test: a Produce that lands on the broker's `produce_writer`
//! pipeline is routed through the installed [`Replicator`]. With a
//! [`RaftReplicator`] hooked up to a 3-node in-process [`qx_raft::Cluster`],
//! every broker's local `Topic` independently observes the same record at
//! the same offset.
//!
//! This is the wiring proof that the standalone replicator tests in
//! [`raft_replication.rs`](raft_replication.rs) can't give on their own:
//! it exercises `Broker::produce_writer` → `produce_writer::run` →
//! `Replicator::replicate_produce` end to end.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use qx_broker::replication::{RaftReplicator, fresh_pending_map};
use qx_broker::{Broker, BrokerConfig};
use qx_raft::{ApplyCallback, Cluster, NodeId};
use tempfile::TempDir;

const NUM_BROKERS: u32 = 3;
const TOPIC: &str = "orders";
const PARTITION_COUNT: u32 = 4;

struct BrokerHandle {
    _dir: TempDir,
    broker: Arc<Broker>,
}

async fn bootstrap_three_broker_cluster()
-> (Vec<BrokerHandle>, Arc<Cluster>, Vec<Arc<RaftReplicator>>) {
    let brokers: Vec<BrokerHandle> = (0..NUM_BROKERS)
        .map(|_| {
            let dir = TempDir::new().expect("tempdir");
            let broker = Broker::open_with_config(dir.path(), BrokerConfig::default());
            broker
                .open_topic_blocking(TOPIC, PARTITION_COUNT)
                .expect("open topic on broker");
            BrokerHandle { _dir: dir, broker }
        })
        .collect();

    // Collect each broker's Topic Arc — these are what each node's
    // Raft applier writes into.
    let topics: Vec<_> = brokers
        .iter()
        .map(|h| h.broker.lookup_topic(TOPIC).expect("topic opened"))
        .collect();

    // Per-node pending map: the applier on node N signals the
    // replicator's pending if the entry's proposer_node_id == N.
    let pendings: Vec<_> = (0..NUM_BROKERS).map(|_| fresh_pending_map()).collect();
    let appliers: Vec<ApplyCallback> = topics
        .iter()
        .enumerate()
        .map(|(i, t)| RaftReplicator::applier_for(i as NodeId, t.clone(), pendings[i].clone()))
        .collect();

    let cluster = Arc::new(
        Cluster::bootstrap_with_appliers(NUM_BROKERS, appliers)
            .await
            .expect("bootstrap cluster"),
    );

    // Per-broker replicator → install. Returns Arc so the test holds a
    // strong reference for inspection if needed.
    let replicators: Vec<Arc<RaftReplicator>> = brokers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let r = Arc::new(RaftReplicator::with_pending(
                cluster.clone(),
                i as NodeId,
                pendings[i].clone(),
            ));
            h.broker
                .install_replicator(r.clone())
                .expect("install_replicator");
            r
        })
        .collect();

    (brokers, cluster, replicators)
}

async fn wait_for_apply_on_all(cluster: &Cluster, expected_len: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_ok = true;
        for n in 0..cluster.size() as NodeId {
            if cluster.state_len(n).await.unwrap_or(0) < expected_len {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            return;
        }
        if Instant::now() >= deadline {
            panic!("apply did not reach all replicas in {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn produce_for_test_routes_through_raft_to_every_broker() {
    let (brokers, cluster, _r) = bootstrap_three_broker_cluster().await;

    // Produce via broker[0] — which proxies through its installed
    // RaftReplicator, which appends to Raft, which fires the apply
    // callback on every node, which calls `topic.send_batch_to(...)`
    // on every broker's local Topic.
    let payloads = vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")];
    let ids = brokers[0]
        .broker
        .produce_for_test(TOPIC, 1, payloads.clone())
        .await
        .expect("produce_for_test");
    assert_eq!(ids, vec![0u64, 1]);

    wait_for_apply_on_all(&cluster, 1, Duration::from_secs(5)).await;

    // Read back via every broker's own local Topic. The Replicator's
    // wiring is end-to-end ONLY if all three converge.
    for (idx, h) in brokers.iter().enumerate() {
        let t = h.broker.lookup_topic(TOPIC).expect("topic still open");
        for (offset, expected) in payloads.iter().enumerate() {
            let actual = t
                .read_record(1, offset as u64)
                .expect("read_record")
                .unwrap_or_else(|| {
                    panic!("broker {idx} missing offset {offset} on partition 1")
                });
            assert_eq!(&actual[..], &expected[..], "broker {idx} payload diverged");
        }
    }
}

#[tokio::test]
async fn produce_for_test_survives_raft_leader_shutdown() {
    let (brokers, cluster, _r) = bootstrap_three_broker_cluster().await;

    let payloads = vec![Bytes::from_static(b"durable")];
    brokers[0]
        .broker
        .produce_for_test(TOPIC, 0, payloads.clone())
        .await
        .expect("produce_for_test");
    wait_for_apply_on_all(&cluster, 1, Duration::from_secs(5)).await;

    let leader = cluster.leader().await.expect("leader elected");
    cluster.shutdown_node(leader).await.expect("shutdown");

    // Surviving brokers (any non-leader index) must still serve the record.
    let mut survivors = 0;
    for (idx, h) in brokers.iter().enumerate() {
        if idx as NodeId == leader {
            continue;
        }
        let t = h.broker.lookup_topic(TOPIC).expect("topic still open");
        let actual = t
            .read_record(0, 0)
            .expect("read_record")
            .expect("record present");
        assert_eq!(&actual[..], &payloads[0][..]);
        survivors += 1;
    }
    assert_eq!(survivors, (NUM_BROKERS - 1) as usize);
}
