//! End-to-end tests for the openraft-backed `Cluster`. Bootstraps a real
//! 3-node openraft cluster in-process, waits for election, writes through
//! the leader, and verifies replication on the followers' state machines.

use std::time::Duration;

use std::sync::Arc;
use std::sync::Mutex;

use qx_queue::QueueConfig;
use qx_raft::{
    Cluster, GrpcCluster, IsolationLevel, PersistentCluster, PersistentReplicatedTopic,
    ReplicatedTopic,
};
use qx_storage::PartitionConfig;
use qx_topic::{Topic, TopicConfig};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

#[derive(Serialize, Deserialize, Clone)]
enum TopicCommand {
    Send { key: Vec<u8>, payload: Vec<u8> },
}

#[tokio::test]
async fn persistent_replicated_topic_survives_restart_through_a_snapshot() {
    let tmp = tempdir().unwrap();
    let partition = {
        let topic = PersistentReplicatedTopic::bootstrap(tmp.path(), 3, 4)
            .await
            .unwrap();
        let p = topic.send(b"k", b"a".to_vec()).await.unwrap();
        topic.send(b"k", b"b".to_vec()).await.unwrap();
        topic.send(b"k", b"c".to_vec()).await.unwrap();
        topic
            .wait_for_apply(p, 3, std::time::Duration::from_secs(3))
            .await
            .unwrap();

        // Force a snapshot on every replica — openraft will (potentially)
        // purge early Raft-log entries afterward. The state machine log on
        // disk should still reflect everything we sent.
        for n in 0..3u64 {
            topic.trigger_snapshot(n).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        topic.shutdown().await.unwrap();
        p
    };

    let topic = PersistentReplicatedTopic::open(tmp.path(), 3, 4)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for node in 0..3u64 {
        let recs = topic.stream(node, "g", partition, 100).unwrap();
        let payloads: Vec<_> = recs.into_iter().map(|(_, b)| b).collect();
        assert_eq!(
            payloads,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "node {node} lost state across snapshot+restart: {payloads:?}"
        );
    }
}

#[tokio::test]
async fn persistent_replicated_topic_survives_restart() {
    let tmp = tempdir().unwrap();

    // Phase 1: bootstrap, write, wait for replication, shutdown.
    let partition = {
        let topic = PersistentReplicatedTopic::bootstrap(tmp.path(), 3, 4)
            .await
            .unwrap();
        let p = topic.send(b"order-42", b"first".to_vec()).await.unwrap();
        topic.send(b"order-42", b"second".to_vec()).await.unwrap();
        topic.send(b"order-42", b"third".to_vec()).await.unwrap();
        topic
            .wait_for_apply(p, 3, std::time::Duration::from_secs(3))
            .await
            .unwrap();
        topic.shutdown().await.unwrap();
        p
    };

    // Phase 2: reopen. Existing log entries should replay through the apply
    // callback to rebuild each node's NodeTopicState before Raft restarts.
    let topic = PersistentReplicatedTopic::open(tmp.path(), 3, 4).await.unwrap();
    // Give Raft a moment to elect a leader after reopen.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for node in 0..3u64 {
        let recs = topic.stream(node, "g", partition, 100).unwrap();
        let payloads: Vec<_> = recs.into_iter().map(|(_, b)| b).collect();
        assert_eq!(
            payloads,
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
            "node {node} lost replicated state across restart"
        );
    }
}

#[tokio::test]
async fn compact_by_key_keeps_only_latest_eligible_per_key() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();
    // Same key three times — all NonTransactional. After compaction only the
    // latest survives across every replica.
    let p1 = topic.send(b"entity-1", b"v1".to_vec()).await.unwrap();
    let p2 = topic.send(b"entity-1", b"v2".to_vec()).await.unwrap();
    let p3 = topic.send(b"entity-1", b"v3".to_vec()).await.unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p2, p3);
    let p_other = topic.send(b"entity-2", b"keep".to_vec()).await.unwrap();
    let _ = p_other;
    topic
        .wait_for_apply(p1, 3, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    topic.compact_by_key(p1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for n in 0..3u64 {
        let rc = topic
            .stream_with_isolation(n, "g", p1, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        let payloads: Vec<_> = rc.into_iter().map(|(_, b)| b).collect();
        assert_eq!(payloads, vec![b"v3".to_vec()], "node {n} retained {payloads:?}");
    }
}

#[tokio::test]
async fn compact_by_key_preserves_pending_records() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();
    // Two committed records with the same key, plus a Pending one with the
    // same key. Compaction must keep the latest committed AND the pending.
    let p = topic.send(b"k", b"old".to_vec()).await.unwrap();
    topic.send(b"k", b"newer".to_vec()).await.unwrap();
    topic
        .send_txn("in-flight", b"k", b"future".to_vec())
        .await
        .unwrap();
    topic
        .wait_for_apply(p, 3, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    topic.compact_by_key(p).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // ReadCommitted: only "newer" survives (the pending stays in storage
    // but is invisible at this isolation level).
    let rc = topic
        .stream_with_isolation(0, "g", p, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    let rc_payloads: Vec<_> = rc.into_iter().map(|(_, b)| b).collect();
    assert_eq!(rc_payloads, vec![b"newer".to_vec()]);

    // ReadUncommitted also sees the pending record, proving it wasn't compacted.
    let ru = topic
        .stream_with_isolation(0, "g", p, 100, IsolationLevel::ReadUncommitted)
        .unwrap();
    let ru_payloads: Vec<_> = ru.into_iter().map(|(_, b)| b).collect();
    assert_eq!(ru_payloads, vec![b"newer".to_vec(), b"future".to_vec()]);
}

#[tokio::test]
async fn compact_before_drops_eligible_records_older_than_cutoff() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();
    let p = topic.send(b"k", b"old".to_vec()).await.unwrap();
    topic
        .wait_for_apply(p, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    // Sleep so the next record's created_nanos is strictly higher.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let cutoff_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    topic.send(b"k", b"recent".to_vec()).await.unwrap();
    topic
        .wait_for_apply(p, 2, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    topic.compact_before(p, cutoff_nanos).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for n in 0..3u64 {
        let rc = topic
            .stream_with_isolation(n, "g", p, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        let payloads: Vec<_> = rc.into_iter().map(|(_, b)| b).collect();
        assert_eq!(payloads, vec![b"recent".to_vec()], "node {n}: {payloads:?}");
    }
}

#[tokio::test]
async fn cross_partition_transaction_commits_atomically() {
    let topic = ReplicatedTopic::bootstrap(3, 4).await.unwrap();
    let p1 = topic.send_txn("txn-X", b"alpha", b"A".to_vec()).await.unwrap();
    let p2 = topic.send_txn("txn-X", b"beta", b"B".to_vec()).await.unwrap();
    assert_ne!(p1, p2, "test requires the two keys to hash to different partitions");

    topic
        .wait_for_apply(p1, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    topic
        .wait_for_apply(p2, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    // Pre-commit: ReadCommitted sees neither partition.
    let rc_p1 = topic
        .stream_with_isolation(0, "g", p1, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    let rc_p2 = topic
        .stream_with_isolation(0, "g", p2, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    assert!(rc_p1.is_empty() && rc_p2.is_empty());

    topic.commit_txn("txn-X").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Post-commit: both partitions visible to ReadCommitted, atomically.
    for n in 0..3u64 {
        let rc1 = topic
            .stream_with_isolation(n, "g", p1, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        let rc2 = topic
            .stream_with_isolation(n, "g", p2, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        assert_eq!(rc1.len(), 1, "node {n}/p{p1} should see committed");
        assert_eq!(rc2.len(), 1, "node {n}/p{p2} should see committed");
    }
}

#[tokio::test]
async fn gc_aborted_reclaims_storage() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();
    let p = topic.send_txn("t", b"k", b"goes".to_vec()).await.unwrap();
    topic
        .wait_for_apply(p, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    topic.abort_txn("t").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Each of 3 replicas has 1 aborted record → gc removes 3
    let removed = topic.gc_aborted();
    assert_eq!(removed, 3);
    for n in 0..3u64 {
        assert_eq!(topic.node_record_count(n, p).unwrap(), 0);
    }
}

#[tokio::test]
async fn producer_epoch_fences_zombie_writes() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();

    // Producer-A begins at epoch 1 and stages a write.
    topic.begin_txn("t", "producer-A", 1).await.unwrap();
    let p = topic
        .send_txn_fenced("t", "producer-A", 1, b"k", b"a".to_vec())
        .await
        .unwrap();
    topic
        .wait_for_apply(p, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    // Producer-B takes over with epoch 2. A's pending write should be aborted.
    topic.begin_txn("t", "producer-B", 2).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Producer-A's belated commit (still on epoch 1) is fenced — silently dropped.
    topic.commit_txn_fenced("t", "producer-A", 1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rc = topic
        .stream_with_isolation(0, "g", p, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    assert!(rc.is_empty(), "fenced commit must not make A's write visible");

    // Producer-B writes and commits cleanly.
    topic
        .send_txn_fenced("t", "producer-B", 2, b"k", b"b".to_vec())
        .await
        .unwrap();
    topic
        .commit_txn_fenced("t", "producer-B", 2)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let rc = topic
        .stream_with_isolation(0, "g", p, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    let payloads: Vec<_> = rc.into_iter().map(|(_, b)| b).collect();
    assert_eq!(payloads, vec![b"b".to_vec()], "only B's write should be visible");
}

#[tokio::test]
async fn transactional_send_is_visible_only_after_commit() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();

    let p = topic
        .send_txn("txn-A", b"k", b"provisional".to_vec())
        .await
        .unwrap();
    topic
        .wait_for_apply(p, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    // Before commit: ReadCommitted sees nothing; ReadUncommitted sees it.
    for n in 0..3u64 {
        let rc = topic
            .stream_with_isolation(n, "g", p, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        let ru = topic
            .stream_with_isolation(n, "g", p, 100, IsolationLevel::ReadUncommitted)
            .unwrap();
        assert!(rc.is_empty(), "node {n}: rc should be empty pre-commit");
        assert_eq!(ru.len(), 1, "node {n}: ru should see the pending record");
        assert_eq!(ru[0].1, b"provisional");
    }

    topic.commit_txn("txn-A").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // After commit: both isolation levels see it.
    for n in 0..3u64 {
        let rc = topic
            .stream_with_isolation(n, "g", p, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        assert_eq!(rc.len(), 1, "node {n}: rc should see committed record");
        assert_eq!(rc[0].1, b"provisional");
    }
}

#[tokio::test]
async fn aborted_transactional_send_is_never_visible() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();

    let p = topic
        .send_txn("txn-B", b"k", b"will-roll-back".to_vec())
        .await
        .unwrap();
    topic
        .wait_for_apply(p, 1, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    topic.abort_txn("txn-B").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for n in 0..3u64 {
        let rc = topic
            .stream_with_isolation(n, "g", p, 100, IsolationLevel::ReadCommitted)
            .unwrap();
        let ru = topic
            .stream_with_isolation(n, "g", p, 100, IsolationLevel::ReadUncommitted)
            .unwrap();
        assert!(rc.is_empty(), "node {n}: rc should not see aborted");
        assert!(ru.is_empty(), "node {n}: ru should not see aborted either");
    }
}

#[tokio::test]
async fn mixed_transactional_and_non_transactional_writes_coexist() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();

    let p = topic.send(b"k", b"non-txn".to_vec()).await.unwrap();
    topic
        .send_txn("txn-C", b"k", b"in-flight".to_vec())
        .await
        .unwrap();
    topic
        .wait_for_apply(p, 2, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    // ReadCommitted only sees the non-transactional record.
    let rc = topic
        .stream_with_isolation(0, "g", p, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    let rc_payloads: Vec<_> = rc.into_iter().map(|(_, b)| b).collect();
    assert_eq!(rc_payloads, vec![b"non-txn".to_vec()]);

    // Now commit the txn.
    topic.commit_txn("txn-C").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let rc2 = topic
        .stream_with_isolation(0, "g", p, 100, IsolationLevel::ReadCommitted)
        .unwrap();
    let rc2_payloads: Vec<_> = rc2.into_iter().map(|(_, b)| b).collect();
    assert_eq!(rc2_payloads, vec![b"non-txn".to_vec(), b"in-flight".to_vec()]);
}

#[tokio::test]
async fn replicated_topic_linearizable_read_sees_just_committed_writes() {
    let topic = ReplicatedTopic::bootstrap(3, 4).await.unwrap();

    let p = topic.send(b"k", b"v1".to_vec()).await.unwrap();
    let p2 = topic.send(b"k", b"v2".to_vec()).await.unwrap();
    assert_eq!(p, p2);

    // Linearizable read returns immediately with both records — leader has
    // committed and applied them.
    let recs = topic.stream_linearizable("g", p, 100).await.unwrap();
    let payloads: Vec<_> = recs.into_iter().map(|(_, b)| b).collect();
    assert_eq!(payloads, vec![b"v1".to_vec(), b"v2".to_vec()]);
}

#[tokio::test]
async fn replicated_topic_sends_arrive_on_all_nodes_with_consistent_ids() {
    let topic = ReplicatedTopic::bootstrap(3, 4).await.unwrap();

    let p1 = topic.send(b"user-1", b"v1".to_vec()).await.unwrap();
    let p2 = topic.send(b"user-1", b"v2".to_vec()).await.unwrap();
    assert_eq!(p1, p2, "same key must hit the same partition");

    topic
        .wait_for_apply(p1, 2, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    // Every replica observes the same (id, payload) pairs in the same partition
    let expected: Vec<(u64, Vec<u8>)> = vec![(0, b"v1".to_vec()), (1, b"v2".to_vec())];
    for node in 0..3u64 {
        let recs = topic.stream(node, "any", p1, 100).unwrap();
        assert_eq!(recs, expected, "node {node} diverged");
    }
}

#[tokio::test]
async fn replicated_topic_commit_offset_filters_subsequent_streams() {
    let topic = ReplicatedTopic::bootstrap(3, 2).await.unwrap();

    let p = topic.send(b"k", b"a".to_vec()).await.unwrap();
    topic.send(b"k", b"b".to_vec()).await.unwrap();
    topic.send(b"k", b"c".to_vec()).await.unwrap();
    topic
        .wait_for_apply(p, 3, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    topic.commit_offset("g", p, 2).await.unwrap();
    // Wait for the commit to apply on every node
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for node in 0..3u64 {
        let recs = topic.stream(node, "g", p, 100).unwrap();
        let payloads: Vec<_> = recs.into_iter().map(|(_, p)| p).collect();
        assert_eq!(payloads, vec![b"c".to_vec()]);
    }
}

#[tokio::test]
async fn replicated_topic_distributes_sends_to_all_replicas_via_raft() {
    // One Topic per node, each with a callback that decodes a `TopicCommand`
    // from the replicated entry and applies it locally. After a Raft `append`,
    // all three Topics independently observe the same send.

    let tmp_dirs: Vec<_> = (0..3).map(|_| tempdir().unwrap()).collect();
    let cfg = TopicConfig {
        partition_count: 4,
        queue: QueueConfig {
            partition: PartitionConfig::default(),
            visibility_timeout: std::time::Duration::from_secs(30),
            max_attempts: 5,
            durable_send: false,
            eventual_commits: false,
            track_message_state: true,
        },
    };
    let topics: Vec<Arc<Mutex<Topic>>> = tmp_dirs
        .iter()
        .map(|d| Arc::new(Mutex::new(Topic::open(d.path(), cfg.clone()).unwrap())))
        .collect();

    type Applier = Arc<dyn Fn(u64, &[u8]) + Send + Sync>;
    let appliers: Vec<Applier> = topics
        .iter()
        .map(|t| {
            let t = t.clone();
            let cb: Applier = Arc::new(move |_idx: u64, bytes: &[u8]| {
                let cmd: TopicCommand = serde_json::from_slice(bytes).unwrap();
                match cmd {
                    TopicCommand::Send { key, payload } => {
                        let mut topic = t.lock().unwrap();
                        let _ = topic.send(&key, &payload);
                    }
                }
            });
            cb
        })
        .collect();

    let cluster = Cluster::bootstrap_with_appliers(3, appliers).await.unwrap();

    let cmd = TopicCommand::Send {
        key: b"order-1".to_vec(),
        payload: b"buy(QX)".to_vec(),
    };
    let bytes = serde_json::to_vec(&cmd).unwrap();
    let idx = cluster.append(bytes).await.unwrap();
    cluster
        .wait_for_replication(idx, std::time::Duration::from_secs(3))
        .await
        .unwrap();

    for (i, t) in topics.iter().enumerate() {
        let mut topic = t.lock().unwrap();
        let batch = topic.receive(10).unwrap();
        assert_eq!(
            batch.len(),
            1,
            "node {i} expected one replicated message, got {}",
            batch.len()
        );
        assert_eq!(&batch[0].payload[..], b"buy(QX)");
    }
}

#[tokio::test]
async fn network_partition_isolates_a_node_while_quorum_keeps_writing() {
    let cluster = Cluster::bootstrap(3).await.unwrap();
    let leader = cluster.leader().await.unwrap();
    let isolated = (0..cluster.size() as u64)
        .find(|n| *n != leader)
        .unwrap();

    // Partition `isolated` from the other two nodes. The remaining two have
    // a quorum (2 of 3) and must keep accepting writes; the isolated node
    // will fall behind in its state machine until healed.
    for peer in 0..cluster.size() as u64 {
        if peer != isolated {
            cluster.partition(isolated, peer);
        }
    }

    let idx = cluster.append(b"during-partition".to_vec()).await.unwrap();
    let _ = idx;

    // Give the isolated node a moment to NOT receive the write.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut survivors_with_write = 0usize;
    for n in 0..cluster.size() as u64 {
        if n == isolated {
            continue;
        }
        let log = cluster.state_log(n).await.unwrap();
        if log.iter().any(|e| e == b"during-partition") {
            survivors_with_write += 1;
        }
    }
    assert_eq!(
        survivors_with_write, 2,
        "the two non-isolated nodes must have the write"
    );
    let isolated_log = cluster.state_log(isolated).await.unwrap();
    assert!(
        !isolated_log.iter().any(|e| e == b"during-partition"),
        "isolated node must NOT see the write while partitioned"
    );

    // Heal — isolated node should catch up.
    for peer in 0..cluster.size() as u64 {
        if peer != isolated {
            cluster.heal(isolated, peer);
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let log = cluster.state_log(isolated).await.unwrap();
        if log.iter().any(|e| e == b"during-partition") {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("isolated node did not catch up after heal");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn cluster_keeps_writing_after_a_follower_is_shut_down() {
    let cluster = Cluster::bootstrap(3).await.unwrap();
    let before_idx = cluster.append(b"before".to_vec()).await.unwrap();
    cluster
        .wait_for_replication(before_idx, std::time::Duration::from_secs(2))
        .await
        .unwrap();

    // Identify a follower (any node that isn't the current leader) and kill it.
    let leader = cluster.leader().await.unwrap();
    let follower = (0..cluster.size() as u64).find(|n| *n != leader).unwrap();
    cluster.shutdown_node(follower).await.unwrap();

    // With 2 live nodes (the leader + one other follower) the cluster still
    // has a quorum and must continue accepting writes.
    let after_idx = cluster.append(b"after".to_vec()).await.unwrap();
    assert!(after_idx > before_idx);

    // The remaining live nodes both have the new entry. The dead node won't.
    let mut survivors_have_post_write = 0;
    for n in 0..cluster.size() as u64 {
        if n == follower {
            continue;
        }
        let log = cluster.state_log(n).await.unwrap();
        if log.iter().any(|e| e == b"after") {
            survivors_have_post_write += 1;
        }
    }
    assert_eq!(survivors_have_post_write, 2);
}

#[tokio::test]
async fn three_node_cluster_elects_a_leader() {
    let cluster = Cluster::bootstrap(3).await.unwrap();
    let leader = cluster.leader().await;
    assert!(leader.is_some(), "expected an elected leader");
}

#[tokio::test]
async fn append_through_leader_replicates_to_followers() {
    let cluster = Cluster::bootstrap(3).await.unwrap();
    let idx = cluster.append(b"hello".to_vec()).await.unwrap();
    cluster
        .wait_for_replication(idx, Duration::from_secs(2))
        .await
        .unwrap();

    for node in 0..cluster.size() as u64 {
        let log = cluster.state_log(node).await.unwrap();
        assert!(
            log.iter().any(|e| e == b"hello"),
            "node {node} missing replicated entry: {log:?}"
        );
    }
}

#[tokio::test]
async fn grpc_cluster_replicates_writes_over_tls() {
    use qx_raft::grpc::{RaftClientTls, RaftServerTls};

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();
    let server_tls = RaftServerTls {
        cert_pem: cert_pem.clone(),
        key_pem,
    };
    let client_tls = RaftClientTls {
        ca_pem: cert_pem,
        domain: "localhost".into(),
    };

    let tmp = tempdir().unwrap();
    let cluster = GrpcCluster::bootstrap_with_tls(tmp.path(), 3, server_tls, client_tls)
        .await
        .unwrap();
    let idx = cluster.append(b"over-tls".to_vec()).await.unwrap();
    cluster
        .wait_for_replication(idx, Duration::from_secs(10))
        .await
        .unwrap();
    for node in 0..cluster.size() as u64 {
        let log = cluster.state_log(node).await.unwrap();
        assert!(log.iter().any(|e| e == b"over-tls"), "node {node}: {log:?}");
    }
}

#[tokio::test]
async fn grpc_cluster_replicates_writes_over_real_tonic() {
    let tmp = tempdir().unwrap();
    let cluster = GrpcCluster::bootstrap(tmp.path(), 3).await.unwrap();
    let idx = cluster.append(b"over-grpc".to_vec()).await.unwrap();
    cluster
        .wait_for_replication(idx, Duration::from_secs(10))
        .await
        .unwrap();
    for node in 0..cluster.size() as u64 {
        let log = cluster.state_log(node).await.unwrap();
        assert!(
            log.iter().any(|e| e == b"over-grpc"),
            "node {node} missing entry: {log:?}"
        );
    }
}

#[tokio::test]
async fn persistent_cluster_replicates_writes() {
    let tmp = tempdir().unwrap();
    let cluster = PersistentCluster::bootstrap(tmp.path(), 3).await.unwrap();
    let idx = cluster.append(b"persistent".to_vec()).await.unwrap();
    cluster
        .wait_for_replication(idx, Duration::from_secs(3))
        .await
        .unwrap();
    for node in 0..cluster.size() as u64 {
        let log = cluster.state_log(node).await.unwrap();
        assert!(log.iter().any(|e| e == b"persistent"));
    }
}

#[tokio::test]
async fn persistent_cluster_state_survives_restart() {
    let tmp = tempdir().unwrap();
    {
        let cluster = PersistentCluster::bootstrap(tmp.path(), 3).await.unwrap();
        let idx = cluster.append(b"before-restart".to_vec()).await.unwrap();
        cluster
            .wait_for_replication(idx, Duration::from_secs(3))
            .await
            .unwrap();
        cluster.shutdown().await.unwrap();
    }

    let cluster = PersistentCluster::open(tmp.path(), 3).await.unwrap();
    // Each node's state machine should already contain the pre-restart entry
    // (no need to wait for a new leader election to verify replication).
    for node in 0..cluster.size() as u64 {
        let log = cluster.state_log(node).await.unwrap();
        assert!(
            log.iter().any(|e| e == b"before-restart"),
            "node {node} lost replicated entry across restart: {log:?}"
        );
    }
}

#[tokio::test]
async fn multiple_appends_preserve_order_across_followers() {
    let cluster = Cluster::bootstrap(3).await.unwrap();
    let last = cluster.append(b"a".to_vec()).await.unwrap();
    cluster.append(b"b".to_vec()).await.unwrap();
    let final_idx = cluster.append(b"c".to_vec()).await.unwrap();
    let _ = last;

    cluster
        .wait_for_replication(final_idx, Duration::from_secs(2))
        .await
        .unwrap();

    for node in 0..cluster.size() as u64 {
        let log = cluster.state_log(node).await.unwrap();
        assert_eq!(log[0], b"a");
        assert_eq!(log[1], b"b");
        assert_eq!(log[2], b"c");
    }
}
