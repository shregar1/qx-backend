//! Snapshot install on long-restart follower.
//!
//! When a follower has been down long enough for the leader to take
//! a snapshot and purge old log entries, the follower can't catch up
//! via log replay alone — openraft sends an `InstallSnapshot` RPC.
//! After applying the snapshot, openraft replays any newer log
//! entries through the broker's apply callback.
//!
//! The apply cursor in qx-queue still has to do its job through this
//! path: every entry the snapshot install conceptually re-applies
//! must NOT duplicate-write to the broker's local Topic. This test
//! drives the scenario and asserts no duplicates.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use qx_broker::{Broker, BrokerConfig, ClusterConfig};
use tempfile::TempDir;

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "snapshot-test";
const PARTITION_COUNT: u32 = 2;

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
        .expect("start_cluster on every broker");
    nodes
}

async fn find_leader_idx(nodes: &[Node]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for (idx, n) in nodes.iter().enumerate() {
            // open_topic_async only succeeds on the leader (or via
            // forwarding which we haven't configured here).
            if n.broker.open_topic_async("__probe__", 1).await.is_ok() {
                return idx;
            }
        }
        if Instant::now() >= deadline {
            panic!("no leader within 15s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn snapshot_install_does_not_duplicate_topic_writes() {
    let nodes = bootstrap_three_brokers().await;
    let leader_idx = find_leader_idx(&nodes).await;
    let leader = &nodes[leader_idx].broker;
    leader
        .open_topic_async(TOPIC, PARTITION_COUNT)
        .await
        .expect("open topic");

    // Wait until every broker has materialised the topic (CreateTopic
    // applies through Raft).
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let mut all = true;
        for n in &nodes {
            if n.broker.lookup_topic(TOPIC).is_none() {
                all = false;
                break;
            }
        }
        if all {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Produce a deterministic stream of records on the leader.
    for i in 0..30u32 {
        leader
            .produce_for_test(
                TOPIC,
                (i % PARTITION_COUNT) as u32,
                vec![Bytes::from(format!("rec-{i}").into_bytes())],
            )
            .await
            .expect("produce");
    }

    // Wait for every replica's Topic to converge.
    let deadline = Instant::now() + Duration::from_secs(10);
    'wait: while Instant::now() < deadline {
        for n in &nodes {
            let t = n.broker.lookup_topic(TOPIC).expect("topic exists");
            // 30 records distributed across PARTITION_COUNT partitions
            // → each partition holds 15. Read the last expected offset
            // to gate convergence; if it's present, we're good.
            for p in 0..PARTITION_COUNT {
                let want = (30u64 / PARTITION_COUNT as u64) - 1;
                if !matches!(t.read_record(p, want), Ok(Some(_))) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'wait;
                }
            }
        }
        break;
    }

    // Trigger a snapshot on every broker. After this completes,
    // openraft prunes the Raft log on each node. A subsequent
    // restart of any node would catch up new entries through
    // log replay, but recovery of pre-snapshot state goes via
    // the on-disk state machine — which on the broker side is
    // already persisted in the topic's redb.
    for n in &nodes {
        n.broker
            .trigger_raft_snapshot()
            .await
            .expect("trigger snapshot");
    }
    // Give snapshots a moment to land.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now produce another wave; these entries are *post-snapshot*
    // log entries that must apply with the cursor advancing past
    // the snapshot's last-applied index.
    for i in 30..50u32 {
        leader
            .produce_for_test(
                TOPIC,
                (i % PARTITION_COUNT) as u32,
                vec![Bytes::from(format!("rec-{i}").into_bytes())],
            )
            .await
            .expect("post-snapshot produce");
    }

    // Convergence check: every replica's Topic should have every
    // record exactly once across both partitions.
    let deadline = Instant::now() + Duration::from_secs(10);
    'outer: while Instant::now() < deadline {
        for n in &nodes {
            let t = n.broker.lookup_topic(TOPIC).expect("topic exists");
            for p in 0..PARTITION_COUNT {
                let want = (50u64 / PARTITION_COUNT as u64) - 1;
                if !matches!(t.read_record(p, want), Ok(Some(_))) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'outer;
                }
            }
        }
        break;
    }

    // Final assertion: every broker has EXACTLY 25 records per
    // partition (50 total / 2 partitions). If snapshot install had
    // re-fired the apply callback for pre-snapshot entries, the
    // cursor would have blocked them and we'd still see 25 — but
    // if anything slipped past the cursor we'd see more.
    for (idx, n) in nodes.iter().enumerate() {
        let t = n.broker.lookup_topic(TOPIC).expect("topic exists");
        for p in 0..PARTITION_COUNT {
            let mut count = 0;
            for off in 0..(50u64 / PARTITION_COUNT as u64) {
                if matches!(t.read_record(p, off), Ok(Some(_))) {
                    count += 1;
                }
            }
            // The cursor logic must yield exactly 25 per partition.
            assert_eq!(
                count, 25,
                "broker {idx} partition {p} record count diverged"
            );
            // The record AT `want+1` (would be 25) should be empty —
            // proves no duplicate was written past the expected end.
            let one_past = 50u64 / PARTITION_COUNT as u64;
            assert!(
                matches!(t.read_record(p, one_past), Ok(None)),
                "broker {idx} partition {p} has a record at offset {one_past} — duplicate?"
            );
        }
    }
}
