//! Production-shape multi-broker bootstrap: every broker is opened
//! with `BrokerConfig::cluster: Some(ClusterConfig)`, then calls
//! `Broker::start_cluster().await` exactly once. That single call
//! binds a Raft listener, starts a `GrpcCluster::start_node` view,
//! initialises membership on node 0, and installs a `RaftReplicator`
//! that every Produce on this broker routes through.
//!
//! This is the closest the test harness can come to actual multi-
//! process deployment without spawning subprocesses — every broker
//! runs in this one tokio runtime, but they communicate exclusively
//! over real tonic-gRPC on 127.0.0.1.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use qx_broker::{Broker, BrokerConfig, ClusterConfig};
use tempfile::TempDir;

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "ledger";
const PARTITION_COUNT: u32 = 4;

struct Node {
    _dir: TempDir,
    broker: Arc<Broker>,
}

async fn bootstrap_via_config_cluster() -> Vec<Node> {
    // Reserve a port per broker BEFORE opening anything — every
    // broker's ClusterConfig needs to list every peer's address.
    let mut listeners = Vec::new();
    let mut addrs: Vec<(u64, String)> = Vec::new();
    for id in 0..NUM_BROKERS {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push((id, l.local_addr().unwrap().to_string()));
        listeners.push(l);
    }
    // Release the reservation listeners — start_cluster() will rebind
    // each broker's address. Tiny race window before start_cluster
    // re-binds; on a quiet test machine it doesn't matter.
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
        broker.open_topic_blocking(TOPIC, PARTITION_COUNT).unwrap();
        nodes.push(Node { _dir: dir, broker });
    }

    // Start every broker's cluster bootstrap concurrently. start_cluster
    // binds a listener, spins up the Raft node, and only blocks long
    // enough for openraft to register the membership entry (on node 0)
    // or accept inbound RPCs (on followers).
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

    // Wait until any broker reports a leader. The election timeout is
    // a few hundred ms; the helper polls until it sees one.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut any_leader = None;
        for n in &nodes {
            if let Some(l) = pick_known_leader(&n.broker).await {
                any_leader = Some(l);
                break;
            }
        }
        if any_leader.is_some() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("no leader elected within 10s after start_cluster");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    nodes
}

/// Best-effort: peek at the broker's replicator to find the leader.
/// The Broker doesn't currently expose a typed cluster-leader API
/// outside `start_cluster`, but a Produce against a non-leader will
/// surface ForwardToLeader, so we just probe by attempting a tiny
/// no-op Produce.
async fn pick_known_leader(broker: &Arc<Broker>) -> Option<u64> {
    // Attempt a zero-payload Produce on partition 0. If we are the
    // leader, it succeeds and we learn we're the leader. If we are
    // a follower, openraft's ForwardToLeader bubbles up as an error
    // string carrying the leader's NodeId.
    match broker
        .produce_for_test(TOPIC, 0, vec![Bytes::from_static(&[0xff])])
        .await
    {
        Ok(_) => Some(u64::MAX), // self is leader; specific id unknown via this probe
        Err(e) => {
            // Parse "ForwardToLeader { leader_id: Some(N), ... }" out of
            // the error string. openraft 0.9 surfaces this format.
            let lid = e.find("leader_id: Some(").and_then(|i| {
                let rest = &e[i + "leader_id: Some(".len()..];
                let end = rest.find(')')?;
                rest[..end].trim().parse::<u64>().ok()
            });
            lid
        }
    }
}

async fn find_leader_idx(nodes: &[Node]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for (idx, n) in nodes.iter().enumerate() {
            // We probe by issuing a payload Produce on the broker and
            // observing whether it succeeded (self is leader) or
            // returned a ForwardToLeader.
            let probe = n
                .broker
                .produce_for_test(TOPIC, 0, vec![Bytes::from_static(b"probe")])
                .await;
            match probe {
                Ok(_) => return idx,
                Err(s) if s.contains("ForwardToLeader") => continue,
                Err(s) => panic!("unexpected probe error on broker {idx}: {s}"),
            }
        }
        if Instant::now() >= deadline {
            panic!("could not identify a leader within 10s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn broker_config_cluster_bootstraps_grpc_replication_end_to_end() {
    let nodes = bootstrap_via_config_cluster().await;

    // The "probe" already wrote one record on each pass; identify the
    // leader by which broker accepts the next Produce.
    let leader_idx = find_leader_idx(&nodes).await;

    // The probe + the find_leader pass wrote at least one record on
    // partition 0. Real produce on partition 1 (clean).
    let payloads = vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")];
    let ids = nodes[leader_idx]
        .broker
        .produce_for_test(TOPIC, 1, payloads.clone())
        .await
        .expect("produce on leader");
    assert_eq!(ids, vec![0u64, 1]);

    // Wait until every broker has applied the replicated entry.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut converged = true;
        for n in &nodes {
            let t = n.broker.lookup_topic(TOPIC).expect("topic open");
            for off in 0..(payloads.len() as u64) {
                if !matches!(t.read_record(1, off), Ok(Some(_))) {
                    converged = false;
                    break;
                }
            }
            if !converged {
                break;
            }
        }
        if converged {
            break;
        }
        if Instant::now() >= deadline {
            panic!("apply did not reach every broker");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for (idx, n) in nodes.iter().enumerate() {
        let t = n.broker.lookup_topic(TOPIC).expect("topic open");
        for (offset, expected) in payloads.iter().enumerate() {
            let actual = t
                .read_record(1, offset as u64)
                .expect("read_record")
                .unwrap_or_else(|| panic!("broker {idx} missing offset {offset}"));
            assert_eq!(&actual[..], &expected[..]);
        }
    }
}
