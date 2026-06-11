//! Transparent leader forwarding.
//!
//! Three brokers, each with both `voters` (Raft inter-broker
//! addresses) AND `client_addrs` (peer-facing gRPC) configured. A
//! gRPC client connects to any broker; the broker's `send` handler
//! routes through `replicate_or_forward_send`. If the local node is
//! the Raft leader, the produce lands directly. If it's a follower,
//! the helper opens a tonic channel to the leader's client gRPC,
//! forwards the request, and returns the leader's result to the
//! original caller.
//!
//! The end-to-end signal: a client that picks the WRONG broker (a
//! non-leader) still gets a successful Produce — no manual retry,
//! no metadata gymnastics.

use std::sync::Arc;
use std::time::{Duration, Instant};

use qx_broker::{Broker, BrokerConfig, Client, ClusterConfig};
use tempfile::TempDir;

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "forwarded";

struct Node {
    _dir: TempDir,
    broker: Arc<Broker>,
    client_addr: String,
}

async fn bootstrap_forwarding_cluster() -> Vec<Node> {
    // Reserve client + raft listeners so every voter knows every peer's
    // address before any broker starts serving.
    let mut client_ports = Vec::new();
    let mut raft_addrs = Vec::new();
    let mut client_listeners = Vec::new();
    let mut client_socket_addrs = Vec::new();
    let mut raft_socket_addrs = Vec::new();
    for _ in 0..NUM_BROKERS {
        let client_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_socket_addrs.push(client_l.local_addr().unwrap());
        client_listeners.push(client_l);
        let raft_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        raft_socket_addrs.push(raft_l.local_addr().unwrap());
    }
    for (id, sa) in client_socket_addrs.iter().enumerate() {
        client_ports.push((id as u64, sa.port()));
    }
    for (id, sa) in raft_socket_addrs.iter().enumerate() {
        raft_addrs.push((id as u64, format!("127.0.0.1:{}", sa.port())));
    }

    let client_addrs: Vec<(u64, String)> = client_ports
        .iter()
        .map(|(id, p)| (*id, format!("127.0.0.1:{p}")))
        .collect();

    let mut nodes = Vec::with_capacity(NUM_BROKERS as usize);
    for id in 0..NUM_BROKERS {
        let dir = TempDir::new().unwrap();
        let cfg = BrokerConfig {
            cluster: Some(ClusterConfig {
                node_id: id,
                voters: raft_addrs.clone(),
                raft_tls: None,
                client_addrs: client_addrs.clone(),
            }),
            ..Default::default()
        };
        let broker = Broker::open_with_config(dir.path(), cfg);
        // Bring the broker's gRPC up. We hand it the listener we
        // reserved earlier — the same one whose port is in
        // `client_addrs` — so peer forwarding can dial it.
        let listener = client_listeners.remove(0);
        let bclone = broker.clone();
        tokio::spawn(async move {
            let _ = bclone.serve_with_listener(listener).await;
        });
        nodes.push(Node {
            _dir: dir,
            broker,
            client_addr: client_addrs[id as usize].1.clone(),
        });
    }

    // Bootstrap each broker's Raft node concurrently.
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

/// Loops over the brokers' gRPC clients until OpenTopic succeeds on
/// one (i.e. a leader has been elected and that one is reachable —
/// could be by forwarding too). Then waits until every other broker
/// can also `lookup_topic` locally (i.e. the apply has propagated).
async fn wait_for_topic_on_every_broker(nodes: &[Node]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut clients: Vec<Client> = Vec::new();
    for n in nodes {
        clients.push(
            Client::connect(format!("http://{}", n.client_addr))
                .await
                .expect("connect"),
        );
    }
    'create: loop {
        for c in clients.iter_mut() {
            if c.open_topic(TOPIC, 4).await.is_ok() {
                break 'create;
            }
        }
        if Instant::now() >= deadline {
            panic!("CreateTopic never accepted on any broker");
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    loop {
        let mut all = true;
        for n in nodes {
            if n.broker.lookup_topic(TOPIC).is_none() {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        if Instant::now() >= deadline {
            panic!("topic create did not propagate to every broker");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn produce_on_any_broker_lands_through_leader_forwarding() {
    let nodes = bootstrap_forwarding_cluster().await;
    wait_for_topic_on_every_broker(&nodes).await;

    // Send the SAME request through every broker's client gRPC. Even
    // brokers that aren't the leader must accept and return Ok —
    // because they forward to the leader on our behalf.
    for n in &nodes {
        let mut c = Client::connect(format!("http://{}", n.client_addr))
            .await
            .expect("connect");
        let (_partition, _id) = c
            .send(TOPIC, b"forward-key", b"forwarded-payload")
            .await
            .expect("send succeeds on every broker via forwarding");
    }

    // The leader is the only one that issues record ids, so all
    // record-ids should be sequential and every broker's local topic
    // (after replication) sees all four messages.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut converged = true;
        for n in &nodes {
            let t = n.broker.lookup_topic(TOPIC).expect("topic open");
            // Three send calls (one per broker as the entry point);
            // they all hash to the same partition because the key is
            // identical. Verify each broker has at least 3 records on
            // that partition.
            let p = t.partition_for_key(b"forward-key");
            let mut seen = 0;
            for off in 0..(NUM_BROKERS as u64) {
                if matches!(t.read_record(p, off), Ok(Some(_))) {
                    seen += 1;
                }
            }
            if seen < NUM_BROKERS as u64 {
                converged = false;
                break;
            }
        }
        if converged {
            break;
        }
        if Instant::now() >= deadline {
            panic!("forwarded messages did not converge on every broker");
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}
