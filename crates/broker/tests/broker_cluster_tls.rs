//! Multi-broker cluster bootstrap with TLS on the inter-broker Raft
//! transport.
//!
//! Every broker is configured with `ClusterConfig::raft_tls: Some(...)`.
//! `Broker::start_cluster` reads that, converts it to
//! `(RaftServerTls, RaftClientTls)`, and passes it to
//! `GrpcCluster::start_node`. Inter-broker AppendEntries / Vote RPCs
//! then ride tonic-rustls instead of plaintext http.
//!
//! Without TLS-correct cert chain handling the cluster never elects a
//! leader (peer dials fail) — so the test producing a record on the
//! leader and seeing it on every broker is itself the TLS validation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use qx_broker::{Broker, BrokerConfig, ClusterConfig, TlsConfig};
use tempfile::TempDir;

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "ledger";
const PARTITION_COUNT: u32 = 4;

struct Node {
    _dir: TempDir,
    broker: Arc<Broker>,
}

async fn bootstrap_tls_cluster() -> Vec<Node> {
    // One self-signed cert covers every broker — the simplest setup
    // for a homogeneous internal mesh. Real deployments would issue
    // per-broker certs via a shared CA; the broker code accepts any
    // PEM-encoded CA bundle via `TlsConfig::client_ca_pem`.
    let cert = rcgen::generate_simple_self_signed(vec![
        "127.0.0.1".to_string(),
        "localhost".to_string(),
    ])
    .expect("self-signed cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();

    // Reserve a port per broker first so every voter knows every peer's
    // address before the cluster spins up.
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
                raft_tls: Some(TlsConfig {
                    cert_pem: cert_pem.clone(),
                    key_pem: key_pem.clone(),
                    // self-signed → the same cert serves as our CA.
                    client_ca_pem: Some(cert_pem.clone()),
                }),
                client_addrs: Vec::new(),
            }),
            ..Default::default()
        };
        let broker = Broker::open_with_config(dir.path(), cfg);
        broker.open_topic_blocking(TOPIC, PARTITION_COUNT).unwrap();
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
        .expect("start_cluster on all brokers (TLS)");

    // The TLS handshake adds latency to inter-broker RPCs; give the
    // election a bit more wall-clock budget than the plaintext test.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for n in &nodes {
            let probe = n
                .broker
                .produce_for_test(TOPIC, 0, vec![Bytes::from_static(b"election-probe")])
                .await;
            if probe.is_ok() {
                return nodes;
            }
        }
        if Instant::now() >= deadline {
            panic!("no leader elected over TLS within 20s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_bootstrap_over_tls_replicates_end_to_end() {
    let nodes = bootstrap_tls_cluster().await;

    // Find the leader by probing.
    let mut leader_idx = None;
    for (idx, n) in nodes.iter().enumerate() {
        let probe = n
            .broker
            .produce_for_test(TOPIC, 1, vec![Bytes::from_static(b"lead-probe")])
            .await;
        if probe.is_ok() {
            leader_idx = Some(idx);
            break;
        }
    }
    let leader_idx = leader_idx.expect("found leader via probe");

    let payloads = vec![Bytes::from_static(b"tls-replicated")];
    let ids = nodes[leader_idx]
        .broker
        .produce_for_test(TOPIC, 2, payloads.clone())
        .await
        .expect("produce on leader over TLS");
    assert_eq!(ids, vec![0u64]);

    // Wait until the entry reaches every replica's local Topic.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut converged = true;
        for n in &nodes {
            let t = n.broker.lookup_topic(TOPIC).expect("topic");
            if !matches!(t.read_record(2, 0), Ok(Some(_))) {
                converged = false;
                break;
            }
        }
        if converged {
            break;
        }
        if Instant::now() >= deadline {
            panic!("TLS replication did not converge");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for (idx, n) in nodes.iter().enumerate() {
        let t = n.broker.lookup_topic(TOPIC).expect("topic");
        let actual = t
            .read_record(2, 0)
            .expect("read_record")
            .unwrap_or_else(|| panic!("broker {idx} missing TLS record"));
        assert_eq!(&actual[..], &payloads[0][..]);
    }
}
