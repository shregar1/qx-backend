//! Multi-process integration test: spawn three `qxd` binaries with
//! cluster TOML configs, wait for them to form a Raft cluster, then
//! drive a Produce through the leader's gRPC and verify the record
//! replicates to a non-leader's local Topic.
//!
//! This is the closest the test suite can come to a real production
//! deployment without a multi-host orchestrator: each broker is a
//! separate OS process, they speak gRPC to each other for both
//! client-facing traffic and inter-broker Raft, and configuration
//! flows through the documented TOML file format.

use std::process::Stdio;
use std::time::Duration;

use qx_broker::Client;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::time::{Instant, sleep};

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "ledger";

#[tokio::test]
async fn three_qxd_subprocesses_form_a_cluster_and_replicate_a_produce() {
    let tmp = TempDir::new().unwrap();

    // Reserve six ports up front: three for client gRPC, three for
    // the inter-broker Raft transport. We HOLD the listeners until
    // immediately before each qxd is spawned — under workspace-
    // parallel `cargo test`, releasing the port first lets another
    // test grab it in the window before the child binds. Keeping the
    // listener bound until the spawn site narrows the race to a few
    // microseconds and the `bind_with_retry` loop on the qxd side
    // closes the remaining gap.
    let mut client_ports = Vec::new();
    let mut raft_ports = Vec::new();
    let mut reservations: Vec<tokio::net::TcpListener> = Vec::new();
    for _ in 0..NUM_BROKERS {
        let p1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_ports.push(p1.local_addr().unwrap().port());
        raft_ports.push(p2.local_addr().unwrap().port());
        reservations.push(p1);
        reservations.push(p2);
    }
    let voters_block = (0..NUM_BROKERS)
        .map(|i| {
            format!(
                "  [[cluster.voters]]\n  node_id = {i}\n  addr = \"127.0.0.1:{}\"\n",
                raft_ports[i as usize]
            )
        })
        .collect::<Vec<_>>()
        .join("");

    let mut children = Vec::new();
    let mut config_paths = Vec::new();
    for id in 0..NUM_BROKERS {
        let data_dir = tmp.path().join(format!("data-{id}"));
        std::fs::create_dir_all(&data_dir).unwrap();
        let config_path = tmp.path().join(format!("qxd-{id}.toml"));
        let cfg = format!(
            r#"[server]
addr = "127.0.0.1:{client_port}"
data_dir = "{data_dir}"

[cluster]
node_id = {id}

{voters}
"#,
            client_port = client_ports[id as usize],
            data_dir = data_dir.display(),
            voters = voters_block,
        );
        write_file(&config_path, cfg.as_bytes()).await;
        config_paths.push(config_path);
    }

    // Release the port reservations now and immediately spawn the
    // children that will rebind them.
    drop(reservations);
    for config_path in &config_paths {
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_qxd"))
            .arg("--config")
            .arg(config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn qxd");
        children.push(child);
    }

    // Wait until every broker is accepting client gRPC connections.
    for port in &client_ports {
        wait_until_port_open(*port, Duration::from_secs(15)).await;
    }

    // CreateTopic now replicates through Raft. A client that hits a
    // follower with OpenTopic gets a ForwardToLeader error in the
    // status string; clients retry against another broker (the same
    // pattern Kafka clients use). Probe each in turn until one
    // succeeds — by then the topic exists on every broker.
    let mut clients: Vec<Client> = Vec::new();
    for port in &client_ports {
        let url = format!("http://127.0.0.1:{port}");
        let c = Client::connect(url).await.expect("client connect");
        clients.push(c);
    }
    let create_deadline = Instant::now() + Duration::from_secs(20);
    let mut created = false;
    while !created && Instant::now() < create_deadline {
        for c in clients.iter_mut() {
            if let Ok(count) = c.open_topic(TOPIC, 4).await {
                assert_eq!(count, 4);
                created = true;
                break;
            }
        }
        if !created {
            sleep(Duration::from_millis(200)).await;
        }
    }
    assert!(
        created,
        "OpenTopic failed on every broker within the deadline (no leader elected?)"
    );

    // Probe to find which broker openraft elected leader. Send returns
    // Ok on the leader and a tonic Status carrying ForwardToLeader on
    // followers — keep probing until one succeeds.
    let leader_idx = {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let mut found = None;
            for (idx, c) in clients.iter_mut().enumerate() {
                if c.send(TOPIC, b"election-probe", b"election-probe")
                    .await
                    .is_ok()
                {
                    found = Some(idx);
                    break;
                }
            }
            if let Some(idx) = found {
                break idx;
            }
            if Instant::now() >= deadline {
                panic!("no leader elected over multi-process gRPC within 20s");
            }
            sleep(Duration::from_millis(150)).await;
        }
    };

    // Send a known payload through the leader. The Replicator
    // routes it through Raft; every broker's local Topic must
    // observe the same record.
    let (_partition, _id) = clients[leader_idx]
        .send(TOPIC, b"target-key", b"replicated-value")
        .await
        .expect("send on leader");

    // Verify replication landed on a non-leader. Read via receive
    // (which uses the broker's LOCAL Topic), so a positive result is
    // proof that the entry applied on this peer.
    let other_idx = (leader_idx + 1) % clients.len();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut saw_replicated = false;
    while Instant::now() < deadline {
        let batch = clients[other_idx]
            .receive(TOPIC, 16)
            .await
            .expect("receive on follower");
        if batch.iter().any(|m| &m.payload[..] == b"replicated-value") {
            saw_replicated = true;
            break;
        }
        sleep(Duration::from_millis(150)).await;
    }
    assert!(
        saw_replicated,
        "non-leader broker {other_idx} never saw the replicated record"
    );

    // Tear down children.
    drop(clients);
    for mut c in children {
        let _ = c.kill().await;
        let _ = c.wait().await;
    }
}

async fn write_file(path: &std::path::Path, contents: &[u8]) {
    let mut f = tokio::fs::File::create(path).await.unwrap();
    f.write_all(contents).await.unwrap();
    f.flush().await.unwrap();
}

async fn wait_until_port_open(port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("qxd never bound to port {port} within {timeout:?}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}
