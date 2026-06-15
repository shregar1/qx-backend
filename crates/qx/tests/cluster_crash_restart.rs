//! Multi-process crash + restart: spawn three `qx` brokers
//! configured for cluster mode, drive a steady stream of Produces
//! through one of them, SIGKILL a follower, restart it with its
//! original data directory, then drive more Produces and verify:
//!
//!   1. The cluster keeps accepting writes while the follower is
//!      down (the leader + the surviving follower form a quorum).
//!   2. The restarted broker catches up via Raft replication.
//!   3. The restarted broker's local Topic ends up with EVERY
//!      replicated record exactly once — no duplicates from
//!      openraft re-applying entries past `last_applied`, no gaps.
//!
//! This is the integration test the apply-cursor logic was added
//! for. Without the cursor, the restart-apply path in openraft can
//! double-write entries to the broker's Topic; the cursor (stored
//! atomically with the topic write inside the same redb commit)
//! makes the re-apply a no-op.

use std::process::Stdio;
use std::time::Duration;

use qx_broker::Client;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::time::{Instant, sleep};

const NUM_BROKERS: u64 = 3;
const TOPIC: &str = "after-restart";

#[tokio::test]
async fn follower_restart_catches_up_without_duplicate_apply() {
    let tmp = TempDir::new().unwrap();
    // Reserve 3 client ports + 3 raft ports up front.
    let (client_ports, raft_ports) = reserve_ports(NUM_BROKERS as usize).await;

    let voters_block = (0..NUM_BROKERS)
        .map(|i| {
            format!(
                "  [[cluster.voters]]\n  node_id = {i}\n  addr = \"127.0.0.1:{}\"\n  client_addr = \"127.0.0.1:{}\"\n",
                raft_ports[i as usize], client_ports[i as usize]
            )
        })
        .collect::<Vec<_>>()
        .join("");

    let mut config_paths = Vec::new();
    let mut data_dirs: Vec<std::path::PathBuf> = Vec::new();
    for id in 0..NUM_BROKERS {
        let data_dir = tmp.path().join(format!("data-{id}"));
        std::fs::create_dir_all(&data_dir).unwrap();
        let config_path = tmp.path().join(format!("qx-{id}.toml"));
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
        data_dirs.push(data_dir);
    }

    // Spawn the three brokers.
    let mut children = Vec::new();
    for path in &config_paths {
        children.push(spawn_qx(path).await);
    }
    for p in &client_ports {
        wait_until_port_open(*p, Duration::from_secs(15)).await;
    }

    // Connect a client to each broker so we can probe and produce.
    let mut clients: Vec<Client> = Vec::new();
    for p in &client_ports {
        clients.push(
            Client::connect(format!("http://127.0.0.1:{p}"))
                .await
                .expect("client connect"),
        );
    }

    // Create the topic. Forwarding is on (we configured client_addrs),
    // so OpenTopic on any broker should work.
    let create_deadline = Instant::now() + Duration::from_secs(20);
    'create: loop {
        for c in clients.iter_mut() {
            if c.open_topic(TOPIC, 4).await.is_ok() {
                break 'create;
            }
        }
        if Instant::now() >= create_deadline {
            panic!("OpenTopic never accepted");
        }
        sleep(Duration::from_millis(150)).await;
    }

    // First wave: 20 produces via broker 0. Each lands (via
    // forwarding if needed) on the leader and replicates to all 3.
    for i in 0..20u32 {
        let key = format!("k-{i}");
        let val = format!("pre-restart-{i}");
        clients[0]
            .send(TOPIC, key.as_bytes(), val.as_bytes())
            .await
            .expect("send pre-restart");
    }

    // Wait for replication to converge. Probe via broker 1 (a
    // follower) — its receive returns from its LOCAL Topic state, so
    // a positive result means the apply landed there.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let batch = clients[1].receive(TOPIC, 64).await.expect("receive");
        let pre = batch
            .iter()
            .filter(|m| {
                std::str::from_utf8(&m.payload)
                    .map(|s| s.starts_with("pre-restart-"))
                    .unwrap_or(false)
            })
            .count();
        if pre >= 20 {
            // Re-deliver these on the next receive by nacking — we
            // want to count them post-restart, not consume them now.
            for m in batch {
                let _ = clients[1].nack(TOPIC, m.receipt).await;
            }
            break;
        }
        for m in batch {
            let _ = clients[1].nack(TOPIC, m.receipt).await;
        }
        if Instant::now() >= deadline {
            panic!("replication did not converge before restart");
        }
        sleep(Duration::from_millis(150)).await;
    }

    // Pick a follower to kill. The probe-then-OpenTopic loop above
    // could have made broker 0 the leader (it tried first); kill a
    // different one to make sure we're killing a follower. Index 2
    // is far enough away to be safe in practice.
    let victim_idx: usize = 2;
    children[victim_idx].kill().await.expect("SIGKILL victim");
    let _ = children[victim_idx].wait().await;

    // Drop the killed client; it would block on a dead connection.
    drop(clients.remove(victim_idx));

    // Second wave: 10 more produces while one broker is down. Quorum
    // is 2/3 so the cluster must keep accepting.
    for i in 0..10u32 {
        let key = format!("k-down-{i}");
        let val = format!("during-down-{i}");
        // Send via the first remaining client. Forwarding routes us
        // to whichever broker is the leader.
        clients[0]
            .send(TOPIC, key.as_bytes(), val.as_bytes())
            .await
            .expect("send while one follower down");
    }

    // Restart the killed broker with the same config file (and
    // therefore the same data directory). Its Raft log + topic data
    // are still on disk; it has to catch up the 10 newer entries
    // through Raft replication.
    let restart = spawn_qx(&config_paths[victim_idx]).await;
    children.insert(victim_idx, restart);
    wait_until_port_open(client_ports[victim_idx], Duration::from_secs(15)).await;

    let mut restarted_client = Client::connect(format!("http://127.0.0.1:{}", client_ports[victim_idx]))
        .await
        .expect("reconnect restarted broker");
    clients.insert(victim_idx, restarted_client.clone());

    // Wait until the restarted broker has applied all 30 entries to
    // its local Topic (20 pre-restart + 10 during-down). We measure
    // by counting how many distinct payloads with our known prefixes
    // it can deliver.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut converged = false;
    let mut last_seen_pre = 0;
    let mut last_seen_down = 0;
    while !converged && Instant::now() < deadline {
        let batch = restarted_client
            .receive(TOPIC, 128)
            .await
            .expect("receive from restarted broker");
        let pre = batch
            .iter()
            .filter(|m| {
                std::str::from_utf8(&m.payload)
                    .map(|s| s.starts_with("pre-restart-"))
                    .unwrap_or(false)
            })
            .count();
        let down = batch
            .iter()
            .filter(|m| {
                std::str::from_utf8(&m.payload)
                    .map(|s| s.starts_with("during-down-"))
                    .unwrap_or(false)
            })
            .count();
        last_seen_pre = pre;
        last_seen_down = down;
        // Nack so the next iteration sees the same content.
        for m in batch {
            let _ = restarted_client.nack(TOPIC, m.receipt).await;
        }
        if pre == 20 && down == 10 {
            converged = true;
        } else {
            sleep(Duration::from_millis(200)).await;
        }
    }
    assert!(
        converged,
        "restarted broker did not catch up: pre={last_seen_pre}/20 down={last_seen_down}/10"
    );

    // Final and most important check: no duplicates. The apply
    // cursor must have blocked openraft from double-applying the
    // pre-restart entries when the restarted broker rebuilt its
    // state machine from disk. Each unique payload should appear
    // exactly once in the topic.
    //
    // The receive loop above only sees in-flight messages; nack
    // returns them to Pending. Drain everything by acking this time
    // and check counts.
    let mut total_pre = 0;
    let mut total_down = 0;
    let mut seen_payloads = std::collections::HashSet::new();
    let drain_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < drain_deadline {
        let batch = restarted_client
            .receive(TOPIC, 128)
            .await
            .expect("drain receive");
        if batch.is_empty() {
            break;
        }
        for m in batch {
            let payload = String::from_utf8_lossy(&m.payload).to_string();
            if payload.starts_with("pre-restart-") {
                total_pre += 1;
            }
            if payload.starts_with("during-down-") {
                total_down += 1;
            }
            assert!(
                seen_payloads.insert(payload.clone()),
                "duplicate payload after restart: {payload}"
            );
            let _ = restarted_client.ack(TOPIC, m.receipt).await;
        }
    }
    assert_eq!(total_pre, 20, "pre-restart payload count diverged from 20");
    assert_eq!(total_down, 10, "during-down payload count diverged from 10");

    // Tear down children.
    drop(clients);
    for mut c in children {
        let _ = c.kill().await;
        let _ = c.wait().await;
    }
}

async fn reserve_ports(n: usize) -> (Vec<u16>, Vec<u16>) {
    let mut a = Vec::new();
    let mut b = Vec::new();
    for _ in 0..n {
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        a.push(l1.local_addr().unwrap().port());
        b.push(l2.local_addr().unwrap().port());
    }
    (a, b)
}

async fn spawn_qx(config_path: &std::path::Path) -> tokio::process::Child {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_qx"))
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn qx")
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
            panic!("qx never bound to port {port} within {timeout:?}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}
