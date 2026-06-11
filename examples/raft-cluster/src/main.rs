//! 3-node qx-raft cluster demo.
//!
//! Boots a `ReplicatedTopic` with 3 nodes, sends 10 records through it,
//! waits for every replica to apply the same log, and prints what each
//! node sees. The point: prove the raft crate replicates correctly. The
//! broker doesn't use this yet — wiring it into the Kafka API surface is
//! a separate ticket (every `Arc<Mutex<Topic>>` site becomes
//! `Arc<ReplicatedTopic>` and switches from sync `.send_to()` to async
//! `.send().await`).
//!
//! Run with:
//!
//! ```bash
//! cargo run -p raft-cluster-demo
//! ```

use std::time::Duration;

use qx_raft::ReplicatedTopic;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("→ bootstrapping a 3-node ReplicatedTopic with 1 partition");
    let topic = ReplicatedTopic::bootstrap(3, 1).await?;
    println!("  cluster size: {} node(s)", 3);

    // Send 10 records through the leader. Each `send` returns the
    // partition it landed in; with 1 partition that's always 0.
    println!("→ sending 10 records through the leader");
    for i in 0..10 {
        let payload = format!("record-{i}").into_bytes();
        let partition = topic.send(b"demo-key", payload).await?;
        println!("  appended record {i} → partition {partition}");
    }

    // Wait for every node's local apply to catch up. This is the
    // "replication landed everywhere" moment.
    println!("→ waiting for every node to apply the full log");
    topic
        .wait_for_apply(0, 10, Duration::from_secs(5))
        .await?;
    println!("  all nodes caught up to count=10");

    // Read from each node independently. Replication is correct iff
    // each node returns the same 10 records in the same order.
    println!("→ verifying each node's view of partition 0:");
    let mut seen: Vec<Vec<(u64, Vec<u8>)>> = Vec::new();
    for node in 0..3 {
        let recs = topic.stream(node, "demo-group", 0, 1024)?;
        let preview: Vec<String> = recs
            .iter()
            .take(3)
            .map(|(id, p)| format!("{}={}", id, String::from_utf8_lossy(p)))
            .collect();
        println!(
            "  node {node}: {} records [{}{}]",
            recs.len(),
            preview.join(", "),
            if recs.len() > 3 { ", …" } else { "" }
        );
        seen.push(recs);
    }

    // Cross-check.
    let baseline = &seen[0];
    for (i, view) in seen.iter().enumerate().skip(1) {
        assert_eq!(
            view.len(),
            baseline.len(),
            "node {i} disagrees on count with node 0"
        );
        for (j, (id, payload)) in view.iter().enumerate() {
            assert_eq!(*id, baseline[j].0, "node {i} record {j} id diverges");
            assert_eq!(payload, &baseline[j].1, "node {i} record {j} payload diverges");
        }
    }
    println!("✓ all nodes converged on identical state — replication works");

    Ok(())
}
