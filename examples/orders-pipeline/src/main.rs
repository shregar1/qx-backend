//! orders-pipeline — a self-contained example of how to wire QX end-to-end.
//!
//! Embeds a broker (no `qxd` needed to run), then in the same process:
//! 1. A producer task sends 10 orders to topic `orders` with key = order id.
//! 2. Two consumers join the `fulfillment` group, pull their assigned
//!    partitions, and (for each order) create a workflow task `deploy-order`
//!    that gets marked Done on completion.
//! 3. After processing, the example prints a summary and exits.
//!
//! Run: `cargo run -p orders-pipeline`

use std::time::Duration;

use qx_broker::{Broker, Client};

const TOPIC: &str = "orders";
const GROUP: &str = "fulfillment";
const WORKFLOW: &str = "deploy-order";
const PARTITIONS: u32 = 4;
const ORDER_COUNT: u32 = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    println!("orders-pipeline: data_dir = {}", tmp.path().display());

    // 1. Start an embedded broker
    let broker = Broker::open(tmp.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let url = format!("http://{addr}");
    println!("orders-pipeline: broker listening on {url}");
    let broker_for_serve = broker.clone();
    tokio::spawn(async move {
        broker_for_serve
            .serve_with_listener(listener)
            .await
            .ok();
    });

    // Give the server a moment to start accepting
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. Producer
    let mut producer = Client::connect(url.clone()).await?;
    producer.open_topic(TOPIC, PARTITIONS).await?;
    println!("orders-pipeline: producing {ORDER_COUNT} orders");
    for i in 0..ORDER_COUNT {
        let key = format!("order-{i:03}");
        let payload = format!(r#"{{"order_id":"{key}","sku":"QX-T-shirt","qty":1}}"#);
        producer
            .send(TOPIC, key.as_bytes(), payload.as_bytes())
            .await?;
    }

    // 3. Two consumers join the group
    let mut consumer_a = Client::connect(url.clone()).await?;
    let (member_a, _, _) = consumer_a.join_group(TOPIC, GROUP).await?;
    let mut consumer_b = Client::connect(url.clone()).await?;
    let (member_b, _, assignment_b) = consumer_b.join_group(TOPIC, GROUP).await?;

    // After B joins, A's old assignment is stale — heartbeat to get the
    // post-rebalance partition list.
    let (_, assignment_a) = consumer_a.heartbeat(TOPIC, GROUP, member_a).await?;
    println!(
        "orders-pipeline: A=member{member_a} partitions={assignment_a:?} | B=member{member_b} partitions={assignment_b:?}"
    );

    // Each consumer processes its assigned partitions
    let mut tasks: Vec<u64> = Vec::new();
    for (consumer, assignment) in [
        (&mut consumer_a, assignment_a.clone()),
        (&mut consumer_b, assignment_b.clone()),
    ] {
        for partition in &assignment {
            let batch = consumer
                .stream_from_group(TOPIC, GROUP, *partition, 100)
                .await?;
            for rec in &batch {
                // For each order, kick off a workflow task
                let task_id = consumer
                    .create_root_task(WORKFLOW, rec.payload.clone())
                    .await?;
                tasks.push(task_id);
                consumer
                    .complete_task(WORKFLOW, task_id, b"processed".to_vec())
                    .await?;
            }
            if let Some(last) = batch.last() {
                consumer
                    .commit_group_offset(TOPIC, GROUP, *partition, last.id + 1)
                    .await?;
            }
        }
    }

    // 4. Summary
    println!(
        "orders-pipeline: done. orders sent={ORDER_COUNT} workflow tasks completed={}",
        tasks.len(),
    );
    println!("orders-pipeline: (queue stats show all {ORDER_COUNT} pending because");
    println!("                 stream/commit-offset is the stream consumer model");
    println!("                 — records stay in the log; consumers track offsets.)");

    // Cleanly leave the group
    consumer_a.leave_group(TOPIC, GROUP, member_a).await?;
    consumer_b.leave_group(TOPIC, GROUP, member_b).await?;

    // Keep broker alive long enough that the closing RPCs complete
    drop(producer);
    drop(consumer_a);
    drop(consumer_b);
    let _ = broker;

    Ok(())
}

fn tempdir() -> std::io::Result<TempDir> {
    let mut path = std::env::temp_dir();
    path.push(format!("orders-pipeline-{}", std::process::id()));
    std::fs::create_dir_all(&path)?;
    Ok(TempDir { path })
}

struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

