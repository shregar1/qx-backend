//! Live-demo driver for the embedded qx dashboard.
//!
//! Spawns:
//!   * Topic producers at varied rates so the Overview throughput
//!     sparkline animates and the Topics page shows non-zero
//!     msgs/s and bytes/s on every row.
//!   * Consumer groups that join + heartbeat + commit offsets so
//!     the Groups page lists real members and lag.
//!   * Workflow tasks (root + child + complete / fail / cancel) so
//!     the Workflows page populates with non-zero counts in every
//!     status column.
//!
//! Reads:
//!   * `QX_URL`   — broker gRPC URL (default `http://127.0.0.1:55411`)
//!   * `QX_TOKEN` — bearer token (required)
//!
//! Run from the workspace root:
//!
//!   QX_URL=http://127.0.0.1:55411 \
//!   QX_TOKEN=qx_live_… \
//!     cargo run --release -p dashboard-demo
//!
//! ^C to stop. The driver is idempotent — re-running just resumes
//! producing into the same topics + workflows.

use std::time::Duration;

use qx_broker::Client;

const DEFAULT_URL: &str = "http://127.0.0.1:55411";

/// One row in the Topics page after the driver has been running for
/// a few minutes. Picks topic-name + partition-count + send rate so
/// the dashboard's sparkline + StatCards have a wide spread.
struct TopicPlan {
    name: &'static str,
    partitions: u32,
    /// Milliseconds between sends per producer.
    interval_ms: u64,
    /// Bytes per message — varied so the bytes/s column doesn't
    /// look uniform.
    payload_size: usize,
}

const TOPICS: &[TopicPlan] = &[
    TopicPlan { name: "orders",       partitions: 4, interval_ms:  60,  payload_size:  256 },
    TopicPlan { name: "payments",     partitions: 2, interval_ms: 100,  payload_size:  512 },
    TopicPlan { name: "shipments",    partitions: 2, interval_ms: 250,  payload_size:  192 },
    TopicPlan { name: "telemetry",    partitions: 8, interval_ms:  20,  payload_size:   96 },
    TopicPlan { name: "audit",        partitions: 1, interval_ms: 500,  payload_size: 1024 },
];

/// `(group, topic)` pairs — one consumer per pair. Lag accumulates
/// because we ack slower than the producers send.
const GROUPS: &[(&str, &str)] = &[
    ("fulfilment-svc",   "orders"),
    ("ledger-svc",       "payments"),
    ("notifier-svc",     "orders"),
    ("warehouse-svc",    "shipments"),
    ("analytics-svc",    "telemetry"),
];

/// Workflows we'll mint tasks against. Each spawns a writer that
/// keeps the funnel `pending → running → done/failed/cancelled`
/// in motion so every status column on the Workflows page lights up.
const WORKFLOWS: &[&str] = &[
    "ship-deploy",
    "import-csv",
    "billing-rollup",
    "user-onboarding",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let url = std::env::var("QX_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let token = std::env::var("QX_TOKEN")
        .expect("QX_TOKEN must be set (the keyx PAT qx accepts)");

    eprintln!("demo: target broker = {url}");
    eprintln!("demo: bearer prefix = {}…", &token[..token.len().min(12)]);

    // Bootstrap every topic up-front so the Topics page populates on
    // the very first dashboard poll (otherwise the first ~2s show an
    // empty grid).
    let mut admin = Client::connect_with_token(url.clone(), token.clone())
        .await
        .expect("connect");
    for t in TOPICS {
        let count = admin
            .open_topic(t.name, t.partitions)
            .await
            .expect("open_topic");
        eprintln!("demo: opened topic {:<10}  partitions={count}", t.name);
    }
    drop(admin);

    // ── Producers ───────────────────────────────────────────────────
    for t in TOPICS {
        let url = url.clone();
        let token = token.clone();
        let payload = std::iter::repeat(b'.').take(t.payload_size).collect::<Vec<u8>>();
        let name = t.name;
        let interval = t.interval_ms;
        tokio::spawn(async move {
            let mut c = match Client::connect_with_token(url, token).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("demo: producer/{name} connect failed: {e:?}");
                    return;
                }
            };
            let mut i: u64 = 0;
            let mut tick = tokio::time::interval(Duration::from_millis(interval));
            loop {
                tick.tick().await;
                let key = format!("k-{:04}", i % 100);
                if let Err(e) = c.send(name, key.as_bytes(), &payload).await {
                    eprintln!("demo: producer/{name} send failed: {e:?}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                i = i.wrapping_add(1);
            }
        });
    }

    // ── Consumer groups ────────────────────────────────────────────
    //
    // Join ONCE per group (each `join_group` call adds a member —
    // looping it like the previous version did inflated the
    // members column to N × iterations). Then loop the stream +
    // commit cycle only, so members stays at the real consumer
    // count (1 per group) and lag still moves.
    for (group, topic) in GROUPS {
        let url = url.clone();
        let token = token.clone();
        let group = group.to_string();
        let topic = topic.to_string();
        tokio::spawn(async move {
            let mut c = match Client::connect_with_token(url, token).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("demo: group/{group} connect failed: {e:?}");
                    return;
                }
            };
            let assignment = loop {
                match c.join_group(&topic, &group).await {
                    Ok((_id, _gen, a)) => break a,
                    Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
                }
            };
            loop {
                for &partition in &assignment {
                    let batch = c
                        .stream_from_group(&topic, &group, partition, 5)
                        .await
                        .unwrap_or_default();
                    if let Some(last) = batch.last() {
                        let _ = c
                            .commit_group_offset(&topic, &group, partition, last.id + 1)
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(750)).await;
            }
        });
    }

    // ── Workflows ──────────────────────────────────────────────────
    //
    // For each workflow: keep ~3 root tasks in flight at any time,
    // a child task per root, then complete / fail / cancel them in
    // a 60/30/10 split so every status column is populated.
    for &wf in WORKFLOWS {
        let url = url.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let mut c = match Client::connect_with_token(url, token).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("demo: workflow/{wf} connect failed: {e:?}");
                    return;
                }
            };
            let mut counter: u64 = 0;
            loop {
                // Mint a root.
                let root = match c.create_root_task(wf, format!("input-{counter}").into_bytes()).await {
                    Ok(id) => id,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                // One child task linked to the root.
                if let Ok(child) = c
                    .create_child_task(wf, root, format!("sub-{counter}").into_bytes())
                    .await
                {
                    // Production-like distribution out of 20 buckets so
                    // every state column lights up within ~8s (20 iter
                    // × 400ms). The previous 25% fail / 15% cancel
                    // made the Workflows page look catastrophic — that
                    // was a demo-shape problem, not a broker error.
                    //
                    //   17/20 = 85% complete   (happy path)
                    //    1/20 =  5% fail       (synthetic error rate)
                    //    1/20 =  5% cancel     (cascades to child)
                    //    1/20 =  5% leave running (populates "running")
                    let bucket = counter % 20;
                    if bucket < 17 {
                        let _ = c.complete_task(wf, child, b"done".to_vec()).await;
                        let _ = c.complete_task(wf, root, b"done".to_vec()).await;
                    } else if bucket == 17 {
                        let _ = c
                            .fail_task(wf, child, "synthetic-fail".to_string())
                            .await;
                        let _ = c.complete_task(wf, root, b"done".to_vec()).await;
                    } else if bucket == 18 {
                        // Cancel the root — cascades to the child via
                        // `cancel_max_descendants`.
                        let _ = c.cancel_task(wf, root).await;
                    } else {
                        // Leave both running.
                    }
                }
                counter = counter.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        });
    }

    eprintln!("demo: producers + consumers + workflows running. ^C to stop.");
    // Park forever; the spawned tasks do the work.
    std::future::pending::<()>().await;
}
