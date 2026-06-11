//! Throughput micro-benchmark for the queue layer. Not a criterion harness
//! — it's `#[ignore]`d so it doesn't run by default. Invoke with
//!
//! ```
//! cargo test -p qx-queue --release --test bench_redb -- --ignored --nocapture
//! ```
//!
//! Goal: establish a real, runnable baseline number for slice 1
//! ("kill per-message redb commit"). After the range-encoding refactor
//! the same harness should show a multi-x throughput improvement on
//! batched send + receive + ack.

use std::time::{Duration, Instant};

use qx_queue::{Queue, QueueConfig};
use qx_storage::PartitionConfig;
use tempfile::tempdir;

fn fast_cfg() -> QueueConfig {
    QueueConfig {
        partition: PartitionConfig::default(),
        visibility_timeout: Duration::from_secs(300),
        max_attempts: 5,
        durable_send: false,    // no fsync on log
        eventual_commits: true, // no fsync on redb
        track_message_state: true,
    }
}

fn report(label: &str, n: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let ops = (n as f64) / secs;
    println!(
        "  {:<32} {:>10} ops in {:>7.3}s  =>  {:>10.0} ops/s  ({:>7.2} us/op)",
        label,
        n,
        secs,
        ops,
        (secs * 1e6) / (n as f64),
    );
}

#[test]
#[ignore]
fn bench_send_single() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), fast_cfg()).unwrap();
    let n: u64 = 50_000;
    let payload = vec![0u8; 64];
    let t = Instant::now();
    for _ in 0..n {
        q.send(&payload).unwrap();
    }
    report("send (single, 64B payload)", n, t.elapsed());
}

#[test]
#[ignore]
fn bench_send_batched_100() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), fast_cfg()).unwrap();
    let n_batches: u64 = 2_000;
    let batch_size = 100;
    let payload = vec![0u8; 64];
    let refs: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();
    let t = Instant::now();
    for _ in 0..n_batches {
        q.send_batch(&refs).unwrap();
    }
    let total = n_batches * (batch_size as u64);
    report("send_batch(100, 64B)", total, t.elapsed());
}

#[test]
#[ignore]
fn bench_send_batched_1000() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), fast_cfg()).unwrap();
    let n_batches: u64 = 500;
    let batch_size = 1_000;
    let payload = vec![0u8; 64];
    let refs: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();
    let t = Instant::now();
    for _ in 0..n_batches {
        q.send_batch(&refs).unwrap();
    }
    let total = n_batches * (batch_size as u64);
    report("send_batch(1000, 64B)", total, t.elapsed());
}

#[test]
#[ignore]
fn bench_send_receive_ack_loop() {
    // The "full lifecycle" path — produce, consume, ack in batches.
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), fast_cfg()).unwrap();
    let n_batches: u64 = 500;
    let batch_size = 100;
    let payload = vec![0u8; 64];
    let refs: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();
    let t = Instant::now();
    for _ in 0..n_batches {
        q.send_batch(&refs).unwrap();
        let received = q.receive(batch_size).unwrap();
        for m in received {
            q.ack(m.receipt).unwrap();
        }
    }
    let total = n_batches * (batch_size as u64);
    report("send+recv+ack (batch=100)", total, t.elapsed());
}

#[test]
#[ignore]
fn bench_send_receive_ack_batch_loop() {
    // Same lifecycle as `bench_send_receive_ack_loop` but uses
    // `ack_batch` to amortize the redb commit cost. This is the
    // headline win for slice 1: pre-refactor was bound to 1 commit
    // per ack, post-refactor users can collapse 100 acks into 1
    // commit.
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), fast_cfg()).unwrap();
    let n_batches: u64 = 500;
    let batch_size = 100;
    let payload = vec![0u8; 64];
    let refs: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();
    let t = Instant::now();
    for _ in 0..n_batches {
        q.send_batch(&refs).unwrap();
        let received = q.receive(batch_size).unwrap();
        let receipts: Vec<_> = received.into_iter().map(|m| m.receipt).collect();
        q.ack_batch(&receipts).unwrap();
    }
    let total = n_batches * (batch_size as u64);
    report("send+recv+ack_batch (b=100)", total, t.elapsed());
}

#[test]
#[ignore]
fn bench_steady_state_at_scale() {
    // Stage 1M messages, then time receive+ack throughput on the
    // already-populated queue. This is the workload that exposes the
    // B-tree-per-message ceiling — the messages table is large, every
    // receive() iterates its head, every ack() does a delete + commit.
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), fast_cfg()).unwrap();
    let stage: u64 = 200_000;
    let batch_size = 100;
    let payload = vec![0u8; 64];
    let refs: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();

    let stage_t = Instant::now();
    let mut staged = 0;
    while staged < stage {
        q.send_batch(&refs).unwrap();
        staged += batch_size as u64;
    }
    report("[stage] send_batch(100)", staged, stage_t.elapsed());

    let drain_t = Instant::now();
    let mut drained = 0;
    while drained < staged {
        let batch = q.receive(batch_size).unwrap();
        if batch.is_empty() {
            break;
        }
        let count = batch.len() as u64;
        let receipts: Vec<_> = batch.into_iter().map(|m| m.receipt).collect();
        q.ack_batch(&receipts).unwrap();
        drained += count;
    }
    report(
        "[drain] receive+ack_batch (b=100)",
        drained,
        drain_t.elapsed(),
    );
}
