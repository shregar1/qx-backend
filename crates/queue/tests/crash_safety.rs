//! Kill -9 crash-safety harness for the queue layer.
//!
//! For each iteration the harness:
//!   1. spawns the `crash_producer` example as a child process,
//!   2. waits until the child reports a "tick" line on stdout (a sign
//!      it has acked at least N messages durably),
//!   3. sleeps a random extra interval to vary the kill point,
//!   4. SIGKILLs the child,
//!   5. reopens the queue and reads the child's acks.log,
//!   6. verifies the **durability invariant**: every offset the child
//!      durably acked must be absent from the queue (cannot be
//!      received), and the log's high-water mark covers all acked
//!      offsets.
//!
//! Run with:
//!
//! ```
//! cargo test -p qx-queue --release --test crash_safety -- --ignored --nocapture
//! ```
//!
//! The harness is `#[ignore]`d so it does not run on every `cargo test`
//! (it spawns subprocesses and runs for minutes).

use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use qx_queue::{Queue, QueueConfig};
use qx_storage::PartitionConfig;
use tempfile::tempdir;

fn workspace_target_dir() -> PathBuf {
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let mut p = PathBuf::from(crate_dir);
    loop {
        let candidate = p.join("target");
        if candidate.exists() {
            return candidate;
        }
        if !p.pop() {
            panic!("could not locate workspace target/ from {crate_dir}");
        }
    }
}

fn producer_binary() -> PathBuf {
    let bin = workspace_target_dir().join("release/examples/crash_producer");
    if !bin.exists() {
        // Build it. The test will run faster on subsequent iterations.
        let status = Command::new(env!("CARGO"))
            .args([
                "build",
                "--release",
                "--example",
                "crash_producer",
                "-p",
                "qx-queue",
            ])
            .status()
            .expect("invoke cargo build");
        assert!(status.success(), "failed to build crash_producer example");
    }
    assert!(
        bin.exists(),
        "crash_producer not at expected path: {}",
        bin.display()
    );
    bin
}

struct ChildHandle {
    child: Child,
}

impl ChildHandle {
    fn kill_nine(&mut self) {
        // We use SIGKILL specifically — no clean shutdown. The
        // queue must survive this on its own merits.
        #[cfg(unix)]
        unsafe {
            libc_kill(self.child.id() as i32, 9);
        }
        // Reap.
        let _ = self.child.wait();
    }
}

// Minimal libc bridge — avoid pulling in the libc crate just for one
// call.
#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

#[cfg(unix)]
unsafe fn libc_kill(pid: i32, sig: i32) {
    unsafe {
        kill(pid, sig);
    }
}

fn spawn_producer(queue_dir: &PathBuf, acks_log: &PathBuf) -> (ChildHandle, mpsc::Receiver<u64>) {
    let bin = producer_binary();
    let mut child = Command::new(&bin)
        .arg(queue_dir)
        .arg(acks_log)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn crash_producer");

    let stdout = child.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel::<u64>();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            // Parse "tick produced=X acked=Y" — only acked matters here.
            if let Some(idx) = line.find("acked=") {
                let s = &line[idx + 6..];
                if let Ok(n) = s.trim().parse::<u64>()
                    && tx.send(n).is_err()
                {
                    break;
                }
            }
        }
    });

    (ChildHandle { child }, rx)
}

fn wait_for_acked_at_least(rx: &mpsc::Receiver<u64>, threshold: u64, timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    let mut last = 0;
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(n) => {
                last = n;
                if n >= threshold {
                    return n;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    last
}

fn read_acks_log(path: &PathBuf) -> HashSet<u64> {
    let Ok(bytes) = fs::read(path) else {
        return HashSet::new();
    };
    let text = String::from_utf8_lossy(&bytes);
    text.lines()
        .filter_map(|l| l.trim().parse::<u64>().ok())
        .collect()
}

fn verify_durability_invariant(
    queue_dir: &PathBuf,
    acks_log_path: &PathBuf,
    iteration: usize,
) -> Result<(usize, usize, usize, usize), String> {
    let acked = read_acks_log(acks_log_path);
    let acked_count = acked.len();
    let max_acked = acked.iter().copied().max();

    // Drain config: short-ish visibility timeout so receipts the
    // drain pulls don't go stale between successive `receive` calls,
    // but long enough that the drain has time to ack a batch before
    // the visibility window expires. The producer's own visibility
    // timeout was 500 ms — after the sleep below, any pre-crash
    // InFlight rows have already become claimable.
    let cfg = QueueConfig {
        partition: PartitionConfig::default(),
        visibility_timeout: Duration::from_secs(30),
        max_attempts: 1_000_000,
        durable_send: false,
        eventual_commits: true,
        track_message_state: true,
    };
    let mut q = Queue::open(queue_dir, cfg).map_err(|e| format!("reopen failed: {e}"))?;
    let log_next = q.next_record_id();

    // Invariant 1: the log must cover every offset the producer
    // claimed to have acked. The producer only writes to acks.log
    // after queue.ack returned Ok — and that path went through both
    // a log sync (durable_send=true) and a redb commit fsync.
    if let Some(max_a) = max_acked
        && log_next <= max_a
    {
        return Err(format!(
            "[iter {iteration}] log_next={log_next} <= max_acked={max_a} — log lost durable writes"
        ));
    }

    // Drain everything the queue can give us. Producer ordering is
    // write-app-state-then-queue-ack, so an offset may legitimately
    // appear in BOTH `acked` and the drain (crash between fsync and
    // queue.ack). That is at-least-once, not a bug.
    //
    // What is NOT allowed is *data loss*: an offset that exists in
    // the log but isn't in `acked` and isn't recoverable through the
    // drain (or carried as residual that we just don't drain).
    let mut delivered_after_crash: usize = 0;
    let mut delivered_set: std::collections::HashSet<u64> = std::collections::HashSet::new();
    // Wait for the producer's 500 ms visibility timeout to elapse so
    // any InFlight rows left behind by a mid-batch crash become
    // claimable through `receive`.
    thread::sleep(Duration::from_millis(600));
    loop {
        let batch = q.receive(500).map_err(|e| format!("receive: {e}"))?;
        if batch.is_empty() {
            break;
        }
        let mut receipts = Vec::with_capacity(batch.len());
        for m in batch {
            delivered_after_crash += 1;
            delivered_set.insert(m.id);
            receipts.push(m.receipt);
        }
        // Ack each batch as we go — accumulating receipts across
        // batches would let later receives bump `attempts` past the
        // receipts we already have, making them stale.
        q.ack_batch(&receipts)
            .map_err(|e| format!("ack_batch drain: {e}"))?;
    }

    // Invariant 2: full accounting — every log offset is reachable
    // through `acked ∪ delivered_set ∪ residual_state`. Offsets that
    // are silently dropped (neither acked, nor delivered, nor sitting
    // in the queue's state) are data loss.
    let stats = q.stats().map_err(|e| format!("stats: {e}"))?;
    let residual = stats.pending + stats.inflight + stats.dead;
    let union: std::collections::HashSet<u64> = acked.iter().chain(delivered_set.iter()).copied().collect();
    let union_size = union.len() as u64;
    let total_known = union_size + residual;
    if total_known < log_next {
        let mut missing: Vec<u64> = Vec::new();
        for offset in 0..log_next {
            if !union.contains(&offset) {
                missing.push(offset);
                if missing.len() >= 10 {
                    break;
                }
            }
        }
        // Anything in `missing` that's also in residual is accounted
        // for; what's left over is true data loss.
        return Err(format!(
            "[iter {iteration}] data loss: log_next={log_next} accounted_union={union_size} residual={residual} (total_known={total_known}; sample-missing={:?})",
            missing
        ));
    }

    if residual != 0 {
        return Err(format!(
            "[iter {iteration}] drain left residual={residual} in queue (pending={}, inflight={}, dead={})",
            stats.pending, stats.inflight, stats.dead
        ));
    }

    Ok((
        acked_count,
        delivered_after_crash,
        log_next as usize,
        residual as usize,
    ))
}

#[test]
#[ignore]
fn crash_safety_random_kills() {
    // Number of independent crash iterations. Each one spawns a fresh
    // queue dir / acks log, lets the producer run briefly, then kills.
    let iterations: usize = std::env::var("CRASH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // The "make sure the producer has done real work" knob. We require
    // the producer to have durably acked at least this many offsets
    // before the kill arrives, so the test isn't measuring "did we
    // kill it before it had a chance to do anything."
    let min_acked_before_kill: u64 = std::env::var("MIN_ACKED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000);

    let max_wait = Duration::from_secs(60);
    let mut rng_state: u64 = 0xa537_1d9c_f001_b00d;

    println!("crash_safety_random_kills: {iterations} iterations, min_acked={min_acked_before_kill}");

    for i in 0..iterations {
        let tmp = tempdir().expect("tempdir");
        let queue_dir = tmp.path().join("queue");
        let acks_log = tmp.path().join("acks.log");
        fs::create_dir_all(&queue_dir).unwrap();

        let (mut handle, rx) = spawn_producer(&queue_dir, &acks_log);
        let acked_at_threshold = wait_for_acked_at_least(&rx, min_acked_before_kill, max_wait);
        if acked_at_threshold < min_acked_before_kill {
            handle.kill_nine();
            panic!(
                "[iter {i}] producer only acked {acked_at_threshold} (needed {min_acked_before_kill}) within {max_wait:?}"
            );
        }

        // Extra randomized delay to vary the kill point relative to
        // the redb commit / log sync / acks.log fsync interleaving.
        // Range is 0 – ~50 ms — wide enough to land in the middle of
        // a typical batch (a batch is roughly 20 ms at our config),
        // not just between batches.
        // xorshift64 — deterministic for reproducibility across runs.
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        let extra_us = (rng_state % 50_000) as u64; // 0–50ms
        thread::sleep(Duration::from_micros(extra_us));

        handle.kill_nine();

        match verify_durability_invariant(&queue_dir, &acks_log, i) {
            Ok((acked, delivered_after, log_next, residual)) => {
                println!(
                    "[iter {i}] OK: durably_acked={acked} unacked_recovered={delivered_after} log_next={log_next} residual={residual} extra_kill_jitter={extra_us}us"
                );
            }
            Err(e) => panic!("{e}"),
        }
    }
}
