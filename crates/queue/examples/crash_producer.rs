//! Producer side of the kill -9 crash-safety harness.
//!
//! Usage:
//!   crash_producer <queue_dir> <acks_log_path>
//!
//! Loops forever doing send → receive → ack. After every `ack` that
//! returns `Ok`, the offset is appended to `acks_log_path` and the
//! file is fsync'd. The acks log is the application's source of truth
//! for "this offset is durably acknowledged" — anything the queue
//! later re-delivers from such an offset is a durability bug.
//!
//! Crash model: the parent harness SIGKILLs this binary at a random
//! point. There is no signal handler; we are expected to die abruptly.
//! The queue is configured with full durability (`durable_send=true`,
//! `eventual_commits=false`) so the post-crash invariant is:
//!
//!     ∀ offset ∈ acks_log : offset must NOT appear in the queue.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::exit;
use std::time::Duration;

use qx_queue::{Queue, QueueConfig};
use qx_storage::PartitionConfig;

fn main() {
    let mut args = std::env::args().skip(1);
    let queue_dir = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: crash_producer <queue_dir> <acks_log_path>");
            exit(2);
        }
    };
    let acks_log_path = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: crash_producer <queue_dir> <acks_log_path>");
            exit(2);
        }
    };

    let cfg = QueueConfig {
        partition: PartitionConfig::default(),
        // 500 ms — long enough that the producer's own ack-loop never
        // races its own visibility timeout (a batch's worth of acks
        // takes single-digit ms), short enough that the crash-safety
        // harness can drain leftover InFlight rows after a brief
        // sleep instead of needing to wait minutes.
        visibility_timeout: Duration::from_millis(500),
        max_attempts: 100,
        durable_send: true,
        eventual_commits: false,
        track_message_state: true,
    };
    let mut q = Queue::open(&queue_dir, cfg).expect("open queue");

    let mut acks_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&acks_log_path)
        .expect("open acks log");

    let payload = vec![0xa5u8; 128];
    let batch_size = 50;
    let refs: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();

    let mut produced: u64 = 0;
    let mut acked: u64 = 0;
    let mut next_progress_tick: u64 = 500;

    loop {
        // Produce a batch — fully durable (log fsync + redb commit).
        q.send_batch(&refs).expect("send_batch");
        produced += batch_size as u64;

        // Receive what's available (may be more than we just produced if
        // earlier batches are still pending visibility).
        let batch = q.receive(batch_size).expect("receive");
        if batch.is_empty() {
            continue;
        }

        // Standard at-least-once consumer ordering:
        //   1) write_app_state(offset) durably (acks.log + fsync)
        //   2) queue.ack(receipt)
        // Crash between (1) and (2) leaves the offset both in
        // acks.log and re-deliverable from the queue — the consumer
        // must be idempotent. The test harness's invariant therefore
        // only requires `acks ∪ drained ∪ residual ⊇ {0..log_next}`
        // (no data loss); overlap between `acked` and `drained` is
        // allowed.
        for msg in batch {
            let offset = msg.id;
            writeln!(acks_log, "{offset}").expect("write ack offset");
            acks_log.flush().expect("flush acks log");
            acks_log.sync_all().expect("fsync acks log");
            q.ack(msg.receipt).expect("ack");
            acked += 1;
        }

        if acked >= next_progress_tick {
            println!("tick produced={produced} acked={acked}");
            // Let the parent know progress via stdout flush.
            std::io::stdout().flush().ok();
            next_progress_tick += 500;
        }
    }
}
