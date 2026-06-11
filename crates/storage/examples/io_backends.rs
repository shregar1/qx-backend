//! Standalone benchmark: append + fsync throughput, sync vs io_uring.
//!
//! On Linux this binary runs four workloads, each against a fresh
//! tempdir so no file-cache state leaks between sync and io_uring:
//!
//!   - sync_append_fsync_per_record    write + fsync per record
//!   - sync_append_batched_fsync       N writes then one fsync
//!   - uring_append_fsync_per_record   io_uring write + fsync per record
//!   - uring_append_batched_fsync      io_uring N writes then one fsync
//!
//! On any non-Linux target only the sync workloads run.
//!
//! Invocation:
//!   cargo run -p qx-storage --release --example io_backends
//!   BENCH_RUNS=5 cargo run --release -p qx-storage --example io_backends
//!
//! ## What we found
//!
//! Three runs inside Docker Desktop's Linux VM (kernel 6.10, aarch64,
//! virtio-backed filesystem), 5_000 single-fsync ops and 500 batched
//! 100-fsync ops per workload:
//!
//! | workload                       |  sync avg | uring avg | ratio |
//! |--------------------------------|----------:|----------:|------:|
//! | append + fsync per record      |   413 op/s|   371 op/s|  0.90 |
//! | append + fsync per 100 records | 26,902 op/s| 24,047 op/s|  0.89 |
//!
//! io_uring is **not measurably faster** than `std::fs` + `sync_data`
//! in this environment — within the noise band of the underlying
//! virtio-fs / fsync cost. An earlier "2× faster" reading was a
//! cache-locality artefact from reusing the same file path across
//! sync and uring runs; this version isolates them per tempdir.
//!
//! What this does NOT establish: that io_uring is useless on bare-
//! metal Linux + NVMe. The fsync floor in Docker Desktop is set by
//! the virtio passthrough, not by the userspace I/O API; on real
//! hardware the syscall-batching benefit of io_uring may surface.
//! The honest call: **don't integrate io_uring into PartitionLog
//! until a bare-metal Linux + NVMe bench shows it actually wins.**
//! Slice 1's range-encoding work already eliminated the redb-commit
//! bottleneck that PERF.md flagged as the 172 MiB/s ceiling, so the
//! next bottleneck — wherever it lands — needs to be measured before
//! choosing a backend.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

fn report(label: &str, n: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let ops = (n as f64) / secs;
    let us_per_op = (secs * 1e6) / (n as f64);
    println!(
        "  {:<40} {:>10} ops  {:>7.3}s  =>  {:>10.0} ops/s  ({:>7.2} us/op)",
        label, n, secs, ops, us_per_op
    );
}

fn sync_append_fsync_per_record(path: &Path, payload: &[u8], n: u64) {
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open sync file");
    let t = Instant::now();
    for _ in 0..n {
        f.write_all(payload).expect("write");
        f.sync_data().expect("fsync");
    }
    report("sync append + fsync per record", n, t.elapsed());
}

fn sync_append_batched_fsync(path: &Path, payload: &[u8], n: u64, batch: u64) {
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open sync file");
    let t = Instant::now();
    let mut written = 0u64;
    while written < n {
        let take = batch.min(n - written);
        for _ in 0..take {
            f.write_all(payload).expect("write");
        }
        f.sync_data().expect("fsync");
        written += take;
    }
    report(
        &format!("sync append + fsync per {batch} records"),
        n,
        t.elapsed(),
    );
}

#[cfg(target_os = "linux")]
fn uring_append_fsync_per_record(path: &Path, payload: &[u8], n: u64) {
    // Single-record submission: one write SQE, wait for completion,
    // one fsync SQE, wait. No batching. Exposes the per-syscall floor
    // of io_uring relative to plain `write`/`fsync`.
    let f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open uring file");
    let fd = f.as_raw_fd();

    let mut ring = IoUring::new(8).expect("create io_uring");
    let mut offset: u64 = 0;
    let t = Instant::now();
    for _ in 0..n {
        // SAFETY: the buffer outlives the submission/wait cycle below;
        // we don't return until the kernel completes the entry.
        let write_e = opcode::Write::new(types::Fd(fd), payload.as_ptr(), payload.len() as u32)
            .offset(offset)
            .build()
            .user_data(0);
        unsafe {
            ring.submission()
                .push(&write_e)
                .expect("push write sqe");
        }
        ring.submit_and_wait(1).expect("submit write");
        let cqe = ring.completion().next().expect("write cqe");
        let written = cqe.result();
        assert!(written >= 0, "write returned {written}");
        assert_eq!(written as usize, payload.len(), "short write");
        offset += payload.len() as u64;

        // fsync via io_uring (FSYNC opcode with FDATASYNC flag).
        let fsync_e = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(1);
        unsafe {
            ring.submission()
                .push(&fsync_e)
                .expect("push fsync sqe");
        }
        ring.submit_and_wait(1).expect("submit fsync");
        let cqe = ring.completion().next().expect("fsync cqe");
        assert_eq!(cqe.result(), 0, "fsync failed: {}", cqe.result());
    }
    report("io_uring append + fsync per record", n, t.elapsed());
}

#[cfg(target_os = "linux")]
fn uring_append_batched_fsync(path: &Path, payload: &[u8], n: u64, batch: u64) {
    // Submit `batch` writes back-to-back into the ring, wait for all of
    // them, then submit one fsync. This is where io_uring is supposed
    // to shine: many SQEs in flight, one round-trip into the kernel
    // per batch.
    let f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open uring file");
    let fd = f.as_raw_fd();

    // Ring depth ≥ batch + 1 so we can queue all writes plus the fsync.
    let depth = (batch as u32 + 1).max(8);
    let mut ring = IoUring::new(depth).expect("create io_uring");

    let mut offset: u64 = 0;
    let mut written_records = 0u64;
    let t = Instant::now();
    while written_records < n {
        let take = batch.min(n - written_records);
        for _ in 0..take {
            let write_e =
                opcode::Write::new(types::Fd(fd), payload.as_ptr(), payload.len() as u32)
                    .offset(offset)
                    .build()
                    .user_data(0);
            unsafe {
                ring.submission()
                    .push(&write_e)
                    .expect("push write sqe");
            }
            offset += payload.len() as u64;
        }
        // Drain all the write CQEs.
        ring.submit_and_wait(take as usize).expect("submit writes");
        let mut drained = 0usize;
        while let Some(cqe) = ring.completion().next() {
            assert!(
                cqe.result() >= 0,
                "write failed in batch: {}",
                cqe.result()
            );
            drained += 1;
        }
        assert_eq!(drained, take as usize, "drained {drained}, expected {take}");

        // One fsync for the whole batch.
        let fsync_e = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(1);
        unsafe {
            ring.submission()
                .push(&fsync_e)
                .expect("push fsync sqe");
        }
        ring.submit_and_wait(1).expect("submit fsync");
        let cqe = ring.completion().next().expect("fsync cqe");
        assert_eq!(cqe.result(), 0, "fsync failed: {}", cqe.result());
        written_records += take;
    }
    report(
        &format!("io_uring append + fsync per {batch} records"),
        n,
        t.elapsed(),
    );
}

fn main() {
    let payload = vec![0xa5u8; 256];
    // Each pair of workloads writes to its own fresh tempdir so the
    // sync run can't get a hot-cache advantage from the prior
    // io_uring run (or vice versa). Filesystem caching is the single
    // biggest source of noise in this benchmark, especially on Docker
    // Desktop's virtio-backed filesystem where the first write to a
    // file is multiple times slower than subsequent writes.
    let runs: u32 = std::env::var("BENCH_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    println!("io_backends benchmark — payload {} bytes", payload.len());
    println!("platform: {}", std::env::consts::OS);
    println!("runs per workload: {runs}");

    for run in 0..runs {
        println!("\n=== run {} of {} ===", run + 1, runs);
        let tmp = tempfile::tempdir().expect("tempdir");
        sync_append_fsync_per_record(&tmp.path().join("sync.log"), &payload, 5_000);
        sync_append_batched_fsync(&tmp.path().join("sync_batch.log"), &payload, 50_000, 100);
        #[cfg(target_os = "linux")]
        {
            uring_append_fsync_per_record(&tmp.path().join("uring.log"), &payload, 5_000);
            uring_append_batched_fsync(&tmp.path().join("uring_batch.log"), &payload, 50_000, 100);
        }
        #[cfg(not(target_os = "linux"))]
        {
            println!("  (io_uring paths skipped — Linux only)");
        }
    }
}
