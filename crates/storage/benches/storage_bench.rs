//! Criterion benches for qx-storage. Run with `cargo bench`.
//!
//! Three groups:
//!   - `append/<bytes>`     : steady-state append throughput, no fsync
//!   - `append_sync/<bytes>`: append followed by fsync_data (durable path)
//!   - `read/<bytes>`       : random-access read of pre-populated log
//!   - `reopen/<records>`   : cold-open scan cost (recovery)

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use qx_storage::{PartitionConfig, PartitionLog};
use tempfile::tempdir;

fn fresh_log(max_segment_bytes: u64) -> (tempfile::TempDir, PartitionLog) {
    let tmp = tempdir().unwrap();
    let cfg = PartitionConfig { max_segment_bytes };
    let log = PartitionLog::open(tmp.path(), cfg).unwrap();
    (tmp, log)
}

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");
    for &size in &[16usize, 256, 1024, 4096] {
        let payload = vec![0x42u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            let (_tmp, mut log) = fresh_log(1 << 34);
            b.iter(|| log.append(&payload).unwrap());
        });
    }
    group.finish();
}

fn bench_append_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_sync");
    for &size in &[256usize, 4096] {
        let payload = vec![0x42u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            let (_tmp, mut log) = fresh_log(1 << 34);
            b.iter(|| {
                log.append(&payload).unwrap();
                log.sync().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    for &size in &[16usize, 256, 1024] {
        let payload = vec![0x42u8; size];
        let (_tmp, mut log) = fresh_log(1 << 34);
        for _ in 0..10_000 {
            log.append(&payload).unwrap();
        }
        let n = log.len();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            let mut i: u64 = 0;
            b.iter(|| {
                let r = log.read(i % n).unwrap();
                i = i.wrapping_add(1);
                r
            });
        });
    }
    group.finish();
}

fn bench_reopen(c: &mut Criterion) {
    let mut group = c.benchmark_group("reopen");
    for &n in &[1_000u32, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let tmp = tempdir().unwrap();
                    {
                        let mut log =
                            PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
                        let payload = vec![0u8; 64];
                        for _ in 0..n {
                            log.append(&payload).unwrap();
                        }
                        log.sync().unwrap();
                    }
                    let start = Instant::now();
                    let _log =
                        PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

fn bench_append_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_batch");
    for &batch_size in &[10usize, 100, 1000] {
        for &payload_size in &[64usize, 1024] {
            let payloads: Vec<Vec<u8>> =
                (0..batch_size).map(|_| vec![0x42u8; payload_size]).collect();
            let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
            group.throughput(Throughput::Bytes((batch_size * payload_size) as u64));
            let id = BenchmarkId::new(format!("{batch_size}x{payload_size}"), payload_size);
            group.bench_with_input(id, &(), |b, _| {
                let (_tmp, mut log) = fresh_log(1 << 34);
                b.iter(|| log.append_batch(&refs).unwrap());
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_append,
    bench_append_batch,
    bench_append_sync,
    bench_read,
    bench_reopen
);
criterion_main!(benches);
