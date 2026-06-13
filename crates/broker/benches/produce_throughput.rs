//! In-process produce throughput baseline.
//!
//! This bench bypasses the Kafka wire (`kafka_bench`) and the gRPC
//! transport, calling `Broker::produce_for_test` directly. It
//! measures the steady-state throughput of:
//!
//!   payload bytes → produce_writer → topic.send_to → storage commit
//!
//! i.e. everything the broker does *after* a request lands. The
//! number this produces is the upper bound any wire format can
//! ever approach on this machine — useful as the GiB/s baseline
//! when WAL / io_uring / per-core sharding work lands.
//!
//! Three payload sizes (1 KiB / 16 KiB / 256 KiB), each with two
//! batch sizes (1 record and 64 records per request) so we can
//! see how amortising the produce_writer round-trip across a
//! batch affects throughput. Run with:
//!
//!   cargo bench -p qx-broker --bench produce_throughput
//!
//! The results print as MB/s and ops/s.

use std::time::Duration;

use bytes::Bytes;
use criterion::{
    BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use qx_broker::{Broker, BrokerConfig};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Build a broker with one topic / four partitions on a fresh
/// tmp dir. Returns the broker plus the TempDir so the caller
/// keeps it alive for the bench's lifetime.
fn fresh_broker(_rt: &Runtime) -> (TempDir, std::sync::Arc<Broker>) {
    let tmp = TempDir::new().unwrap();
    let broker = Broker::open_with_config(tmp.path(), BrokerConfig::default());
    broker
        .open_topic_blocking("bench", 4)
        .expect("open bench topic");
    (tmp, broker)
}

fn bench_produce(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("in_process_produce");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    for &payload_bytes in &[1024usize, 16 * 1024, 256 * 1024] {
        for &batch_size in &[1usize, 64] {
            let id = BenchmarkId::from_parameter(format!(
                "{}_x_{}",
                human_bytes(payload_bytes),
                batch_size
            ));
            group.throughput(Throughput::Bytes((payload_bytes * batch_size) as u64));
            group.bench_with_input(id, &(payload_bytes, batch_size), |b, &(p, n)| {
                let (_tmp, broker) = fresh_broker(&rt);
                // Pre-build the payload vector once so per-iteration
                // cost is purely the produce path.
                let one = Bytes::from(vec![0u8; p]);
                let batch: Vec<Bytes> = (0..n).map(|_| one.clone()).collect();
                b.iter(|| {
                    let broker = broker.clone();
                    let batch = batch.clone();
                    rt.block_on(async move {
                        broker.produce_for_test("bench", 0, batch).await.unwrap();
                    });
                });
            });
        }
    }

    group.finish();
}

fn human_bytes(n: usize) -> String {
    if n >= 1024 * 1024 {
        format!("{}MiB", n / (1024 * 1024))
    } else if n >= 1024 {
        format!("{}KiB", n / 1024)
    } else {
        format!("{n}B")
    }
}

criterion_group!(benches, bench_produce);
criterion_main!(benches);
