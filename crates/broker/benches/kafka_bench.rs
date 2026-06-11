//! End-to-end Kafka-wire produce throughput benchmark.
//!
//! Boots a real broker on an ephemeral port, opens one TCP connection,
//! pre-builds a Produce v9 request, and measures how fast we can
//! round-trip it. The numbers here include the full broker pipeline:
//! decode header → decode body → CRC32C verify → topic.send_to →
//! encode response.
//!
//! Run with `cargo bench -p qx-broker`.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use qx_broker::{Broker, KafkaConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

fn write_uvarint(buf: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        buf.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn write_zigzag(buf: &mut Vec<u8>, v: i32) {
    write_uvarint(buf, ((v << 1) ^ (v >> 31)) as u32);
}

fn write_compact_string(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u32 + 1);
    buf.extend_from_slice(s.as_bytes());
}

fn flex_header(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(&api_key.to_be_bytes());
    h.extend_from_slice(&api_version.to_be_bytes());
    h.extend_from_slice(&correlation_id.to_be_bytes());
    h.extend_from_slice(&(5i16).to_be_bytes());
    h.extend_from_slice(b"bench");
    h.push(0);
    h
}

fn build_record_batch(records: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 0]); // attributes
    body.extend_from_slice(&((records.len() as i32 - 1).to_be_bytes()));
    body.extend_from_slice(&0i64.to_be_bytes());
    body.extend_from_slice(&0i64.to_be_bytes());
    body.extend_from_slice(&(-1i64).to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&(records.len() as i32).to_be_bytes());
    for (i, (k, v)) in records.iter().enumerate() {
        let mut rec = Vec::new();
        rec.push(0);
        write_zigzag(&mut rec, 0);
        write_zigzag(&mut rec, i as i32);
        write_zigzag(&mut rec, k.len() as i32);
        rec.extend_from_slice(k);
        write_zigzag(&mut rec, v.len() as i32);
        rec.extend_from_slice(v);
        write_zigzag(&mut rec, 0);
        write_zigzag(&mut body, rec.len() as i32);
        body.extend_from_slice(&rec);
    }
    let crc = crc32c::crc32c(&body);
    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes());
    batch.extend_from_slice(&((4 + 1 + 4 + body.len()) as i32).to_be_bytes());
    batch.extend_from_slice(&0i32.to_be_bytes());
    batch.push(2);
    batch.extend_from_slice(&crc.to_be_bytes());
    batch.extend_from_slice(&body);
    batch
}

fn build_produce(topic: &str, partition: i32, batch: &[u8]) -> Vec<u8> {
    let mut req = flex_header(0, 9, 1);
    write_uvarint(&mut req, 0); // transactional_id null
    req.extend_from_slice(&(1i16).to_be_bytes());
    req.extend_from_slice(&(30_000i32).to_be_bytes());
    write_uvarint(&mut req, 2);
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2);
    req.extend_from_slice(&partition.to_be_bytes());
    write_uvarint(&mut req, batch.len() as u32 + 1);
    req.extend_from_slice(batch);
    req.push(0);
    req.push(0);
    req.push(0);
    req
}

/// Build one Produce request that targets `partitions` partitions of
/// `topic`, each with the same record batch.
fn build_produce_multi_partition(topic: &str, partitions: i32, batch: &[u8]) -> Vec<u8> {
    let mut req = flex_header(0, 9, 1);
    write_uvarint(&mut req, 0);
    req.extend_from_slice(&(1i16).to_be_bytes());
    req.extend_from_slice(&(30_000i32).to_be_bytes());
    write_uvarint(&mut req, 2); // 1 topic
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, partitions as u32 + 1); // N partitions
    for p in 0..partitions {
        req.extend_from_slice(&p.to_be_bytes());
        write_uvarint(&mut req, batch.len() as u32 + 1);
        req.extend_from_slice(batch);
        req.push(0); // partition tagged
    }
    req.push(0); // topic tagged
    req.push(0); // request tagged
    req
}

fn build_create_topics(name: &str, partitions: i32) -> Vec<u8> {
    let mut req = flex_header(19, 7, 1);
    write_uvarint(&mut req, 2);
    write_compact_string(&mut req, name);
    req.extend_from_slice(&partitions.to_be_bytes());
    req.extend_from_slice(&(1i16).to_be_bytes());
    write_uvarint(&mut req, 0);
    write_uvarint(&mut req, 0);
    req.push(0);
    req.extend_from_slice(&(30_000i32).to_be_bytes());
    req.push(0);
    req.push(0);
    req
}

fn build_fetch(topic: &str, partition: i32, offset: i64, max_bytes: i32) -> Vec<u8> {
    let mut req = flex_header(1, 12, 4);
    req.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    req.extend_from_slice(&(0i32).to_be_bytes()); // max_wait_ms
    req.extend_from_slice(&(0i32).to_be_bytes()); // min_bytes
    req.extend_from_slice(&max_bytes.to_be_bytes());
    req.push(0); // isolation_level
    req.extend_from_slice(&(0i32).to_be_bytes()); // session_id
    req.extend_from_slice(&(0i32).to_be_bytes()); // session_epoch
    write_uvarint(&mut req, 2);
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2);
    req.extend_from_slice(&partition.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch
    req.extend_from_slice(&offset.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes()); // last_fetched_epoch
    req.extend_from_slice(&(0i64).to_be_bytes()); // log_start_offset
    req.extend_from_slice(&max_bytes.to_be_bytes()); // partition_max_bytes
    req.push(0); // partition tagged
    req.push(0); // topic tagged
    write_uvarint(&mut req, 1); // forgotten: empty
    write_compact_string(&mut req, ""); // rack_id
    req.push(0); // req tagged
    req
}

struct Setup {
    _broker: std::sync::Arc<Broker>,
    _dir: TempDir,
    rt: Runtime,
    stream: TcpStream,
}

fn boot() -> Setup {
    boot_with(false)
}

fn boot_with(eventual: bool) -> Setup {
    let rt = Runtime::new().unwrap();
    let dir = TempDir::new().unwrap();
    let broker = Broker::open_with_config(
        dir.path(),
        qx_broker::BrokerConfig {
            eventual_commits: eventual,
            ..Default::default()
        },
    );

    let stream = rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = KafkaConfig {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: addr.port() as i32,
            cluster_id: "qx-bench".to_string(),
            node_id: 0,
            ..Default::default()
        };
        let broker_clone = broker.clone();
        tokio::spawn(async move {
            broker_clone.serve_kafka_with_listener(listener, cfg).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut s = TcpStream::connect(addr).await.unwrap();
        let create = build_create_topics("bench", 1);
        s.write_all(&(create.len() as i32).to_be_bytes()).await.unwrap();
        s.write_all(&create).await.unwrap();
        let mut size_buf = [0u8; 4];
        s.read_exact(&mut size_buf).await.unwrap();
        let mut resp = vec![0u8; i32::from_be_bytes(size_buf) as usize];
        s.read_exact(&mut resp).await.unwrap();
        s
    });

    Setup {
        _broker: broker,
        _dir: dir,
        rt,
        stream,
    }
}

fn bench_produce_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_produce");
    for &(records_per_batch, payload_size) in &[
        (1usize, 64usize),
        (1, 1024),
        (100, 64),
        (100, 1024),
    ] {
        let value = vec![b'q'; payload_size];
        let recs: Vec<(&[u8], &[u8])> = (0..records_per_batch)
            .map(|_| (b"k".as_ref(), value.as_slice()))
            .collect();
        let batch = build_record_batch(&recs);
        let frame = build_produce("bench", 0, &batch);
        let total_bytes = (records_per_batch * payload_size) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        let id = BenchmarkId::new(
            "records-x-payload",
            format!("{records_per_batch}x{payload_size}"),
        );
        group.bench_function(id, |b| {
            // Fresh broker per parameter combination so the topic log doesn't
            // grow without bound across criterion's iterations.
            let mut setup = boot();
            let Setup { rt, stream, .. } = &mut setup;
            let len_prefix = (frame.len() as i32).to_be_bytes();
            b.iter(|| {
                rt.block_on(async {
                    stream.write_all(&len_prefix).await.unwrap();
                    stream.write_all(&frame).await.unwrap();
                    let mut size_buf = [0u8; 4];
                    stream.read_exact(&mut size_buf).await.unwrap();
                    let size = i32::from_be_bytes(size_buf) as usize;
                    let mut resp = vec![0u8; size];
                    stream.read_exact(&mut resp).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

/// Stress: 16 concurrent producers, each sending a large batch to one of
/// 16 partitions in an `eventual_commits` broker. Tests whether the
/// combined data path (TCP → decode → group-commit writer × 16 →
/// no-fsync redb commit → response) can sustain near-bandwidth
/// throughput when the fsync floor is removed.
fn bench_sustained_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_sustained_async");
    for &(clients, payload_per_record) in
        &[(8usize, 1024usize), (16, 1024), (16, 4096), (32, 4096)]
    {
        let value = vec![b'q'; payload_per_record];
        // 100 records per Produce, packed into a single batch.
        let recs: Vec<(&[u8], &[u8])> = (0..100)
            .map(|_| (b"k".as_ref(), value.as_slice()))
            .collect();
        let batch = build_record_batch(&recs);
        let frames: Vec<Vec<u8>> = (0..clients)
            .map(|c| build_produce("bench", (c % 16) as i32, &batch))
            .collect();
        let frames_arc = std::sync::Arc::new(frames);
        let bytes_per_iter = (clients * 100 * payload_per_record) as u64;
        group.throughput(Throughput::Bytes(bytes_per_iter));
        let id = BenchmarkId::new(
            "clients-x-payload",
            format!("{clients}x{payload_per_record}"),
        );
        group.bench_function(id, move |b| {
            let rt = Runtime::new().unwrap();
            let dir = TempDir::new().unwrap();
            // Hyper mode auto-applies eventual_commits + log_only_storage,
            // matching what we'd set manually in Standard mode for the
            // same throughput class. Lets the bench express the "use the
            // fast profile" intent in one line.
            let broker = Broker::open_with_config(
                dir.path(),
                qx_broker::BrokerConfig {
                    mode: qx_broker::BrokerMode::Hyper,
                    ..Default::default()
                },
            );
            let addr = rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let cfg = KafkaConfig {
                    advertised_host: "127.0.0.1".to_string(),
                    advertised_port: addr.port() as i32,
                    cluster_id: "qx-bench".to_string(),
                    node_id: 0,
                    ..Default::default()
                };
                let broker_clone = broker.clone();
                tokio::spawn(async move {
                    broker_clone
                        .serve_kafka_with_listener(listener, cfg)
                        .await
                        .unwrap();
                });
                tokio::time::sleep(Duration::from_millis(50)).await;
                // 16 partitions
                let mut s = TcpStream::connect(addr).await.unwrap();
                let create = build_create_topics("bench", 16);
                s.write_all(&(create.len() as i32).to_be_bytes()).await.unwrap();
                s.write_all(&create).await.unwrap();
                let mut size_buf = [0u8; 4];
                s.read_exact(&mut size_buf).await.unwrap();
                let mut resp = vec![0u8; i32::from_be_bytes(size_buf) as usize];
                s.read_exact(&mut resp).await.unwrap();
                addr
            });
            // Pre-open client streams.
            let mut streams: Vec<TcpStream> = (0..clients)
                .map(|_| rt.block_on(TcpStream::connect(addr)).unwrap())
                .collect();
            b.iter(|| {
                rt.block_on(async {
                    let mut joinset = tokio::task::JoinSet::new();
                    for (i, mut s) in streams.drain(..).enumerate() {
                        let frame = frames_arc[i].clone();
                        joinset.spawn(async move {
                            s.write_all(&(frame.len() as i32).to_be_bytes())
                                .await
                                .unwrap();
                            s.write_all(&frame).await.unwrap();
                            let mut size_buf = [0u8; 4];
                            s.read_exact(&mut size_buf).await.unwrap();
                            let size = i32::from_be_bytes(size_buf) as usize;
                            let mut resp = vec![0u8; size];
                            s.read_exact(&mut resp).await.unwrap();
                            s
                        });
                    }
                    while let Some(s) = joinset.join_next().await {
                        streams.push(s.unwrap());
                    }
                });
            });
        });
    }
    group.finish();
}

fn bench_produce_eventual(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_produce_eventual");
    for &(records_per_batch, payload_size) in &[(1usize, 64usize), (1, 1024), (100, 1024)] {
        let value = vec![b'q'; payload_size];
        let recs: Vec<(&[u8], &[u8])> = (0..records_per_batch)
            .map(|_| (b"k".as_ref(), value.as_slice()))
            .collect();
        let batch = build_record_batch(&recs);
        let frame = build_produce("bench", 0, &batch);
        let total_bytes = (records_per_batch * payload_size) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        let id = BenchmarkId::new(
            "records-x-payload",
            format!("{records_per_batch}x{payload_size}"),
        );
        group.bench_function(id, |b| {
            let mut setup = boot_with(true);
            let Setup { rt, stream, .. } = &mut setup;
            let len_prefix = (frame.len() as i32).to_be_bytes();
            b.iter(|| {
                rt.block_on(async {
                    stream.write_all(&len_prefix).await.unwrap();
                    stream.write_all(&frame).await.unwrap();
                    let mut size_buf = [0u8; 4];
                    stream.read_exact(&mut size_buf).await.unwrap();
                    let size = i32::from_be_bytes(size_buf) as usize;
                    let mut resp = vec![0u8; size];
                    stream.read_exact(&mut resp).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

/// Boot a broker, create a topic with N partitions, return the full setup.
fn boot_with_partitions(partition_count: i32, topic: &str) -> Setup {
    let rt = Runtime::new().unwrap();
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let stream = rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = KafkaConfig {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: addr.port() as i32,
            cluster_id: "qx-bench".to_string(),
            node_id: 0,
            ..Default::default()
        };
        let broker_clone = broker.clone();
        tokio::spawn(async move {
            broker_clone.serve_kafka_with_listener(listener, cfg).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut s = TcpStream::connect(addr).await.unwrap();
        let create = build_create_topics(topic, partition_count);
        s.write_all(&(create.len() as i32).to_be_bytes()).await.unwrap();
        s.write_all(&create).await.unwrap();
        let mut size_buf = [0u8; 4];
        s.read_exact(&mut size_buf).await.unwrap();
        let mut resp = vec![0u8; i32::from_be_bytes(size_buf) as usize];
        s.read_exact(&mut resp).await.unwrap();
        s
    });

    Setup {
        _broker: broker,
        _dir: dir,
        rt,
        stream,
    }
}

fn produce_blocking(
    rt: &Runtime,
    stream: &mut TcpStream,
    frame: &[u8],
) {
    rt.block_on(async {
        stream.write_all(&(frame.len() as i32).to_be_bytes()).await.unwrap();
        stream.write_all(frame).await.unwrap();
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut resp = vec![0u8; size];
        stream.read_exact(&mut resp).await.unwrap();
    });
}

/// Fetch latency: pre-populate the topic with M records of fixed size,
/// then time `Fetch` round-trips that pull them all back from offset 0.
fn bench_fetch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_fetch");
    for &(records_in_topic, payload_size) in &[
        (100usize, 64usize),
        (100, 1024),
        (1000, 64),
        (1000, 1024),
    ] {
        let value = vec![b'q'; payload_size];
        let recs: Vec<(&[u8], &[u8])> = (0..records_in_topic)
            .map(|_| (b"k".as_ref(), value.as_slice()))
            .collect();
        let batch = build_record_batch(&recs);
        let produce_frame = build_produce("bench", 0, &batch);
        let fetch_frame = build_fetch("bench", 0, 0, 8 * 1024 * 1024);
        let total_bytes = (records_in_topic * payload_size) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        let id = BenchmarkId::new(
            "records-x-payload",
            format!("{records_in_topic}x{payload_size}"),
        );
        group.bench_function(id, |b| {
            let mut setup = boot_with_partitions(1, "bench");
            let Setup { rt, stream, .. } = &mut setup;
            // Populate: one large Produce containing all records.
            produce_blocking(rt, stream, &produce_frame);
            let fetch_len = (fetch_frame.len() as i32).to_be_bytes();
            b.iter(|| {
                rt.block_on(async {
                    stream.write_all(&fetch_len).await.unwrap();
                    stream.write_all(&fetch_frame).await.unwrap();
                    let mut size_buf = [0u8; 4];
                    stream.read_exact(&mut size_buf).await.unwrap();
                    let size = i32::from_be_bytes(size_buf) as usize;
                    let mut resp = vec![0u8; size];
                    stream.read_exact(&mut resp).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

/// Concurrent producers: spawn N independent TCP clients, each
/// pumping a 100×1KiB Produce in a tight loop. Criterion measures
/// total time to land K Produce round-trips across all clients.
fn bench_concurrent_producers(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_concurrent");
    for &client_count in &[1usize, 4, 16] {
        let value = vec![b'q'; 1024];
        let recs: Vec<(&[u8], &[u8])> = (0..100)
            .map(|_| (b"k".as_ref(), value.as_slice()))
            .collect();
        let batch = build_record_batch(&recs);
        let frame = build_produce("bench", 0, &batch);
        let frame_arc = std::sync::Arc::new(frame);
        let bytes_per_iter = (100 * 1024 * client_count) as u64;
        group.throughput(Throughput::Bytes(bytes_per_iter));
        let id = BenchmarkId::new("clients", client_count.to_string());
        group.bench_function(id, move |b| {
            let rt = Runtime::new().unwrap();
            let dir = TempDir::new().unwrap();
            let broker = Broker::open(dir.path());
            let addr = rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let cfg = KafkaConfig {
                    advertised_host: "127.0.0.1".to_string(),
                    advertised_port: addr.port() as i32,
                    cluster_id: "qx-bench".to_string(),
                    node_id: 0,
                    ..Default::default()
                };
                let broker_clone = broker.clone();
                tokio::spawn(async move {
                    broker_clone
                        .serve_kafka_with_listener(listener, cfg)
                        .await
                        .unwrap();
                });
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut s = TcpStream::connect(addr).await.unwrap();
                let create = build_create_topics("bench", 1);
                s.write_all(&(create.len() as i32).to_be_bytes()).await.unwrap();
                s.write_all(&create).await.unwrap();
                let mut size_buf = [0u8; 4];
                s.read_exact(&mut size_buf).await.unwrap();
                let mut resp = vec![0u8; i32::from_be_bytes(size_buf) as usize];
                s.read_exact(&mut resp).await.unwrap();
                addr
            });
            // Pre-open N streams so the bench measures Produce, not connect cost.
            let mut streams: Vec<TcpStream> = (0..client_count)
                .map(|_| rt.block_on(TcpStream::connect(addr)).unwrap())
                .collect();
            b.iter(|| {
                rt.block_on(async {
                    let mut joinset = tokio::task::JoinSet::new();
                    for mut s in streams.drain(..) {
                        let frame = frame_arc.clone();
                        joinset.spawn(async move {
                            s.write_all(&(frame.len() as i32).to_be_bytes())
                                .await
                                .unwrap();
                            s.write_all(&frame).await.unwrap();
                            let mut size_buf = [0u8; 4];
                            s.read_exact(&mut size_buf).await.unwrap();
                            let size = i32::from_be_bytes(size_buf) as usize;
                            let mut resp = vec![0u8; size];
                            s.read_exact(&mut resp).await.unwrap();
                            s
                        });
                    }
                    while let Some(res) = joinset.join_next().await {
                        streams.push(res.expect("bench task panicked"));
                    }
                });
            });
            // Keep broker alive until the closure ends.
            let _ = &broker;
            let _ = &dir;
        });
    }
    group.finish();
}

/// Multi-partition fan-out: same total payload split across 1, 4, or 16
/// partitions. Measures whether per-partition state (mutex, queue,
/// state_db) is a contention point.
fn bench_partition_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_partitions");
    for &partition_count in &[1i32, 4, 16] {
        let value = vec![b'q'; 1024];
        // 100 records, distributed round-robin across partitions.
        let recs: Vec<(&[u8], &[u8])> = (0..100)
            .map(|_| (b"k".as_ref(), value.as_slice()))
            .collect();
        let batch = build_record_batch(&recs);
        // Pre-build one Produce per partition; iter sends all of them.
        let frames: Vec<Vec<u8>> = (0..partition_count)
            .map(|p| build_produce("bench", p, &batch))
            .collect();
        let bytes_per_iter = (100 * 1024 * partition_count as usize) as u64;
        group.throughput(Throughput::Bytes(bytes_per_iter));
        let id = BenchmarkId::new("partitions", partition_count.to_string());
        group.bench_function(id, move |b| {
            let mut setup = boot_with_partitions(partition_count, "bench");
            let Setup { rt, stream, .. } = &mut setup;
            b.iter(|| {
                rt.block_on(async {
                    for frame in &frames {
                        stream
                            .write_all(&(frame.len() as i32).to_be_bytes())
                            .await
                            .unwrap();
                        stream.write_all(frame).await.unwrap();
                        let mut size_buf = [0u8; 4];
                        stream.read_exact(&mut size_buf).await.unwrap();
                        let size = i32::from_be_bytes(size_buf) as usize;
                        let mut resp = vec![0u8; size];
                        stream.read_exact(&mut resp).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

/// One Produce request body that hits N partitions. This is the
/// librdkafka pattern (batch records across partitions, ship in one
/// Produce). Tests whether the handler dispatches to all partition
/// writers in parallel.
fn bench_multi_partition_in_one_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_multi_partition_in_one_produce");
    let value = vec![b'q'; 1024];
    let recs: Vec<(&[u8], &[u8])> = (0..100)
        .map(|_| (b"k".as_ref(), value.as_slice()))
        .collect();
    let batch = build_record_batch(&recs);

    for &partition_count in &[1i32, 4, 16] {
        let frame = build_produce_multi_partition("bench", partition_count, &batch);
        let bytes_per_iter = (100 * 1024 * partition_count as usize) as u64;
        group.throughput(Throughput::Bytes(bytes_per_iter));
        let id = BenchmarkId::new("partitions", partition_count.to_string());
        group.bench_function(id, |b| {
            let mut setup = boot_with_partitions(partition_count, "bench");
            let Setup { rt, stream, .. } = &mut setup;
            let len_prefix = (frame.len() as i32).to_be_bytes();
            b.iter(|| {
                rt.block_on(async {
                    stream.write_all(&len_prefix).await.unwrap();
                    stream.write_all(&frame).await.unwrap();
                    let mut size_buf = [0u8; 4];
                    stream.read_exact(&mut size_buf).await.unwrap();
                    let size = i32::from_be_bytes(size_buf) as usize;
                    let mut resp = vec![0u8; size];
                    stream.read_exact(&mut resp).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_produce_throughput,
    bench_produce_eventual,
    bench_sustained_async,
    bench_fetch_throughput,
    bench_concurrent_producers,
    bench_partition_fanout,
    bench_multi_partition_in_one_produce,
);
criterion_main!(benches);
