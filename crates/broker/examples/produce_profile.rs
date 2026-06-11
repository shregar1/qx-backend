//! Profile harness: runs the same workload as `kafka_sustained_async`
//! for a fixed wall-clock duration, then dumps the cumulative
//! breakdown of where time was spent on the Produce hot path. No
//! criterion — direct timing, no statistical overhead. Useful when
//! the criterion bench's run-to-run noise is too large to see a
//! micro-optimization, or when you want to know where to optimize
//! before guessing.
//!
//! Run with:
//!   cargo run --release -p qx-broker --example produce_profile
//!
//! Tunable via env:
//!   QX_PROFILE_DURATION_S: how long to drive the broker for (default 8)
//!   QX_PROFILE_CLIENTS:    concurrent TCP clients (default 16)
//!   QX_PROFILE_PAYLOAD:    bytes per record (default 1024)
//!   QX_PROFILE_RECORDS:    records per Produce (default 100)
//!   QX_PROFILE_PARTITIONS: partition count in the topic (default 16)

use std::sync::Arc;
use std::time::{Duration, Instant};

use qx_broker::{Broker, BrokerConfig, BrokerMode, KafkaConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    // Enable the produce-path timing instrumentation. The handler
    // checks this once at first use via OnceLock; setting it before
    // the broker boots is enough.
    if std::env::var_os("QX_PRODUCE_INSTRUMENT").is_none() {
        // Edition 2024 marks `set_var` unsafe because process-wide
        // env mutation can race with concurrent readers. We run this
        // before any tokio runtime exists, so no reader yet.
        unsafe {
            std::env::set_var("QX_PRODUCE_INSTRUMENT", "1");
        }
    }
    let duration_s = env_u64("QX_PROFILE_DURATION_S", 8);
    let clients = env_u64("QX_PROFILE_CLIENTS", 16) as usize;
    let payload_size = env_u64("QX_PROFILE_PAYLOAD", 1024) as usize;
    let records_per_batch = env_u64("QX_PROFILE_RECORDS", 100) as usize;
    let partitions = env_u64("QX_PROFILE_PARTITIONS", 16) as i32;

    eprintln!(
        "qx produce-profile: {duration_s}s, clients={clients}, payload={payload_size}, records={records_per_batch}, partitions={partitions}"
    );

    let rt = Runtime::new().unwrap();
    let dir = TempDir::new().unwrap();
    let broker = Broker::open_with_config(
        dir.path(),
        BrokerConfig {
            mode: BrokerMode::Hyper,
            ..Default::default()
        },
    );

    let value = vec![b'q'; payload_size];
    let recs: Vec<(&[u8], &[u8])> = (0..records_per_batch)
        .map(|_| (b"k".as_ref(), value.as_slice()))
        .collect();
    let batch = build_record_batch(&recs);
    let frames: Vec<Vec<u8>> = (0..clients)
        .map(|c| build_produce("bench", (c as i32) % partitions, &batch))
        .collect();
    let frames = Arc::new(frames);

    let addr = rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = listener.local_addr().unwrap();
        let cfg = KafkaConfig {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: a.port() as i32,
            cluster_id: "qx-profile".to_string(),
            node_id: 0,
            ..Default::default()
        };
        let bc = broker.clone();
        tokio::spawn(async move {
            bc.serve_kafka_with_listener(listener, cfg).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create the topic.
        let mut s = TcpStream::connect(a).await.unwrap();
        let create = build_create_topics("bench", partitions);
        s.write_all(&(create.len() as i32).to_be_bytes()).await.unwrap();
        s.write_all(&create).await.unwrap();
        let mut size_buf = [0u8; 4];
        s.read_exact(&mut size_buf).await.unwrap();
        let mut resp = vec![0u8; i32::from_be_bytes(size_buf) as usize];
        s.read_exact(&mut resp).await.unwrap();
        a
    });

    // Pre-open all client streams.
    let mut streams: Vec<TcpStream> = (0..clients)
        .map(|_| rt.block_on(TcpStream::connect(addr)).unwrap())
        .collect();

    // Drive the workload for `duration_s` seconds.
    let deadline = Instant::now() + Duration::from_secs(duration_s);
    let mut iterations: u64 = 0;
    let mut bytes_sent: u64 = 0;
    let bytes_per_iter = (clients * records_per_batch * payload_size) as u64;

    rt.block_on(async {
        while Instant::now() < deadline {
            let mut joinset = tokio::task::JoinSet::new();
            for (i, mut s) in streams.drain(..).enumerate() {
                let frame = frames[i].clone();
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
            iterations += 1;
            bytes_sent += bytes_per_iter;
        }
    });

    let elapsed = deadline - Instant::now();
    let real_elapsed = Duration::from_secs(duration_s) - elapsed;
    let mib_per_s = (bytes_sent as f64 / real_elapsed.as_secs_f64()) / (1024.0 * 1024.0);
    eprintln!(
        "drove {iterations} iterations in {:.2}s — throughput: {mib_per_s:.1} MiB/s",
        real_elapsed.as_secs_f64()
    );

    qx_broker::dump_produce_timings();
}

// ---- minimal Kafka frame builders, mirrored from the bench ----

fn build_record_batch(records: &[(&[u8], &[u8])]) -> Vec<u8> {
    use std::convert::TryInto;
    let mut body = Vec::new();
    body.extend_from_slice(&0i16.to_be_bytes()); // attributes
    body.extend_from_slice(&((records.len() as i32) - 1).to_be_bytes()); // last_offset_delta
    body.extend_from_slice(&0i64.to_be_bytes()); // base_ts
    body.extend_from_slice(&0i64.to_be_bytes()); // max_ts
    body.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
    body.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    body.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    body.extend_from_slice(&(records.len() as i32).to_be_bytes()); // record_count

    for (i, (key, value)) in records.iter().enumerate() {
        let mut rec = Vec::new();
        rec.push(0); // attributes
        write_varlong(&mut rec, 0); // ts_delta
        write_varint(&mut rec, i as i32); // offset_delta
        write_varint(&mut rec, key.len() as i32);
        rec.extend_from_slice(key);
        write_varint(&mut rec, value.len() as i32);
        rec.extend_from_slice(value);
        write_varint(&mut rec, 0); // header_count
        write_varint(&mut body, rec.len() as i32);
        body.extend_from_slice(&rec);
    }

    let crc = crc32c::crc32c(&body);
    let mut out = Vec::new();
    out.extend_from_slice(&0i64.to_be_bytes()); // base_offset
    let batch_len: i32 = (4 + 1 + 4 + body.len()).try_into().unwrap(); // leader_epoch + magic + crc + body
    out.extend_from_slice(&batch_len.to_be_bytes());
    out.extend_from_slice(&(-1i32).to_be_bytes()); // partition_leader_epoch
    out.push(2); // magic
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn build_produce(topic: &str, partition: i32, batch: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    // request header v2 (flexible): key i16, version i16, correlation_id i32, client_id nullable_string, tagged_fields
    frame.extend_from_slice(&0i16.to_be_bytes()); // api_key = Produce
    frame.extend_from_slice(&9i16.to_be_bytes()); // version
    frame.extend_from_slice(&7i32.to_be_bytes()); // correlation_id
    frame.extend_from_slice(&(0i16).to_be_bytes()); // client_id = ""
    frame.push(0); // tagged_fields

    // body
    frame.push(0); // transactional_id null (compact_nullable_string)
    frame.extend_from_slice(&1i16.to_be_bytes()); // acks
    frame.extend_from_slice(&5000i32.to_be_bytes()); // timeout_ms
    // topics: compact_array of 1 entry
    write_uvarint(&mut frame, 2);
    write_compact_string(&mut frame, topic);
    write_uvarint(&mut frame, 2); // partitions count + 1
    frame.extend_from_slice(&partition.to_be_bytes());
    write_compact_bytes(&mut frame, Some(batch));
    frame.push(0); // tagged_fields partition
    frame.push(0); // tagged_fields topic
    frame.push(0); // tagged_fields request body
    frame
}

fn build_create_topics(topic: &str, partitions: i32) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&19i16.to_be_bytes()); // CreateTopics
    frame.extend_from_slice(&5i16.to_be_bytes()); // version
    frame.extend_from_slice(&1i32.to_be_bytes()); // correlation_id
    frame.extend_from_slice(&(0i16).to_be_bytes()); // client_id = ""
    frame.push(0); // tagged_fields
    write_uvarint(&mut frame, 2); // topics count
    write_compact_string(&mut frame, topic);
    frame.extend_from_slice(&partitions.to_be_bytes());
    frame.extend_from_slice(&1i16.to_be_bytes()); // replication_factor
    write_uvarint(&mut frame, 1); // assignments empty
    write_uvarint(&mut frame, 1); // configs empty
    frame.push(0); // tagged_fields topic
    frame.extend_from_slice(&60000i32.to_be_bytes()); // timeout
    frame.push(0); // validate_only = false
    frame.push(0); // tagged_fields
    frame
}

fn write_uvarint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
fn write_varint(out: &mut Vec<u8>, v: i32) {
    let zigzag = ((v << 1) ^ (v >> 31)) as u32;
    write_uvarint(out, zigzag);
}
fn write_varlong(out: &mut Vec<u8>, v: i64) {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    let mut x = zigzag;
    while x >= 0x80 {
        out.push((x as u8) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}
fn write_compact_string(out: &mut Vec<u8>, s: &str) {
    write_uvarint(out, s.len() as u32 + 1);
    out.extend_from_slice(s.as_bytes());
}
fn write_compact_bytes(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => write_uvarint(out, 0),
        Some(b) => {
            write_uvarint(out, b.len() as u32 + 1);
            out.extend_from_slice(b);
        }
    }
}
