//! End-to-end transactional semantics: produce inside a transaction,
//! verify READ_COMMITTED consumers see nothing, commit, verify the
//! records become visible; separately verify abort moves the offsets
//! to the partition's `aborted_transactions` list and READ_COMMITTED
//! consumers still see nothing.

use std::time::Duration;

use qx_broker::{Broker, KafkaConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---- tiny wire helpers ----

async fn frame_round_trip(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() as i32).to_be_bytes();
    stream.write_all(&len).await.unwrap();
    stream.write_all(payload).await.unwrap();
    stream.flush().await.unwrap();
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut out = vec![0u8; size];
    stream.read_exact(&mut out).await.unwrap();
    out
}

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
fn write_compact_nullable_string(buf: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => write_uvarint(buf, 0),
        Some(s) => write_compact_string(buf, s),
    }
}
fn flex_header(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(&api_key.to_be_bytes());
    h.extend_from_slice(&api_version.to_be_bytes());
    h.extend_from_slice(&correlation_id.to_be_bytes());
    h.extend_from_slice(&(2i16).to_be_bytes());
    h.extend_from_slice(b"tc");
    h.push(0);
    h
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

fn build_init_producer_id(transactional_id: &str) -> Vec<u8> {
    let mut req = flex_header(22, 4, 2);
    write_compact_nullable_string(&mut req, Some(transactional_id));
    req.extend_from_slice(&(60_000i32).to_be_bytes()); // transaction_timeout_ms
    req.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
    req.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    req.push(0); // tagged
    req
}

fn build_add_partitions_to_txn(
    txn_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topic: &str,
    partitions: &[i32],
) -> Vec<u8> {
    let mut req = flex_header(24, 3, 3);
    write_compact_string(&mut req, txn_id);
    req.extend_from_slice(&producer_id.to_be_bytes());
    req.extend_from_slice(&producer_epoch.to_be_bytes());
    write_uvarint(&mut req, 2); // 1 topic
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, partitions.len() as u32 + 1);
    for p in partitions {
        req.extend_from_slice(&p.to_be_bytes());
    }
    req.push(0); // topic tagged
    req.push(0); // request tagged
    req
}

fn build_end_txn(txn_id: &str, producer_id: i64, producer_epoch: i16, committed: bool) -> Vec<u8> {
    let mut req = flex_header(26, 3, 4);
    write_compact_string(&mut req, txn_id);
    req.extend_from_slice(&producer_id.to_be_bytes());
    req.extend_from_slice(&producer_epoch.to_be_bytes());
    req.push(if committed { 1 } else { 0 });
    req.push(0); // tagged
    req
}

fn build_record_batch_txn(
    records: &[(&[u8], &[u8])],
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Vec<u8> {
    let attributes: i16 = 0x10; // bit 4 = isTransactional
    let mut body = Vec::new();
    body.extend_from_slice(&attributes.to_be_bytes());
    body.extend_from_slice(&((records.len() as i32 - 1).to_be_bytes()));
    body.extend_from_slice(&0i64.to_be_bytes()); // base_timestamp
    body.extend_from_slice(&0i64.to_be_bytes()); // max_timestamp
    body.extend_from_slice(&producer_id.to_be_bytes());
    body.extend_from_slice(&producer_epoch.to_be_bytes());
    body.extend_from_slice(&base_sequence.to_be_bytes());
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

fn build_produce(topic: &str, partition: i32, transactional_id: &str, batch: &[u8]) -> Vec<u8> {
    let mut req = flex_header(0, 9, 5);
    write_compact_nullable_string(&mut req, Some(transactional_id));
    req.extend_from_slice(&(1i16).to_be_bytes()); // acks
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

fn build_fetch(topic: &str, partition: i32, offset: i64, isolation_level: i8) -> Vec<u8> {
    let mut req = flex_header(1, 12, 6);
    req.extend_from_slice(&(-1i32).to_be_bytes());
    req.extend_from_slice(&(0i32).to_be_bytes());
    req.extend_from_slice(&(0i32).to_be_bytes());
    req.extend_from_slice(&(1_048_576i32).to_be_bytes());
    req.push(isolation_level as u8);
    req.extend_from_slice(&(0i32).to_be_bytes());
    req.extend_from_slice(&(0i32).to_be_bytes());
    write_uvarint(&mut req, 2);
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2);
    req.extend_from_slice(&partition.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes());
    req.extend_from_slice(&offset.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes());
    req.extend_from_slice(&(0i64).to_be_bytes());
    req.extend_from_slice(&(1_048_576i32).to_be_bytes());
    req.push(0);
    req.push(0);
    write_uvarint(&mut req, 1);
    write_compact_string(&mut req, "");
    req.push(0);
    req
}

// ---- response readers ----

fn read_i16(buf: &[u8], pos: &mut usize) -> i16 {
    let v = i16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    v
}
fn read_i32(buf: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    v
}
fn read_i64(buf: &[u8], pos: &mut usize) -> i64 {
    let v = i64::from_be_bytes([
        buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3], buf[*pos + 4], buf[*pos + 5],
        buf[*pos + 6], buf[*pos + 7],
    ]);
    *pos += 8;
    v
}
fn read_uvarint(buf: &[u8], pos: &mut usize) -> u32 {
    let mut value: u32 = 0;
    let mut shift = 0u32;
    loop {
        let b = buf[*pos];
        *pos += 1;
        value |= ((b & 0x7f) as u32) << shift;
        if b & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}
fn read_compact_string(buf: &[u8], pos: &mut usize) -> String {
    let raw = read_uvarint(buf, pos);
    let len = (raw - 1) as usize;
    let s = std::str::from_utf8(&buf[*pos..*pos + len]).unwrap().to_string();
    *pos += len;
    s
}

struct FetchView {
    high_watermark: i64,
    last_stable_offset: i64,
    aborted: Vec<(i64, i64)>, // (producer_id, first_offset)
    records_len: usize,
}

fn parse_fetch_partition(resp: &[u8]) -> FetchView {
    let mut pos = 0;
    let _corr = read_i32(resp, &mut pos);
    let _tagged = read_uvarint(resp, &mut pos);
    let _throttle = read_i32(resp, &mut pos);
    let _top_err = read_i16(resp, &mut pos);
    let _session_id = read_i32(resp, &mut pos);
    let topic_count = read_uvarint(resp, &mut pos) - 1;
    assert_eq!(topic_count, 1);
    let _name = read_compact_string(resp, &mut pos);
    let pcount = read_uvarint(resp, &mut pos) - 1;
    assert_eq!(pcount, 1);
    let _idx = read_i32(resp, &mut pos);
    let err = read_i16(resp, &mut pos);
    assert_eq!(err, 0, "fetch err {err}");
    let high_watermark = read_i64(resp, &mut pos);
    let last_stable_offset = read_i64(resp, &mut pos);
    let _log_start = read_i64(resp, &mut pos);
    let aborted_raw = read_uvarint(resp, &mut pos);
    let aborted_count = aborted_raw.saturating_sub(1);
    let mut aborted = Vec::new();
    for _ in 0..aborted_count {
        let pid = read_i64(resp, &mut pos);
        let first_off = read_i64(resp, &mut pos);
        let _at = read_uvarint(resp, &mut pos);
        aborted.push((pid, first_off));
    }
    let _preferred = read_i32(resp, &mut pos);
    let recs_raw = read_uvarint(resp, &mut pos);
    let records_len = recs_raw.saturating_sub(1) as usize;
    FetchView {
        high_watermark,
        last_stable_offset,
        aborted,
        records_len,
    }
}

fn parse_init_producer_id(resp: &[u8]) -> (i64, i16) {
    let mut pos = 0;
    let _corr = read_i32(resp, &mut pos);
    let _tagged = read_uvarint(resp, &mut pos);
    let _throttle = read_i32(resp, &mut pos);
    let err = read_i16(resp, &mut pos);
    assert_eq!(err, 0);
    let pid = read_i64(resp, &mut pos);
    let epoch = read_i16(resp, &mut pos);
    (pid, epoch)
}

// ---- the test ----

async fn boot() -> (
    std::sync::Arc<Broker>,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let dir = TempDir::new().unwrap();
    let (broker, addr, handle) = boot_in(&dir).await;
    std::mem::forget(dir);
    (broker, addr, handle)
}

async fn boot_in(
    dir: &TempDir,
) -> (
    std::sync::Arc<Broker>,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let broker = Broker::open(dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: addr.port() as i32,
        cluster_id: "qx-txn".to_string(),
        node_id: 0,
        ..Default::default()
    };
    let handle = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(listener, cfg).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    (broker, addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_commit_makes_records_visible_to_read_committed() {
    let (broker, addr, handle) = boot().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    let _ = frame_round_trip(&mut s, &build_create_topics("tx", 1)).await;

    let (pid, epoch) =
        parse_init_producer_id(&frame_round_trip(&mut s, &build_init_producer_id("t-commit")).await);
    let _ = frame_round_trip(
        &mut s,
        &build_add_partitions_to_txn("t-commit", pid, epoch, "tx", &[0]),
    )
    .await;

    let batch = build_record_batch_txn(&[(b"k", b"hello-txn")], pid, epoch, 0);
    let _ = frame_round_trip(&mut s, &build_produce("tx", 0, "t-commit", &batch)).await;

    // READ_COMMITTED before commit: LSO should pin at 0 → empty.
    let resp = frame_round_trip(&mut s, &build_fetch("tx", 0, 0, 1)).await;
    let view = parse_fetch_partition(&resp);
    assert_eq!(view.high_watermark, 1);
    assert_eq!(view.last_stable_offset, 0, "LSO must hide in-flight records");
    assert_eq!(view.records_len, 0, "no records should leak to READ_COMMITTED");

    // READ_UNCOMMITTED: still see the record.
    let resp = frame_round_trip(&mut s, &build_fetch("tx", 0, 0, 0)).await;
    let view = parse_fetch_partition(&resp);
    assert!(view.records_len > 0, "READ_UNCOMMITTED should see the record");

    // Commit.
    let _ = frame_round_trip(&mut s, &build_end_txn("t-commit", pid, epoch, true)).await;

    // READ_COMMITTED post-commit: LSO advances to high_watermark.
    let resp = frame_round_trip(&mut s, &build_fetch("tx", 0, 0, 1)).await;
    let view = parse_fetch_partition(&resp);
    assert_eq!(view.last_stable_offset, view.high_watermark);
    assert!(view.records_len > 0, "post-commit READ_COMMITTED should see it");
    assert!(view.aborted.is_empty());

    broker.shutdown().await;
    let _ = handle.await;
}

/// Restart-resilience: open a transaction, produce inside it, shut the
/// broker down without committing, reopen on the same data_dir, and
/// confirm READ_COMMITTED still hides the in-flight records. Then commit
/// against the reopened broker (same producer_id/epoch) and confirm the
/// records become visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_state_persists_across_broker_restart() {
    let dir = TempDir::new().unwrap();

    let (pid, epoch);
    // ---- boot 1 ----
    {
        let (broker, addr, handle) = boot_in(&dir).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        let _ = frame_round_trip(&mut s, &build_create_topics("tr", 1)).await;

        let init = frame_round_trip(&mut s, &build_init_producer_id("t-restart")).await;
        let (pid_local, epoch_local) = parse_init_producer_id(&init);
        pid = pid_local;
        epoch = epoch_local;
        let _ = frame_round_trip(
            &mut s,
            &build_add_partitions_to_txn("t-restart", pid, epoch, "tr", &[0]),
        )
        .await;
        let batch =
            build_record_batch_txn(&[(b"k", b"survives-restart")], pid, epoch, 0);
        let _ = frame_round_trip(&mut s, &build_produce("tr", 0, "t-restart", &batch)).await;

        // Before restart: READ_COMMITTED hides the record.
        let resp = frame_round_trip(&mut s, &build_fetch("tr", 0, 0, 1)).await;
        let view = parse_fetch_partition(&resp);
        assert_eq!(view.last_stable_offset, 0);
        assert_eq!(view.records_len, 0);

        drop(s);
        broker.shutdown().await;
        let _ = handle.await;
        drop(broker);
    }

    // Generous pause to let macOS reclaim file locks released by tokio
    // tasks that aren't tracked by Broker::shutdown (per-connection
    // handlers — they exit, but the runtime doesn't reclaim them
    // synchronously).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ---- boot 2: same data_dir, same in-flight transaction ----
    {
        let (broker, addr, handle) = boot_in(&dir).await;
        let mut s = TcpStream::connect(addr).await.unwrap();

        // Read the topic back through Kafka. The records persisted via
        // qx-storage; the txn registry persisted via redb.
        let resp = frame_round_trip(&mut s, &build_fetch("tr", 0, 0, 1)).await;
        let view = parse_fetch_partition(&resp);
        assert_eq!(view.high_watermark, 1, "record should still be in the log");
        assert_eq!(
            view.last_stable_offset, 0,
            "LSO must stay at 0 after restart — txn was still open"
        );
        assert_eq!(view.records_len, 0, "READ_COMMITTED should still hide it");

        // Commit using the same producer_id/epoch we got pre-restart.
        // (Real producers would re-init, but the registry's persisted
        // state should accept the original epoch since we never bumped it.)
        let resp =
            frame_round_trip(&mut s, &build_end_txn("t-restart", pid, epoch, true)).await;
        let mut pos = 0;
        let _corr = read_i32(&resp, &mut pos);
        let _tagged = read_uvarint(&resp, &mut pos);
        let _throttle = read_i32(&resp, &mut pos);
        let err = read_i16(&resp, &mut pos);
        assert_eq!(err, 0, "EndTxn after restart should succeed; got {err}");

        // READ_COMMITTED now sees the record.
        let resp = frame_round_trip(&mut s, &build_fetch("tr", 0, 0, 1)).await;
        let view = parse_fetch_partition(&resp);
        assert_eq!(view.last_stable_offset, view.high_watermark);
        assert!(view.records_len > 0);

        broker.shutdown().await;
        let _ = handle.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_abort_surfaces_in_aborted_transactions() {
    let (broker, addr, handle) = boot().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    let _ = frame_round_trip(&mut s, &build_create_topics("ta", 1)).await;

    let (pid, epoch) =
        parse_init_producer_id(&frame_round_trip(&mut s, &build_init_producer_id("t-abort")).await);
    let _ = frame_round_trip(
        &mut s,
        &build_add_partitions_to_txn("t-abort", pid, epoch, "ta", &[0]),
    )
    .await;

    let batch = build_record_batch_txn(&[(b"k", b"will-abort")], pid, epoch, 0);
    let _ = frame_round_trip(&mut s, &build_produce("ta", 0, "t-abort", &batch)).await;

    let _ = frame_round_trip(&mut s, &build_end_txn("t-abort", pid, epoch, false)).await;

    let resp = frame_round_trip(&mut s, &build_fetch("ta", 0, 0, 1)).await;
    let view = parse_fetch_partition(&resp);
    assert_eq!(view.last_stable_offset, view.high_watermark);
    assert_eq!(
        view.aborted,
        vec![(pid, 0i64)],
        "aborted_transactions should surface the producer's offset range"
    );

    broker.shutdown().await;
    let _ = handle.await;
}
