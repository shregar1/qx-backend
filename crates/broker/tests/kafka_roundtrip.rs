//! End-to-end: hand-rolled Kafka client produces records, lists offsets,
//! and fetches them back. Exercises Produce v9 → ListOffsets v7 → Fetch v12
//! over the same TCP connection — the closest a unit test can get to a
//! real kcat -P / kcat -C roundtrip.

use std::time::Duration;

use qx_broker::{Broker, KafkaConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---- wire helpers ----

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

fn flex_header(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(&api_key.to_be_bytes());
    h.extend_from_slice(&api_version.to_be_bytes());
    h.extend_from_slice(&correlation_id.to_be_bytes());
    h.extend_from_slice(&(4i16).to_be_bytes());
    h.extend_from_slice(b"test");
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

fn build_record_batch(records: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 0]); // attributes
    body.extend_from_slice(&((records.len() as i32 - 1).to_be_bytes())); // last_offset_delta
    body.extend_from_slice(&0i64.to_be_bytes());
    body.extend_from_slice(&0i64.to_be_bytes());
    body.extend_from_slice(&(-1i64).to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&(records.len() as i32).to_be_bytes());
    for (i, (k, v)) in records.iter().enumerate() {
        let mut rec = Vec::new();
        rec.push(0);
        write_zigzag(&mut rec, 0); // timestamp_delta
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
    let mut req = flex_header(0, 9, 2);
    write_uvarint(&mut req, 0); // transactional_id null
    req.extend_from_slice(&(1i16).to_be_bytes()); // acks
    req.extend_from_slice(&(30_000i32).to_be_bytes()); // timeout
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

fn build_list_offsets(topic: &str, partition: i32, timestamp: i64) -> Vec<u8> {
    let mut req = flex_header(2, 7, 3);
    req.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    req.push(0); // isolation_level
    write_uvarint(&mut req, 2);
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2);
    req.extend_from_slice(&partition.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch
    req.extend_from_slice(&timestamp.to_be_bytes());
    req.push(0); // partition tagged
    req.push(0); // topic tagged
    req.push(0); // req tagged
    req
}

fn build_fetch(topic: &str, partition: i32, offset: i64) -> Vec<u8> {
    let mut req = flex_header(1, 12, 4);
    req.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    req.extend_from_slice(&(0i32).to_be_bytes()); // max_wait_ms
    req.extend_from_slice(&(0i32).to_be_bytes()); // min_bytes
    req.extend_from_slice(&(1_048_576i32).to_be_bytes()); // max_bytes 1 MiB
    req.push(0); // isolation_level
    req.extend_from_slice(&(0i32).to_be_bytes()); // session_id
    req.extend_from_slice(&(0i32).to_be_bytes()); // session_epoch
    // topics
    write_uvarint(&mut req, 2);
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2);
    req.extend_from_slice(&partition.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes()); // current_leader_epoch
    req.extend_from_slice(&offset.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes()); // last_fetched_epoch
    req.extend_from_slice(&(0i64).to_be_bytes()); // log_start_offset
    req.extend_from_slice(&(1_048_576i32).to_be_bytes()); // partition_max_bytes
    req.push(0); // partition tagged
    req.push(0); // topic tagged
    // forgotten
    write_uvarint(&mut req, 1); // empty compact array (length 0 → uvarint 1)
    // rack_id
    write_compact_string(&mut req, "");
    req.push(0); // req tagged
    req
}

// ---- reader helpers ----

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
        buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3],
        buf[*pos + 4], buf[*pos + 5], buf[*pos + 6], buf[*pos + 7],
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
fn read_varint_zigzag(buf: &[u8], pos: &mut usize) -> i32 {
    let raw = read_uvarint(buf, pos);
    ((raw >> 1) as i32) ^ -((raw & 1) as i32)
}
fn read_compact_string(buf: &[u8], pos: &mut usize) -> String {
    let raw = read_uvarint(buf, pos);
    let len = (raw - 1) as usize;
    let s = std::str::from_utf8(&buf[*pos..*pos + len]).unwrap().to_string();
    *pos += len;
    s
}

// ---- test ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_list_offsets_fetch_roundtrip() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let cfg = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx".to_string(),
        node_id: 0,
        ..Default::default()
    };
    let kafka_handle = {
        let broker = broker.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

    // CreateTopics
    let _resp = frame_round_trip(&mut stream, &build_create_topics("rt", 1)).await;

    // Produce 3 records
    let batch = build_record_batch(&[(b"k1", b"hello"), (b"k2", b"world"), (b"k3", b"again")]);
    let resp = frame_round_trip(&mut stream, &build_produce("rt", 0, &batch)).await;
    // Parse correlation, drain
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _ = resp;

    // ListOffsets latest (-1)
    let resp = frame_round_trip(&mut stream, &build_list_offsets("rt", 0, -1)).await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 1);
    let _name = read_compact_string(&resp, &mut pos);
    let pcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(pcount, 1);
    let _idx = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0);
    let _ts = read_i64(&resp, &mut pos);
    let latest = read_i64(&resp, &mut pos);
    assert_eq!(latest, 3, "latest offset should be 3 after 3 records");

    // Fetch from offset 0
    let resp = frame_round_trip(&mut stream, &build_fetch("rt", 0, 0)).await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let _top_err = read_i16(&resp, &mut pos);
    let _session_id = read_i32(&resp, &mut pos);
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 1);
    let _name = read_compact_string(&resp, &mut pos);
    let pcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(pcount, 1);
    let _idx = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0, "fetch returned error {err}");
    let _high_water = read_i64(&resp, &mut pos);
    let _lso = read_i64(&resp, &mut pos);
    let _log_start = read_i64(&resp, &mut pos);
    // Empty compact array encodes as uvarint(0+1)=1.
    let aborted_raw = read_uvarint(&resp, &mut pos);
    assert_eq!(aborted_raw, 1, "aborted_transactions should encode as uvarint(1)");
    let _preferred = read_i32(&resp, &mut pos);

    // records field: compact_bytes = uvarint(len+1) + bytes
    let raw = read_uvarint(&resp, &mut pos);
    assert!(raw >= 2, "expected non-empty records section, got uvarint {raw}");
    let records_len = (raw - 1) as usize;
    let records_bytes = &resp[pos..pos + records_len];
    pos += records_len;
    let _tagged = read_uvarint(&resp, &mut pos);

    // Decode the record batch we got back.
    // base_offset(8) + batch_length(4) + leader_epoch(4) + magic(1) + crc(4) + body
    let mut bp = 0usize;
    let base_offset = read_i64(records_bytes, &mut bp);
    assert_eq!(base_offset, 0);
    let _batch_length = read_i32(records_bytes, &mut bp);
    let _leader_epoch = read_i32(records_bytes, &mut bp);
    let magic = records_bytes[bp];
    bp += 1;
    assert_eq!(magic, 2);
    let _crc = read_i32(records_bytes, &mut bp);
    let _attributes = read_i16(records_bytes, &mut bp);
    let _last_offset_delta = read_i32(records_bytes, &mut bp);
    let _base_ts = read_i64(records_bytes, &mut bp);
    let _max_ts = read_i64(records_bytes, &mut bp);
    let _producer_id = read_i64(records_bytes, &mut bp);
    let _producer_epoch = read_i16(records_bytes, &mut bp);
    let _base_seq = read_i32(records_bytes, &mut bp);
    let record_count = read_i32(records_bytes, &mut bp);
    assert_eq!(record_count, 3);

    let mut values = Vec::new();
    for _ in 0..record_count {
        let _record_len = read_varint_zigzag(records_bytes, &mut bp);
        let _attrs = records_bytes[bp];
        bp += 1;
        let _ts_delta = read_varint_zigzag(records_bytes, &mut bp);
        let _off_delta = read_varint_zigzag(records_bytes, &mut bp);
        let key_len = read_varint_zigzag(records_bytes, &mut bp);
        if key_len > 0 {
            bp += key_len as usize;
        }
        let val_len = read_varint_zigzag(records_bytes, &mut bp);
        let val: Vec<u8> = records_bytes[bp..bp + val_len as usize].to_vec();
        bp += val_len as usize;
        let header_count = read_varint_zigzag(records_bytes, &mut bp);
        for _ in 0..header_count {
            let hk_len = read_varint_zigzag(records_bytes, &mut bp);
            if hk_len > 0 {
                bp += hk_len as usize;
            }
            let hv_len = read_varint_zigzag(records_bytes, &mut bp);
            if hv_len > 0 {
                bp += hv_len as usize;
            }
        }
        values.push(val);
    }

    assert_eq!(values, vec![b"hello".to_vec(), b"world".to_vec(), b"again".to_vec()]);

    broker.shutdown().await;
    let _ = kafka_handle.await;
}
