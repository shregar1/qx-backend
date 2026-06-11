//! End-to-end demo: a real Kafka client (well, a hand-rolled one) speaks
//! the Kafka wire protocol to qx — `CreateTopics` to make the topic, then
//! `Produce` to publish a record — and the trigger dispatcher turns that
//! record into a workflow task.
//!
//! This is the "kafka producer pours into qx, workflow gets triggered"
//! moment. No glue code on the application side; the topic write *is* the
//! workflow trigger.

use std::time::Duration;

use qx_broker::{Broker, KafkaConfig, TriggerConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---- tiny Kafka wire client helpers (only what this test needs) ----

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
    // client_id = "test"
    h.extend_from_slice(&(4i16).to_be_bytes());
    h.extend_from_slice(b"test");
    h.push(0); // tagged_fields
    h
}

fn build_create_topics_request(name: &str, partitions: i32) -> Vec<u8> {
    let mut body = flex_header(19, 7, 1);
    write_uvarint(&mut body, 2); // 1 topic + 1 (compact array len)
    write_compact_string(&mut body, name);
    body.extend_from_slice(&partitions.to_be_bytes());
    body.extend_from_slice(&(1i16).to_be_bytes()); // replication_factor
    write_uvarint(&mut body, 0); // assignments: null
    write_uvarint(&mut body, 0); // configs: null
    body.push(0); // topic tagged_fields
    body.extend_from_slice(&(30_000i32).to_be_bytes()); // timeout_ms
    body.push(0); // validate_only = false
    body.push(0); // tagged_fields
    body
}

fn build_record_batch(records: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    // attributes (i16) = 0
    body.extend_from_slice(&[0, 0]);
    // last_offset_delta (i32)
    body.extend_from_slice(&((records.len() as i32 - 1).to_be_bytes()));
    // base_timestamp (i64), max_timestamp (i64)
    body.extend_from_slice(&0i64.to_be_bytes());
    body.extend_from_slice(&0i64.to_be_bytes());
    // producer_id (i64) = -1
    body.extend_from_slice(&(-1i64).to_be_bytes());
    // producer_epoch (i16) = -1
    body.extend_from_slice(&(-1i16).to_be_bytes());
    // base_sequence (i32) = -1
    body.extend_from_slice(&(-1i32).to_be_bytes());
    // record count (i32)
    body.extend_from_slice(&(records.len() as i32).to_be_bytes());

    for (i, (k, v)) in records.iter().enumerate() {
        let mut rec = Vec::new();
        rec.push(0); // attributes
        write_zigzag(&mut rec, 0); // timestamp_delta
        write_zigzag(&mut rec, i as i32); // offset_delta
        write_zigzag(&mut rec, k.len() as i32);
        rec.extend_from_slice(k);
        write_zigzag(&mut rec, v.len() as i32);
        rec.extend_from_slice(v);
        write_zigzag(&mut rec, 0); // header count
        write_zigzag(&mut body, rec.len() as i32);
        body.extend_from_slice(&rec);
    }

    let crc = crc32c::crc32c(&body);

    let mut batch = Vec::new();
    batch.extend_from_slice(&0i64.to_be_bytes()); // base_offset
    let batch_length = (4 + 1 + 4 + body.len()) as i32;
    batch.extend_from_slice(&batch_length.to_be_bytes());
    batch.extend_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch.push(2); // magic
    batch.extend_from_slice(&crc.to_be_bytes());
    batch.extend_from_slice(&body);
    batch
}

fn build_produce_request(topic: &str, partition: i32, batch: &[u8]) -> Vec<u8> {
    let mut req = flex_header(0, 9, 2);
    write_uvarint(&mut req, 0); // transactional_id: null
    req.extend_from_slice(&(1i16).to_be_bytes()); // acks = 1
    req.extend_from_slice(&(30_000i32).to_be_bytes()); // timeout_ms
    write_uvarint(&mut req, 2); // 1 topic
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2); // 1 partition
    req.extend_from_slice(&partition.to_be_bytes());
    write_uvarint(&mut req, batch.len() as u32 + 1); // compact_bytes length
    req.extend_from_slice(batch);
    req.push(0); // partition tagged_fields
    req.push(0); // topic tagged_fields
    req.push(0); // request tagged_fields
    req
}

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

fn read_uvarint(buf: &[u8], pos: &mut usize) -> u32 {
    let mut value: u32 = 0;
    let mut shift = 0u32;
    loop {
        let byte = buf[*pos];
        *pos += 1;
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

// ---- test ----

async fn wait_until<F>(mut f: F, deadline: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    f()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_produce_flows_into_workflow_via_trigger() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    // Boot the Kafka listener.
    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let cfg = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx".to_string(),
        node_id: 0,
        ..Default::default()
    };
    {
        let broker = broker.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

    // ---- CreateTopics: "events", 1 partition ----
    let create = build_create_topics_request("events", 1);
    let resp = frame_round_trip(&mut stream, &create).await;

    // Response header v1 (flexible): correlation_id + tagged_fields.
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 1);
    // We could parse the per-topic body here, but the simpler check is
    // that the broker's catalog now has the topic with the requested
    // partition count.
    let topics = broker.topic_names_and_counts();
    assert!(
        topics.iter().any(|(n, p)| n == "events" && *p == 1),
        "CreateTopics did not register the topic; got {:?}",
        topics
    );

    // Now register a trigger so produced records flow into a workflow.
    let mut trig = TriggerConfig::new("events-to-wf", "events", "handle_event");
    trig.poll_interval_ms = 25;
    broker.register_trigger(trig).unwrap();

    // ---- Produce: send one record to partition 0 ----
    let batch = build_record_batch(&[(b"k1", b"hello-from-kafka")]);
    let produce = build_produce_request("events", 0, &batch);
    let resp = frame_round_trip(&mut stream, &produce).await;

    // Check produce response had error_code 0 for our partition.
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 1);
    // name (compact_string)
    let name_len = read_uvarint(&resp, &mut pos) - 1;
    pos += name_len as usize;
    let partition_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(partition_count, 1);
    let _index = read_i32(&resp, &mut pos);
    let error_code = read_i16(&resp, &mut pos);
    assert_eq!(error_code, 0, "Produce returned error code {error_code}");

    // ---- Wait for trigger to land the workflow task ----
    let ok = wait_until(
        || {
            broker
                .workflow_payloads("handle_event", 1024)
                .map(|p| p.iter().any(|x| x == b"hello-from-kafka"))
                .unwrap_or(false)
        },
        Duration::from_secs(3),
    )
    .await;
    let payloads = broker.workflow_payloads("handle_event", 1024).unwrap();
    assert!(
        ok,
        "expected a workflow task carrying the produced record; got {:?}",
        payloads
    );
}
