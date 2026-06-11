//! End-to-end test for the Kafka consumer-group surface: a single
//! consumer joins a group, syncs, heartbeats, commits an offset, and
//! reads it back.
//!
//! Wire formats exercised:
//!   FindCoordinator v3, JoinGroup v9, SyncGroup v5, Heartbeat v4,
//!   OffsetCommit v8, OffsetFetch v8, LeaveGroup v4.

use std::time::Duration;

use qx_broker::{Broker, KafkaConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---- wire helpers (same idioms as kafka_roundtrip.rs) ----

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
fn write_compact_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    write_uvarint(buf, b.len() as u32 + 1);
    buf.extend_from_slice(b);
}

fn flex_header(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(&api_key.to_be_bytes());
    h.extend_from_slice(&api_version.to_be_bytes());
    h.extend_from_slice(&correlation_id.to_be_bytes());
    h.extend_from_slice(&(6i16).to_be_bytes());
    h.extend_from_slice(b"client");
    h.push(0);
    h
}

// ---- request builders ----

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

fn build_find_coordinator(group: &str) -> Vec<u8> {
    let mut req = flex_header(10, 3, 2);
    write_compact_string(&mut req, group);
    req.push(0); // key_type = 0 (group)
    req.push(0); // tagged_fields
    req
}

fn build_join_group(group: &str, member_id: &str, metadata: &[u8]) -> Vec<u8> {
    let mut req = flex_header(11, 9, 3);
    write_compact_string(&mut req, group);
    req.extend_from_slice(&(10_000i32).to_be_bytes()); // session_timeout
    req.extend_from_slice(&(60_000i32).to_be_bytes()); // rebalance_timeout
    write_compact_string(&mut req, member_id);
    write_compact_nullable_string(&mut req, None); // group_instance_id
    write_compact_string(&mut req, "consumer");
    write_uvarint(&mut req, 2); // 1 protocol
    write_compact_string(&mut req, "range");
    write_compact_bytes(&mut req, metadata);
    req.push(0); // protocol tagged
    req.push(0); // request tagged
    req
}

fn build_sync_group(
    group: &str,
    generation_id: i32,
    member_id: &str,
    assignments: &[(&str, &[u8])],
) -> Vec<u8> {
    let mut req = flex_header(14, 5, 4);
    write_compact_string(&mut req, group);
    req.extend_from_slice(&generation_id.to_be_bytes());
    write_compact_string(&mut req, member_id);
    write_compact_nullable_string(&mut req, None);
    write_compact_nullable_string(&mut req, Some("consumer"));
    write_compact_nullable_string(&mut req, Some("range"));
    write_uvarint(&mut req, assignments.len() as u32 + 1);
    for (mid, asg) in assignments {
        write_compact_string(&mut req, mid);
        write_compact_bytes(&mut req, asg);
        req.push(0);
    }
    req.push(0); // request tagged
    req
}

fn build_heartbeat(group: &str, generation_id: i32, member_id: &str) -> Vec<u8> {
    let mut req = flex_header(12, 4, 5);
    write_compact_string(&mut req, group);
    req.extend_from_slice(&generation_id.to_be_bytes());
    write_compact_string(&mut req, member_id);
    write_compact_nullable_string(&mut req, None);
    req.push(0);
    req
}

fn build_offset_commit(
    group: &str,
    generation: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Vec<u8> {
    let mut req = flex_header(8, 8, 6);
    write_compact_string(&mut req, group);
    req.extend_from_slice(&generation.to_be_bytes());
    write_compact_string(&mut req, member_id);
    write_compact_nullable_string(&mut req, None);
    write_uvarint(&mut req, 2); // 1 topic
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2); // 1 partition
    req.extend_from_slice(&partition.to_be_bytes());
    req.extend_from_slice(&offset.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes()); // committed_leader_epoch
    write_compact_nullable_string(&mut req, None); // metadata
    req.push(0); // partition tagged
    req.push(0); // topic tagged
    req.push(0); // request tagged
    req
}

fn build_offset_fetch(group: &str, topic: &str, partition: i32) -> Vec<u8> {
    let mut req = flex_header(9, 8, 7);
    write_uvarint(&mut req, 2); // 1 group
    write_compact_string(&mut req, group);
    write_uvarint(&mut req, 2); // 1 topic
    write_compact_string(&mut req, topic);
    write_uvarint(&mut req, 2); // 1 partition
    req.extend_from_slice(&partition.to_be_bytes());
    req.push(0); // topic tagged
    req.push(0); // group tagged
    req.push(0); // require_stable = false
    req.push(0); // request tagged
    req
}

// ---- read helpers ----

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
fn read_compact_string(buf: &[u8], pos: &mut usize) -> String {
    let raw = read_uvarint(buf, pos);
    let len = (raw - 1) as usize;
    let s = std::str::from_utf8(&buf[*pos..*pos + len]).unwrap().to_string();
    *pos += len;
    s
}
fn read_compact_nullable_string(buf: &[u8], pos: &mut usize) -> Option<String> {
    let raw = read_uvarint(buf, pos);
    if raw == 0 {
        return None;
    }
    let len = (raw - 1) as usize;
    let s = std::str::from_utf8(&buf[*pos..*pos + len]).unwrap().to_string();
    *pos += len;
    Some(s)
}

// ---- the test ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_member_group_full_handshake() {
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
    let _ = frame_round_trip(&mut stream, &build_create_topics("ev", 2)).await;

    // --- FindCoordinator ---
    let resp = frame_round_trip(&mut stream, &build_find_coordinator("g1")).await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0);
    let _msg = read_compact_nullable_string(&resp, &mut pos);
    let node_id = read_i32(&resp, &mut pos);
    let host = read_compact_string(&resp, &mut pos);
    let port = read_i32(&resp, &mut pos);
    assert_eq!(node_id, 0);
    assert_eq!(host, "127.0.0.1");
    assert_eq!(port, kafka_addr.port() as i32);

    // --- JoinGroup (initial: empty member_id) ---
    let resp = frame_round_trip(
        &mut stream,
        &build_join_group("g1", "", b"opaque-metadata"),
    )
    .await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0, "JoinGroup error {err}");
    let generation = read_i32(&resp, &mut pos);
    assert!(generation >= 1);
    let _protocol_type = read_compact_nullable_string(&resp, &mut pos);
    let _protocol_name = read_compact_nullable_string(&resp, &mut pos);
    let leader = read_compact_string(&resp, &mut pos);
    let _skip = resp[pos];
    pos += 1;
    let my_member_id = read_compact_string(&resp, &mut pos);
    assert_eq!(leader, my_member_id, "single-member group: we're the leader");

    let member_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(member_count, 1, "single member returned to leader");

    // --- SyncGroup (we're the leader; assign ourselves both partitions) ---
    let assignment_blob = b"my-assignment-bytes"; // opaque to broker
    let resp = frame_round_trip(
        &mut stream,
        &build_sync_group(
            "g1",
            generation,
            &my_member_id,
            &[(my_member_id.as_str(), assignment_blob.as_ref())],
        ),
    )
    .await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0, "SyncGroup error {err}");
    let _ptype = read_compact_nullable_string(&resp, &mut pos);
    let _pname = read_compact_nullable_string(&resp, &mut pos);
    let raw = read_uvarint(&resp, &mut pos) - 1;
    let assigned: Vec<u8> = resp[pos..pos + raw as usize].to_vec();
    assert_eq!(assigned, assignment_blob.as_ref());

    // --- Heartbeat (should succeed since we're Stable) ---
    let resp = frame_round_trip(
        &mut stream,
        &build_heartbeat("g1", generation, &my_member_id),
    )
    .await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0, "Heartbeat error {err}");

    // --- OffsetCommit ---
    let resp = frame_round_trip(
        &mut stream,
        &build_offset_commit("g1", generation, &my_member_id, "ev", 0, 7),
    )
    .await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let tcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(tcount, 1);
    let _name = read_compact_string(&resp, &mut pos);
    let pcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(pcount, 1);
    let _pidx = read_i32(&resp, &mut pos);
    let err = read_i16(&resp, &mut pos);
    assert_eq!(err, 0, "OffsetCommit error {err}");

    // --- OffsetFetch: we should read back 7 ---
    let resp = frame_round_trip(&mut stream, &build_offset_fetch("g1", "ev", 0)).await;
    let mut pos = 0;
    let _corr = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let gcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(gcount, 1);
    let _gid = read_compact_string(&resp, &mut pos);
    let tcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(tcount, 1);
    let _name = read_compact_string(&resp, &mut pos);
    let pcount = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(pcount, 1);
    let _pidx = read_i32(&resp, &mut pos);
    let offset = read_i64(&resp, &mut pos);
    assert_eq!(offset, 7, "OffsetFetch should return what we committed");

    broker.shutdown().await;
    let _ = kafka_handle.await;
}

/// Two consumers join the same group concurrently. Because both arrive
/// inside the 200ms join window, the broker holds both JoinGroups parked
/// until the window closes; the deterministic leader (smallest member_id)
/// then sends a SyncGroup with assignments for both. The follower's
/// SyncGroup arrives later and the broker hands it the stashed slice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_members_complete_rebalance_together() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: addr.port() as i32,
        cluster_id: "qx".to_string(),
        node_id: 0,
        ..Default::default()
    };
    let kafka_handle = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(listener, cfg).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // CreateTopics + populate one connection's worth of state
    {
        let mut bootstrap = TcpStream::connect(addr).await.unwrap();
        let _ = frame_round_trip(&mut bootstrap, &build_create_topics("ev2", 2)).await;
    }

    // Each member runs in its own task with its own TCP connection.
    let join_a = tokio::spawn({
        async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let resp =
                frame_round_trip(&mut s, &build_join_group("g2", "", b"meta-A")).await;
            (parse_join_response(&resp), s)
        }
    });
    // Tiny stagger so we can tell which member arrives first (not strictly
    // required — the 200ms window covers both).
    tokio::time::sleep(Duration::from_millis(20)).await;
    let join_b = tokio::spawn({
        async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let resp =
                frame_round_trip(&mut s, &build_join_group("g2", "", b"meta-B")).await;
            (parse_join_response(&resp), s)
        }
    });

    let ((join_a_resp, mut stream_a), (join_b_resp, mut stream_b)) =
        tokio::try_join!(join_a, join_b).unwrap();

    assert_eq!(join_a_resp.error_code, 0, "A JoinGroup err {}", join_a_resp.error_code);
    assert_eq!(join_b_resp.error_code, 0, "B JoinGroup err {}", join_b_resp.error_code);
    assert_eq!(
        join_a_resp.generation_id, join_b_resp.generation_id,
        "both members should see the same generation"
    );
    assert_eq!(join_a_resp.leader, join_b_resp.leader);

    let leader_id = join_a_resp.leader.clone();
    // Whichever response came back as the leader will list 2 members.
    let leader_resp = if join_a_resp.member_id == leader_id {
        &join_a_resp
    } else {
        &join_b_resp
    };
    assert_eq!(
        leader_resp.member_count, 2,
        "leader should see both members"
    );

    // ---- SyncGroup phase ----
    // Spawn both SyncGroups concurrently. Leader sends an assignment map;
    // follower sends an empty one. The broker should respond to both with
    // each member's slice.
    let leader_is_a = join_a_resp.member_id == leader_id;
    let (leader_stream, leader_member_id, leader_resp_gen, follower_stream, follower_member_id, follower_resp_gen) =
        if leader_is_a {
            (
                stream_a,
                join_a_resp.member_id.clone(),
                join_a_resp.generation_id,
                stream_b,
                join_b_resp.member_id.clone(),
                join_b_resp.generation_id,
            )
        } else {
            (
                stream_b,
                join_b_resp.member_id.clone(),
                join_b_resp.generation_id,
                stream_a,
                join_a_resp.member_id.clone(),
                join_a_resp.generation_id,
            )
        };
    let mut leader_stream = leader_stream;
    let mut follower_stream = follower_stream;

    let leader_assignment: Vec<u8> = b"assignment-for-leader".to_vec();
    let follower_assignment: Vec<u8> = b"assignment-for-follower".to_vec();

    let leader_member_id_for_sync = leader_member_id.clone();
    let follower_member_id_for_sync = follower_member_id.clone();
    let l_a = leader_assignment.clone();
    let f_a = follower_assignment.clone();

    let leader_task = tokio::spawn(async move {
        let req = build_sync_group(
            "g2",
            leader_resp_gen,
            &leader_member_id_for_sync,
            &[
                (leader_member_id_for_sync.as_str(), l_a.as_slice()),
                (follower_member_id_for_sync.as_str(), f_a.as_slice()),
            ],
        );
        let resp = frame_round_trip(&mut leader_stream, &req).await;
        (parse_sync_assignment(&resp), leader_stream)
    });
    // Slight pause so the leader's sync arrives first; otherwise the
    // follower's gets parked and the leader's still completes both.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let follower_member_id_for_sync2 = follower_member_id.clone();
    let follower_task = tokio::spawn(async move {
        let req = build_sync_group("g2", follower_resp_gen, &follower_member_id_for_sync2, &[]);
        let resp = frame_round_trip(&mut follower_stream, &req).await;
        (parse_sync_assignment(&resp), follower_stream)
    });

    let ((leader_sync, mut leader_stream), (follower_sync, mut follower_stream)) =
        tokio::try_join!(leader_task, follower_task).unwrap();

    assert_eq!(leader_sync.error_code, 0, "leader sync err {}", leader_sync.error_code);
    assert_eq!(follower_sync.error_code, 0, "follower sync err {}", follower_sync.error_code);
    assert_eq!(leader_sync.assignment, leader_assignment);
    assert_eq!(follower_sync.assignment, follower_assignment);

    // ---- Both members heartbeat after rebalance — should succeed ----
    for (stream, generation, mid) in [
        (&mut leader_stream, leader_resp_gen, &leader_member_id),
        (&mut follower_stream, follower_resp_gen, &follower_member_id),
    ] {
        let resp = frame_round_trip(stream, &build_heartbeat("g2", generation, mid)).await;
        let mut pos = 0;
        let _corr = read_i32(&resp, &mut pos);
        let _tagged = read_uvarint(&resp, &mut pos);
        let _throttle = read_i32(&resp, &mut pos);
        let err = read_i16(&resp, &mut pos);
        assert_eq!(err, 0, "post-rebalance heartbeat err {err}");
    }

    broker.shutdown().await;
    let _ = kafka_handle.await;
}

#[derive(Debug)]
struct JoinResp {
    error_code: i16,
    generation_id: i32,
    leader: String,
    member_id: String,
    member_count: u32,
}

fn parse_join_response(resp: &[u8]) -> JoinResp {
    let mut pos = 0;
    let _corr = read_i32(resp, &mut pos);
    let _tagged = read_uvarint(resp, &mut pos);
    let _throttle = read_i32(resp, &mut pos);
    let error_code = read_i16(resp, &mut pos);
    let generation_id = read_i32(resp, &mut pos);
    let _ptype = read_compact_nullable_string(resp, &mut pos);
    let _pname = read_compact_nullable_string(resp, &mut pos);
    let leader = read_compact_string(resp, &mut pos);
    let _skip = resp[pos];
    pos += 1;
    let member_id = read_compact_string(resp, &mut pos);
    let member_count = read_uvarint(resp, &mut pos).saturating_sub(1);
    JoinResp {
        error_code,
        generation_id,
        leader,
        member_id,
        member_count,
    }
}

#[derive(Debug)]
struct SyncResp {
    error_code: i16,
    assignment: Vec<u8>,
}

fn parse_sync_assignment(resp: &[u8]) -> SyncResp {
    let mut pos = 0;
    let _corr = read_i32(resp, &mut pos);
    let _tagged = read_uvarint(resp, &mut pos);
    let _throttle = read_i32(resp, &mut pos);
    let error_code = read_i16(resp, &mut pos);
    let _ptype = read_compact_nullable_string(resp, &mut pos);
    let _pname = read_compact_nullable_string(resp, &mut pos);
    let raw = read_uvarint(resp, &mut pos);
    let len = (raw.saturating_sub(1)) as usize;
    let assignment = resp[pos..pos + len].to_vec();
    SyncResp { error_code, assignment }
}

fn read_compact_string_at(buf: &[u8], pos: &mut usize) -> String {
    read_compact_string(buf, pos)
}

#[derive(Debug)]
struct JoinRespWithProto {
    error_code: i16,
    generation_id: i32,
    protocol_name: Option<String>,
    leader: String,
    member_id: String,
    member_count: u32,
}

fn parse_join_response_with_protocol(resp: &[u8]) -> JoinRespWithProto {
    let mut pos = 0;
    let _corr = read_i32(resp, &mut pos);
    let _tagged = read_uvarint(resp, &mut pos);
    let _throttle = read_i32(resp, &mut pos);
    let error_code = read_i16(resp, &mut pos);
    let generation_id = read_i32(resp, &mut pos);
    let _ptype = read_compact_nullable_string(resp, &mut pos);
    let protocol_name = read_compact_nullable_string(resp, &mut pos);
    let leader = read_compact_string_at(resp, &mut pos);
    let _skip = resp[pos];
    pos += 1;
    let member_id = read_compact_string_at(resp, &mut pos);
    let member_count = read_uvarint(resp, &mut pos).saturating_sub(1);
    JoinRespWithProto {
        error_code,
        generation_id,
        protocol_name,
        leader,
        member_id,
        member_count,
    }
}

fn build_join_group_with_protocols(
    group: &str,
    member_id: &str,
    protocols: &[(&str, &[u8])],
) -> Vec<u8> {
    let mut req = flex_header(11, 9, 3);
    write_compact_string(&mut req, group);
    req.extend_from_slice(&(10_000i32).to_be_bytes()); // session_timeout
    req.extend_from_slice(&(60_000i32).to_be_bytes()); // rebalance_timeout
    write_compact_string(&mut req, member_id);
    write_compact_nullable_string(&mut req, None);
    write_compact_string(&mut req, "consumer");
    write_uvarint(&mut req, protocols.len() as u32 + 1);
    for (name, meta) in protocols {
        write_compact_string(&mut req, name);
        write_compact_bytes(&mut req, meta);
        req.push(0); // protocol tagged
    }
    req.push(0); // request tagged
    req
}

/// Cooperative-sticky negotiation: both members offer ("cooperative-sticky",
/// "range") in that priority order. The broker should pick
/// "cooperative-sticky" as the negotiated protocol because both members
/// support it and it's the leader's first preference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cooperative_sticky_protocol_is_negotiated() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: addr.port() as i32,
        cluster_id: "qx".to_string(),
        node_id: 0,
        ..Default::default()
    };
    let kafka_handle = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(listener, cfg).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    let protocols: &[(&str, &[u8])] =
        &[("cooperative-sticky", b"meta-coop"), ("range", b"meta-range")];

    let join_a = tokio::spawn({
        let protocols: Vec<(String, Vec<u8>)> = protocols
            .iter()
            .map(|(n, m)| (n.to_string(), m.to_vec()))
            .collect();
        async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let proto_refs: Vec<(&str, &[u8])> = protocols
                .iter()
                .map(|(n, m)| (n.as_str(), m.as_slice()))
                .collect();
            let req = build_join_group_with_protocols("coop-grp", "", &proto_refs);
            let resp = frame_round_trip(&mut s, &req).await;
            parse_join_response_with_protocol(&resp)
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let join_b = tokio::spawn({
        let protocols: Vec<(String, Vec<u8>)> = protocols
            .iter()
            .map(|(n, m)| (n.to_string(), m.to_vec()))
            .collect();
        async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let proto_refs: Vec<(&str, &[u8])> = protocols
                .iter()
                .map(|(n, m)| (n.as_str(), m.as_slice()))
                .collect();
            let req = build_join_group_with_protocols("coop-grp", "", &proto_refs);
            let resp = frame_round_trip(&mut s, &req).await;
            parse_join_response_with_protocol(&resp)
        }
    });

    let (a, b) = tokio::try_join!(join_a, join_b).unwrap();
    assert_eq!(a.error_code, 0);
    assert_eq!(b.error_code, 0);
    assert_eq!(
        a.protocol_name.as_deref(),
        Some("cooperative-sticky"),
        "expected cooperative-sticky to win; got {:?}",
        a.protocol_name
    );
    assert_eq!(b.protocol_name.as_deref(), Some("cooperative-sticky"));
    assert_eq!(a.generation_id, b.generation_id);
    assert_eq!(a.leader, b.leader);
    let _ = a.member_id;
    let _ = b.member_count;

    broker.shutdown().await;
    let _ = kafka_handle.await;
}

/// When members offer disjoint protocols (one only knows "range", another
/// only knows "roundrobin"), no negotiation is possible. The coordinator
/// falls back to no protocol; both members still get a response so they
/// can decide what to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disjoint_protocols_negotiate_nothing() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: addr.port() as i32,
        cluster_id: "qx".to_string(),
        node_id: 0,
        ..Default::default()
    };
    let kafka_handle = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(listener, cfg).await.unwrap();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    let join_a = tokio::spawn(async move {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let req = build_join_group_with_protocols("disjoint", "", &[("range", b"r")]);
        let resp = frame_round_trip(&mut s, &req).await;
        parse_join_response_with_protocol(&resp)
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let join_b = tokio::spawn(async move {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let req = build_join_group_with_protocols("disjoint", "", &[("roundrobin", b"rr")]);
        let resp = frame_round_trip(&mut s, &req).await;
        parse_join_response_with_protocol(&resp)
    });

    let (a, b) = tokio::try_join!(join_a, join_b).unwrap();
    assert_eq!(a.error_code, 0);
    assert_eq!(b.error_code, 0);
    assert_eq!(
        a.protocol_name, None,
        "disjoint protocols should leave protocol_name = None; got {:?}",
        a.protocol_name
    );
    assert_eq!(b.protocol_name, None);

    broker.shutdown().await;
    let _ = kafka_handle.await;
}

/// Group-committed offsets must survive a broker restart. We commit
/// offset 7 for group "g-restart" on topic "off", reboot, and check
/// OffsetFetch still returns 7 — by reading qx-topic's durable
/// group_offset table when the freshly-spawned coordinator's HashMap
/// is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_group_offsets_survive_restart() {
    let dir = TempDir::new().unwrap();

    // ---- boot 1: do CreateTopics → Join → Sync → OffsetCommit ----
    {
        let broker = Broker::open(dir.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = KafkaConfig {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: addr.port() as i32,
            cluster_id: "qx".to_string(),
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
        let mut s = TcpStream::connect(addr).await.unwrap();
        let _ = frame_round_trip(&mut s, &build_create_topics("off", 1)).await;
        let join =
            frame_round_trip(&mut s, &build_join_group("g-restart", "", b"meta")).await;
        let parsed = parse_join_response(&join);
        let member_id = parsed.member_id.clone();
        let generation = parsed.generation_id;
        // SyncGroup so we're Stable (Heartbeat requires it; OffsetCommit
        // strictly doesn't, but it's the realistic flow).
        let _ = frame_round_trip(
            &mut s,
            &build_sync_group(
                "g-restart",
                generation,
                &member_id,
                &[(member_id.as_str(), b"asg")],
            ),
        )
        .await;
        let commit_resp = frame_round_trip(
            &mut s,
            &build_offset_commit("g-restart", generation, &member_id, "off", 0, 7),
        )
        .await;
        // Inspect: top-level OK, per-partition error_code == 0
        {
            let mut pos = 0;
            let _corr = read_i32(&commit_resp, &mut pos);
            let _tagged = read_uvarint(&commit_resp, &mut pos);
            let _throttle = read_i32(&commit_resp, &mut pos);
            let tcount = read_uvarint(&commit_resp, &mut pos) - 1;
            assert_eq!(tcount, 1);
            let _name = read_compact_string(&commit_resp, &mut pos);
            let pcount = read_uvarint(&commit_resp, &mut pos) - 1;
            assert_eq!(pcount, 1);
            let _pidx = read_i32(&commit_resp, &mut pos);
            let err = read_i16(&commit_resp, &mut pos);
            assert_eq!(err, 0, "OffsetCommit returned error {err}");
        }

        // Also check via OffsetFetch on the SAME broker before shutdown.
        let pre = frame_round_trip(&mut s, &build_offset_fetch("g-restart", "off", 0)).await;
        let mut pos = 0;
        let _corr = read_i32(&pre, &mut pos);
        let _tagged = read_uvarint(&pre, &mut pos);
        let _throttle = read_i32(&pre, &mut pos);
        let _gcount = read_uvarint(&pre, &mut pos);
        let _gid = read_compact_string(&pre, &mut pos);
        let _tcount = read_uvarint(&pre, &mut pos);
        let _tname = read_compact_string(&pre, &mut pos);
        let _pcount = read_uvarint(&pre, &mut pos);
        let _pidx = read_i32(&pre, &mut pos);
        let off = read_i64(&pre, &mut pos);
        assert_eq!(off, 7, "OffsetFetch on same broker should see committed offset");

        drop(s);
        broker.shutdown().await;
        let _ = handle.await;
        drop(broker);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // ---- boot 2: OffsetFetch should find offset 7 via durable fallback ----
    {
        let broker = Broker::open(dir.path());
        // Verify topic discovery actually re-attached "off".
        let topics = broker.topic_names_and_counts();
        eprintln!("after-restart broker.topic_names_and_counts() = {:?}", topics);
        assert!(
            topics.iter().any(|(n, _)| n == "off"),
            "topic 'off' should be rediscovered on boot 2; got {:?}",
            topics
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = KafkaConfig {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: addr.port() as i32,
            cluster_id: "qx".to_string(),
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
        let mut s = TcpStream::connect(addr).await.unwrap();
        let resp = frame_round_trip(&mut s, &build_offset_fetch("g-restart", "off", 0)).await;

        // Parse: corr, tagged, throttle, groups_count, group_id, topics_count,
        //        topic_name, partition_count, partition_index, offset, ...
        let mut pos = 0;
        let _corr = read_i32(&resp, &mut pos);
        let _tagged = read_uvarint(&resp, &mut pos);
        let _throttle = read_i32(&resp, &mut pos);
        let gcount = read_uvarint(&resp, &mut pos) - 1;
        assert_eq!(gcount, 1);
        let _gid = read_compact_string(&resp, &mut pos);
        let tcount = read_uvarint(&resp, &mut pos) - 1;
        assert_eq!(tcount, 1);
        let _tname = read_compact_string(&resp, &mut pos);
        let pcount = read_uvarint(&resp, &mut pos) - 1;
        assert_eq!(pcount, 1);
        let _pidx = read_i32(&resp, &mut pos);
        let offset_after_restart = read_i64(&resp, &mut pos);
        assert_eq!(
            offset_after_restart, 7,
            "OffsetFetch after restart must surface the durable group offset"
        );

        broker.shutdown().await;
        let _ = handle.await;
    }
}
