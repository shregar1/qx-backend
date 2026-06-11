//! End-to-end test for the Kafka wire-protocol surface.
//!
//! Boots a real `Broker`, opens a topic, binds the Kafka listener on an
//! ephemeral port, and round-trips `ApiVersions` + `Metadata` over a raw
//! TCP socket. No client library — we hand-assemble the request bytes so
//! we know the broker is speaking the spec, not just round-tripping its
//! own encoder.

use std::time::Duration;

use qx_broker::{Broker, KafkaConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Send a length-prefixed frame and read the next length-prefixed frame back.
async fn round_trip(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
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

/// Build a flexible request header (v2): api_key, api_version, correlation_id,
/// client_id (nullable_string), tagged_fields (uvarint 0).
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

fn compact_string(s: &str) -> Vec<u8> {
    let mut out = encode_uvarint(s.len() as u32 + 1);
    out.extend_from_slice(s.as_bytes());
    out
}

fn encode_uvarint(mut v: u32) -> Vec<u8> {
    let mut out = Vec::new();
    while v >= 0x80 {
        out.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    out
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_and_metadata_round_trip() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    // Open a topic so Metadata has something to advertise.
    {
        let req = qx_broker::proto::OpenTopicRequest {
            name: "events".to_string(),
            partition_count: 3,
        };
        // Use the gRPC trait-impl by going through the public Client?
        // Easier: just call get_or_open via a tiny helper indirection.
        // We don't have a public helper, so spin up the broker's gRPC
        // listener and reuse it. But that's heavy — let's just create
        // the topic directly via Topic API and force the broker to see
        // it on next metadata call.
        let _ = req; // silence unused
    }

    // Easiest reliable way to land a topic in the broker's catalog: spin
    // up the gRPC server, use the Client to OpenTopic.
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_with_listener(grpc_listener).await.unwrap();
        });
    }
    let mut grpc_client =
        qx_broker::Client::connect(format!("http://{grpc_addr}")).await.unwrap();
    grpc_client.open_topic("events", 3).await.unwrap();

    // Now boot the Kafka listener.
    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let kafka_config = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx-test".to_string(),
        node_id: 0,
        ..Default::default()
    };
    {
        let broker = broker.clone();
        let cfg = kafka_config.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        });
    }
    // Tiny grace period for the listener to be ready.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

    // ---- ApiVersions v3 ----
    let mut req = flex_header(18, 3, 42);
    req.extend_from_slice(&compact_string("test-client"));
    req.extend_from_slice(&compact_string("0.1.0"));
    req.push(0); // tagged_fields

    let resp = round_trip(&mut stream, &req).await;
    // Response header for ApiVersions is v0: just correlation_id.
    let mut pos = 0;
    let correlation_id = read_i32(&resp, &mut pos);
    assert_eq!(correlation_id, 42);
    let error_code = read_i16(&resp, &mut pos);
    assert_eq!(error_code, 0);
    let api_count = read_uvarint(&resp, &mut pos) - 1;
    let mut keys = Vec::new();
    for _ in 0..api_count {
        let key = read_i16(&resp, &mut pos);
        let _min = read_i16(&resp, &mut pos);
        let _max = read_i16(&resp, &mut pos);
        let _tagged = read_uvarint(&resp, &mut pos);
        keys.push(key);
    }
    let _throttle = read_i32(&resp, &mut pos);
    let _tagged = read_uvarint(&resp, &mut pos);
    assert!(keys.contains(&3), "Metadata key 3 not advertised");
    assert!(keys.contains(&18), "ApiVersions key 18 not advertised");

    // ---- Metadata v12 (request all topics) ----
    let mut req = flex_header(3, 12, 99);
    req.push(0); // topics: compact_nullable_array null → uvarint 0
    req.push(0); // allow_auto_topic_creation = false
    req.push(0); // include_topic_authorized_operations = false
    req.push(0); // tagged_fields

    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 0;
    let correlation_id = read_i32(&resp, &mut pos);
    assert_eq!(correlation_id, 99);
    let _resp_tagged = read_uvarint(&resp, &mut pos); // response header v1 tagged
    let _throttle = read_i32(&resp, &mut pos);

    // Brokers
    let broker_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(broker_count, 1, "expected exactly one broker");
    let node_id = read_i32(&resp, &mut pos);
    assert_eq!(node_id, 0);
    let host = read_compact_string(&resp, &mut pos);
    assert_eq!(host, "127.0.0.1");
    let port = read_i32(&resp, &mut pos);
    assert_eq!(port, kafka_addr.port() as i32);
    let _rack = read_compact_nullable_string(&resp, &mut pos);
    let _broker_tagged = read_uvarint(&resp, &mut pos);

    // cluster_id, controller_id
    let cluster_id = read_compact_nullable_string(&resp, &mut pos);
    assert_eq!(cluster_id.as_deref(), Some("qx-test"));
    let controller_id = read_i32(&resp, &mut pos);
    assert_eq!(controller_id, 0);

    // topics
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 1, "expected our one open topic");
    let topic_error = read_i16(&resp, &mut pos);
    assert_eq!(topic_error, 0);
    let topic_name = read_compact_nullable_string(&resp, &mut pos);
    assert_eq!(topic_name.as_deref(), Some("events"));
    // topic_id is 16 bytes for v10+
    pos += 16;
    let is_internal = resp[pos];
    pos += 1;
    assert_eq!(is_internal, 0);
    let partition_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(partition_count, 3, "broker should advertise 3 partitions");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_and_delete_topic_round_trip() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    // gRPC up so we can create the topic, then we'll hit the Kafka port
    // for the new APIs.
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.serve_with_listener(grpc_listener).await.unwrap();
        });
    }
    let mut grpc_client =
        qx_broker::Client::connect(format!("http://{grpc_addr}")).await.unwrap();
    grpc_client.open_topic("to-delete", 2).await.unwrap();

    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let kafka_config = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx-test".to_string(),
        node_id: 0,
        ..Default::default()
    };
    {
        let broker = broker.clone();
        let cfg = kafka_config.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

    // ---- DescribeCluster v0 ----
    let mut req = flex_header(60, 0, 1);
    req.push(0); // include_cluster_authorized_operations = false
    req.push(0); // tagged_fields
    let resp = round_trip(&mut stream, &req).await;

    let mut pos = 0;
    let correlation_id = read_i32(&resp, &mut pos);
    assert_eq!(correlation_id, 1);
    let _resp_tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let error_code = read_i16(&resp, &mut pos);
    assert_eq!(error_code, 0);
    let _err_msg = read_compact_nullable_string(&resp, &mut pos);
    let cluster_id = read_compact_string(&resp, &mut pos);
    assert_eq!(cluster_id, "qx-127.0.0.1");
    let controller_id = read_i32(&resp, &mut pos);
    assert_eq!(controller_id, 0);
    let broker_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(broker_count, 1);

    // ---- DeleteTopics v6 ----
    let mut req = flex_header(20, 6, 2);
    // topics: compact_array of 1 entry
    req.extend_from_slice(&encode_uvarint(2)); // length + 1
    // name = "to-delete" (compact_nullable_string)
    req.extend_from_slice(&compact_string("to-delete"));
    // topic_id = zeroes (16 bytes)
    req.extend_from_slice(&[0u8; 16]);
    req.push(0); // tagged_fields per topic
    req.extend_from_slice(&(5000i32).to_be_bytes()); // timeout_ms
    req.push(0); // tagged_fields
    let resp = round_trip(&mut stream, &req).await;

    let mut pos = 0;
    let correlation_id = read_i32(&resp, &mut pos);
    assert_eq!(correlation_id, 2);
    let _resp_tagged = read_uvarint(&resp, &mut pos);
    let _throttle = read_i32(&resp, &mut pos);
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 1);
    let name = read_compact_nullable_string(&resp, &mut pos);
    assert_eq!(name.as_deref(), Some("to-delete"));
    pos += 16; // topic_id
    let error_code = read_i16(&resp, &mut pos);
    assert_eq!(error_code, 0, "expected NONE, got {error_code}");

    // ---- Metadata afterwards: topic should be gone ----
    let mut req = flex_header(3, 12, 3);
    req.push(0); // topics null
    req.push(0); // allow_auto_topic_creation
    req.push(0); // include_topic_authorized_operations
    req.push(0); // tagged_fields
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 4 + 1 + 4; // correlation_id + tagged + throttle
    let broker_count = read_uvarint(&resp, &mut pos) - 1;
    for _ in 0..broker_count {
        pos += 4; // node_id
        let _ = read_compact_string(&resp, &mut pos); // host
        pos += 4; // port
        let _ = read_compact_nullable_string(&resp, &mut pos); // rack
        let _ = read_uvarint(&resp, &mut pos); // tagged
    }
    let _ = read_compact_nullable_string(&resp, &mut pos); // cluster_id
    pos += 4; // controller_id
    let topic_count = read_uvarint(&resp, &mut pos) - 1;
    assert_eq!(topic_count, 0, "topic should be deleted from metadata");
}

/// Build the legacy (non-flexible) v1 request header used by SaslHandshake.
fn legacy_header(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(&api_key.to_be_bytes());
    h.extend_from_slice(&api_version.to_be_bytes());
    h.extend_from_slice(&correlation_id.to_be_bytes());
    h.extend_from_slice(&(4i16).to_be_bytes());
    h.extend_from_slice(b"test");
    h
}

fn write_kafka_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as i16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_handshake_and_authenticate() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    // Kafka listener with one PLAIN user configured.
    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let mut users = std::collections::HashMap::new();
    users.insert("alice".to_string(), "sesame".to_string());
    let kafka_config = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx-test".to_string(),
        node_id: 0,
        sasl_plain_users: Some(users),
        ..Default::default()
    };
    {
        let broker = broker.clone();
        let cfg = kafka_config.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ---- Connection 1: bad password gets rejected ----
    {
        let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

        // SaslHandshake v1 (non-flexible).
        let mut req = legacy_header(17, 1, 1);
        write_kafka_string(&mut req, "PLAIN");
        let resp = round_trip(&mut stream, &req).await;
        let mut pos = 0;
        assert_eq!(read_i32(&resp, &mut pos), 1);
        let error_code = read_i16(&resp, &mut pos);
        assert_eq!(error_code, 0);

        // SaslAuthenticate v2 (flexible) with wrong password.
        let mut req = flex_header(36, 2, 2);
        let plain = b"\0alice\0wrong-password";
        req.extend_from_slice(&encode_uvarint(plain.len() as u32 + 1));
        req.extend_from_slice(plain);
        req.push(0); // tagged_fields
        let resp = round_trip(&mut stream, &req).await;
        let mut pos = 0;
        assert_eq!(read_i32(&resp, &mut pos), 2);
        let _resp_tagged = read_uvarint(&resp, &mut pos);
        let error_code = read_i16(&resp, &mut pos);
        // 58 = SASL_AUTHENTICATION_FAILED
        assert_eq!(error_code, 58, "expected auth failure, got {error_code}");
    }

    // ---- Connection 2: correct password unlocks the connection ----
    let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

    let mut req = legacy_header(17, 1, 1);
    write_kafka_string(&mut req, "PLAIN");
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 4 + 2; // correlation_id + error_code
    let mech_count = read_i32(&resp, &mut pos);
    // Broker advertises PLAIN + SCRAM-SHA-256.
    assert_eq!(mech_count, 2, "should advertise PLAIN and SCRAM-SHA-256");

    let mut req = flex_header(36, 2, 2);
    let plain = b"\0alice\0sesame";
    req.extend_from_slice(&encode_uvarint(plain.len() as u32 + 1));
    req.extend_from_slice(plain);
    req.push(0); // tagged_fields
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 4 + 1; // correlation_id + tagged
    let error_code = read_i16(&resp, &mut pos);
    assert_eq!(error_code, 0, "auth should succeed, got {error_code}");

    // Now a normal Metadata request should work on this authenticated
    // connection.
    let mut req = flex_header(3, 12, 3);
    req.push(0); // topics null
    req.push(0); // allow_auto_topic_creation
    req.push(0); // include_topic_authorized_operations
    req.push(0); // tagged_fields
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 0;
    assert_eq!(read_i32(&resp, &mut pos), 3);
    // If we got back a real Metadata response (not a connection drop),
    // SASL worked end-to-end.
    assert!(resp.len() > 8, "Metadata response should have content");
}

/// Length-prefixed round trip over any `AsyncRead + AsyncWrite`.
async fn round_trip_generic<S>(stream: &mut S, payload: &[u8]) -> Vec<u8>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_round_trip() {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let cred = qx_broker::ScramCredential::from_password("sesame");
    let mut users = std::collections::HashMap::new();
    users.insert("alice".to_string(), cred.clone());
    let kafka_config = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx-scram".to_string(),
        node_id: 0,
        sasl_scram_users: Some(users),
        ..Default::default()
    };
    {
        let broker = broker.clone();
        let cfg = kafka_config.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut stream = TcpStream::connect(kafka_addr).await.unwrap();

    // Handshake: select SCRAM-SHA-256.
    let mut req = legacy_header(17, 1, 1);
    write_kafka_string(&mut req, "SCRAM-SHA-256");
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 4 + 2;
    let mech_count = read_i32(&resp, &mut pos);
    assert_eq!(mech_count, 2);

    // Round 1: client-first.
    let client_nonce = "abc123";
    let client_first = format!("n,,n=alice,r={client_nonce}");
    let mut req = flex_header(36, 2, 2);
    req.extend_from_slice(&encode_uvarint(client_first.len() as u32 + 1));
    req.extend_from_slice(client_first.as_bytes());
    req.push(0); // tagged_fields
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 4 + 1;
    let code = read_i16(&resp, &mut pos);
    assert_eq!(code, 0);
    pos += {
        // skip error_message (compact_nullable_string) — null => uvarint 0
        let raw = read_uvarint(&resp, &mut pos);
        if raw == 0 { 0 } else { (raw - 1) as usize }
    };
    // server response bytes
    let raw = read_uvarint(&resp, &mut pos);
    let server_first_len = (raw - 1) as usize;
    let server_first = std::str::from_utf8(&resp[pos..pos + server_first_len])
        .unwrap()
        .to_string();
    assert!(server_first.starts_with(&format!("r={client_nonce}")));

    // Parse server-first for combined nonce + salt + iter.
    let mut combined = "";
    let mut salt_b64 = "";
    let mut iter: u32 = 0;
    for attr in server_first.split(',') {
        if let Some((k, v)) = attr.split_once('=') {
            match k {
                "r" => combined = v,
                "s" => salt_b64 = v,
                "i" => iter = v.parse().unwrap(),
                _ => {}
            }
        }
    }
    let b64 = base64::engine::general_purpose::STANDARD;
    let salt = b64.decode(salt_b64).unwrap();

    // Derive client proof exactly like the server.
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(b"sesame", &salt, iter, &mut salted);
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&salted).unwrap();
    mac.update(b"Client Key");
    let client_key: [u8; 32] = mac.finalize().into_bytes().into();
    let mut h = Sha256::new();
    h.update(&client_key);
    let stored_key: [u8; 32] = h.finalize().into();
    let client_first_bare = "n=alice,r=abc123";
    let client_final_no_proof = format!("c=biws,r={combined}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&stored_key).unwrap();
    mac.update(auth_message.as_bytes());
    let client_signature: [u8; 32] = mac.finalize().into_bytes().into();
    let proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(x, y)| x ^ y)
        .collect();
    let proof_b64 = b64.encode(proof);
    let client_final = format!("{client_final_no_proof},p={proof_b64}");

    // Round 2: client-final.
    let mut req = flex_header(36, 2, 3);
    req.extend_from_slice(&encode_uvarint(client_final.len() as u32 + 1));
    req.extend_from_slice(client_final.as_bytes());
    req.push(0);
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 4 + 1;
    let code = read_i16(&resp, &mut pos);
    assert_eq!(code, 0, "scram round 2 should succeed");

    // Connection should now be authenticated — Metadata should work.
    let mut req = flex_header(3, 12, 4);
    req.push(0); req.push(0); req.push(0); req.push(0);
    let resp = round_trip(&mut stream, &req).await;
    let mut pos = 0;
    assert_eq!(read_i32(&resp, &mut pos), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_listener_speaks_tls() {
    // Self-signed cert good for `localhost` and the loopback IP.
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .unwrap();
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();

    let dir = TempDir::new().unwrap();
    let broker = Broker::open(dir.path());

    let kafka_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kafka_addr = kafka_listener.local_addr().unwrap();
    let kafka_config = KafkaConfig {
        advertised_host: "127.0.0.1".to_string(),
        advertised_port: kafka_addr.port() as i32,
        cluster_id: "qx-tls".to_string(),
        node_id: 0,
        tls: Some(qx_broker::TlsConfig {
            cert_pem: cert_pem.clone(),
            key_pem,
            client_ca_pem: None,
        }),
        ..Default::default()
    };
    {
        let broker = broker.clone();
        let cfg = kafka_config.clone();
        tokio::spawn(async move {
            broker.serve_kafka_with_listener(kafka_listener, cfg).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Build a rustls client that trusts our self-signed cert.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    let mut cert_cursor = std::io::Cursor::new(&cert_pem);
    for item in rustls_pemfile::certs(&mut cert_cursor) {
        root_store.add(item.unwrap()).unwrap();
    }
    let client_cfg = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client_cfg));

    let tcp = TcpStream::connect(kafka_addr).await.unwrap();
    let dns_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut stream = connector.connect(dns_name, tcp).await.unwrap();

    // ApiVersions over TLS — proves the handshake worked and the
    // dispatcher is reachable through the encrypted stream.
    let mut req = flex_header(18, 3, 7);
    req.extend_from_slice(&compact_string("tls-client"));
    req.extend_from_slice(&compact_string("0.1.0"));
    req.push(0);
    let resp = round_trip_generic(&mut stream, &req).await;
    let mut pos = 0;
    assert_eq!(read_i32(&resp, &mut pos), 7);
    let error_code = read_i16(&resp, &mut pos);
    assert_eq!(error_code, 0);
    let api_count = read_uvarint(&resp, &mut pos) - 1;
    assert!(api_count > 0, "expected at least one advertised API");
}
