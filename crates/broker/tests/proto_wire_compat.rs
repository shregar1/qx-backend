//! Wire-compat check for the `TaskMessage.error` string→bytes swap.
//!
//! In protobuf, `bytes` and `string` share the same wire encoding —
//! both are LEN-delimited (tag wire-type 2). The only semantic
//! difference is that decoders for `string` validate UTF-8 on the
//! incoming bytes. Our engine validates UTF-8 at `Task::decode_from`
//! time, so any error payload that reaches a `TaskMessage` is
//! guaranteed to be valid UTF-8.
//!
//! That means: an old client compiled against the previous proto
//! (`string error = 7`) can still decode messages produced by the new
//! server (`bytes error = 7`) because the bytes on the wire pass
//! UTF-8 validation. This test pins that behaviour by encoding a
//! TaskMessage in the new format and decoding it through a manually-
//! constructed `string`-typed message with the same tag number.

use prost::Message;

/// Round-trip a `bytes`-typed proto field through prost. Every field
/// the build.rs flagged as `bytes::Bytes` follows the same wire
/// shape — these tests just lock in the encoding so a future
/// "switch back to Vec<u8>" wouldn't silently land. The wire format
/// hasn't changed for any of these (`bytes` and `Vec<u8>` use the
/// same tag-wire-type 2 LEN-delimited encoding); the tests are
/// regression insurance.
fn assert_roundtrip<M: Message + Default + PartialEq>(msg: &M) {
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();
    let decoded = M::decode(&buf[..]).unwrap();
    assert!(&decoded == msg);
}

#[test]
fn send_request_with_bytes_payload_round_trips() {
    assert_roundtrip(&qx_broker::proto::SendRequest {
        topic: "orders".into(),
        key: bytes::Bytes::from_static(b"user-42"),
        payload: bytes::Bytes::from_static(b"hello-on-the-wire"),
    });
}

#[test]
fn send_batch_with_bytes_items_round_trips() {
    let req = qx_broker::proto::SendBatchRequest {
        topic: "batched".into(),
        items: vec![
            qx_broker::proto::SendItem {
                key: bytes::Bytes::from_static(b"k1"),
                payload: bytes::Bytes::from_static(b"v1"),
            },
            qx_broker::proto::SendItem {
                key: bytes::Bytes::from_static(b"k2"),
                payload: bytes::Bytes::from_static(b"v2"),
            },
        ],
    };
    assert_roundtrip(&req);
}

#[test]
fn topic_message_with_bytes_payload_round_trips() {
    assert_roundtrip(&qx_broker::proto::TopicMessage {
        partition: 3,
        id: 99,
        receipt: Some(qx_broker::proto::Receipt {
            partition: 3,
            record_id: 99,
            attempt: 1,
        }),
        attempts: 1,
        payload: bytes::Bytes::from_static(b"received-bytes"),
    });
}

#[test]
fn streamed_record_with_bytes_payload_round_trips() {
    assert_roundtrip(&qx_broker::proto::StreamedRecord {
        partition: 1,
        id: 7,
        payload: bytes::Bytes::from_static(b"streamed-bytes"),
    });
}

#[test]
fn create_task_request_with_bytes_payload_round_trips() {
    assert_roundtrip(&qx_broker::proto::CreateRootTaskRequest {
        workflow: "wf".into(),
        payload: bytes::Bytes::from_static(b"root-task-input"),
    });
    assert_roundtrip(&qx_broker::proto::CreateChildTaskRequest {
        workflow: "wf".into(),
        parent_id: 17,
        payload: bytes::Bytes::from_static(b"child-task-input"),
    });
}

#[test]
fn complete_task_request_with_bytes_result_round_trips() {
    assert_roundtrip(&qx_broker::proto::CompleteTaskRequest {
        workflow: "wf".into(),
        task_id: 42,
        result: bytes::Bytes::from_static(b"task-result"),
    });
}

#[test]
fn complete_task_with_emits_round_trips() {
    assert_roundtrip(&qx_broker::proto::CompleteTaskWithEmitsRequest {
        workflow: "wf".into(),
        task_id: 42,
        result: bytes::Bytes::from_static(b"done"),
        emits: vec![
            qx_broker::proto::TopicEmit {
                topic: "downstream".into(),
                payload: bytes::Bytes::from_static(b"emit-1"),
            },
            qx_broker::proto::TopicEmit {
                topic: "downstream".into(),
                payload: bytes::Bytes::from_static(b"emit-2"),
            },
        ],
    });
}

/// Mirror of `proto::TaskMessage` with the historical `string error`
/// shape. Field numbers and types must match the previous-generation
/// proto exactly.
#[derive(Clone, PartialEq, Eq, prost::Message)]
struct LegacyTaskMessage {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(uint64, optional, tag = "2")]
    parent_id: Option<u64>,
    #[prost(uint32, tag = "3")]
    status: u32,
    #[prost(uint64, tag = "4")]
    created_nanos: u64,
    #[prost(bytes = "vec", tag = "5")]
    payload: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    result: Vec<u8>,
    /// The pre-swap shape — a `string` at field 7.
    #[prost(string, tag = "7")]
    error: String,
}

#[test]
fn new_server_message_decodes_under_legacy_string_error_shape() {
    // Build a TaskMessage in the current (post-swap) shape via the
    // generated proto type. Error is a `Bytes` whose payload is valid
    // UTF-8 — which is the contract the engine maintains.
    let current = qx_broker::proto::TaskMessage {
        id: 17,
        parent_id: Some(3),
        status: 3, // Failed
        created_nanos: 100,
        payload: bytes::Bytes::from_static(b"in"),
        result: bytes::Bytes::from_static(b"out"),
        error: bytes::Bytes::from_static(b"boom: timeout"),
    };
    let mut wire = Vec::new();
    current.encode(&mut wire).expect("encode current shape");

    // A legacy client compiled against `string error = 7` decodes the
    // exact same bytes and observes the UTF-8 string. If our engine
    // ever managed to publish non-UTF-8 bytes the decode below would
    // return `Err(DecodeError("invalid string value: data is not UTF-8"))`.
    let legacy = LegacyTaskMessage::decode(&wire[..])
        .expect("legacy client must decode new payload when error is valid UTF-8");
    assert_eq!(legacy.id, 17);
    assert_eq!(legacy.parent_id, Some(3));
    assert_eq!(legacy.status, 3);
    assert_eq!(legacy.created_nanos, 100);
    assert_eq!(legacy.payload, b"in");
    assert_eq!(legacy.result, b"out");
    assert_eq!(legacy.error, "boom: timeout");
}

#[test]
fn legacy_server_message_decodes_under_current_bytes_error_shape() {
    // Reverse direction: a legacy server sends a TaskMessage encoded
    // with the `string`-typed error field. A new client decodes via
    // the bytes-typed field and sees the same payload (now as
    // `Bytes`). Both sides agree, no migration scripting needed.
    let legacy = LegacyTaskMessage {
        id: 5,
        parent_id: None,
        status: 2,
        created_nanos: 42,
        payload: b"hi".to_vec(),
        result: b"done".to_vec(),
        error: "".into(),
    };
    let mut wire = Vec::new();
    legacy.encode(&mut wire).unwrap();

    let current = qx_broker::proto::TaskMessage::decode(&wire[..]).unwrap();
    assert_eq!(current.id, 5);
    assert_eq!(current.parent_id, None);
    assert_eq!(current.status, 2);
    assert_eq!(current.created_nanos, 42);
    assert_eq!(&current.payload[..], b"hi");
    assert_eq!(&current.result[..], b"done");
    // Empty proto bytes round-trips as empty `Bytes`.
    assert!(current.error.is_empty());
}
