//! DeleteTopics (api_key = 20), v6.
//!
//! v6 body (flexible):
//!
//! ```text
//! topics compact_array<{
//!   name compact_nullable_string  // null when topic_id is set instead
//!   topic_id uuid                  // 16 raw bytes
//!   tagged_fields
//! }>
//! timeout_ms i32
//! tagged_fields
//! ```
//!
//! Response:
//! ```text
//! throttle_time_ms i32
//! topics compact_array<{
//!   name compact_nullable_string
//!   topic_id uuid
//!   error_code i16
//!   error_message compact_nullable_string
//!   tagged_fields
//! }>
//! tagged_fields
//! ```
//!
//! We accept v6 only — that's what modern librdkafka and the
//! AdminClient ship. Older versions used a flat array of strings; not
//! worth carrying.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 20;
pub const MIN_VERSION: i16 = 6;
pub const MAX_VERSION: i16 = 6;

#[derive(Debug)]
pub struct DeletableTopic {
    pub name: Option<String>,
    /// 16-byte topic_id. Zero-filled when the client passes only the name.
    pub topic_id: [u8; 16],
}

#[derive(Debug)]
pub struct DeleteTopicsRequest {
    pub topics: Vec<DeletableTopic>,
    #[allow(dead_code)] // no per-request timeout enforced; we delete synchronously
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let n = r.read_uvarint()?;
        let count = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name = r.read_compact_nullable_string()?;
            let raw = r.read_raw(16, "delete-topics topic_id")?;
            let mut topic_id = [0u8; 16];
            topic_id.copy_from_slice(&raw);
            r.read_tagged_fields()?;
            topics.push(DeletableTopic { name, topic_id });
        }
        let timeout_ms = r.read_i32()?;
        r.read_tagged_fields()?;
        Ok(Self { topics, timeout_ms })
    }
}

pub async fn handle_and_encode(w: &mut Writer, broker: &Broker, req: DeleteTopicsRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.topics.len() as u32);

    for t in req.topics {
        let (error_code, error_message): (i16, Option<String>) = match &t.name {
            None => (
                error_codes::INVALID_REQUEST,
                Some("delete-by-topic-id not supported; pass topic name".into()),
            ),
            Some(name) => match broker.delete_topic(name).await {
                Ok(true) => (error_codes::NONE, None),
                Ok(false) => (
                    error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                    Some(format!("topic '{name}' not found")),
                ),
                Err(e) => (
                    error_codes::UNKNOWN_SERVER_ERROR,
                    Some(format!("internal error: {e}")),
                ),
            },
        };
        w.write_compact_nullable_string(t.name.as_deref());
        w.write_bytes_raw(&t.topic_id);
        w.write_i16(error_code);
        w.write_compact_nullable_string(error_message.as_deref());
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
}
