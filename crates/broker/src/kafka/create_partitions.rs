//! CreatePartitions (api_key = 37), v3.
//!
//! Today qx-topic seals partition count at create time (meta.bin
//! enforces it on reopen). Adding partitions live would require a
//! coordinated swap of the in-memory Topic alongside an additive
//! rewrite of meta.bin — doable but invasive against the current
//! `Vec<Mutex<Queue>>` interior-mutability shape, so we honestly
//! reject the request rather than pretending success.
//!
//! Clients that need to grow a topic should DeleteTopics + CreateTopics
//! with the new partition count.
//!
//! Request layout (v3, flexible):
//!
//! ```text
//! topics compact_array<{
//!   name compact_string
//!   count i32
//!   assignments compact_nullable_array<{
//!     broker_ids compact_array<i32>
//!     tagged_fields
//!   }>
//!   tagged_fields
//! }>
//! timeout_ms i32
//! validate_only bool
//! tagged_fields
//! ```
//!
//! Response:
//! ```text
//! throttle_time_ms i32
//! results compact_array<{
//!   name compact_string
//!   error_code i16
//!   error_message compact_nullable_string
//!   tagged_fields
//! }>
//! tagged_fields
//! ```

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 37;
pub const MIN_VERSION: i16 = 3;
pub const MAX_VERSION: i16 = 3;

#[derive(Debug)]
pub struct CreatePartitionsRequest {
    pub topics: Vec<TopicGrow>,
    #[allow(dead_code)]
    pub timeout_ms: i32,
    #[allow(dead_code)]
    pub validate_only: bool,
}

#[derive(Debug)]
pub struct TopicGrow {
    pub name: String,
    pub count: i32,
}

impl CreatePartitionsRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let n = r.read_uvarint()?;
        let topic_count = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            let name = r.read_compact_string()?;
            let count = r.read_i32()?;
            // assignments — nullable compact array; walk past whatever's there.
            let an = r.read_uvarint()?;
            let acount = if an == 0 { 0 } else { an - 1 };
            for _ in 0..acount {
                let bn = r.read_uvarint()?;
                let bcount = if bn == 0 { 0 } else { bn - 1 };
                for _ in 0..bcount {
                    let _broker_id = r.read_i32()?;
                }
                r.read_tagged_fields()?;
            }
            r.read_tagged_fields()?;
            topics.push(TopicGrow { name, count });
        }
        let timeout_ms = r.read_i32()?;
        let validate_only = r.read_bool()?;
        r.read_tagged_fields()?;
        Ok(Self { topics, timeout_ms, validate_only })
    }
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, req: CreatePartitionsRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.topics.len() as u32);
    for t in req.topics {
        let (error_code, error_message): (i16, Option<&str>) = match broker.lookup(&t.name) {
            None => (
                error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("topic not found"),
            ),
            Some(_) => (
                error_codes::INVALID_PARTITIONS,
                Some("qx-broker does not support live partition growth"),
            ),
        };
        w.write_compact_string(&t.name);
        w.write_i16(error_code);
        w.write_compact_nullable_string(error_message);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
}
