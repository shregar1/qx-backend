//! CreateTopics (api_key = 19).
//!
//! Supports v5-v7. The request carries a list of topics with desired
//! partition counts; we route each to `Broker::get_or_open` which creates
//! the topic if it doesn't exist (using default queue config).
//!
//! `replication_factor` is honored only in the response — we're single-broker
//! today, so anything other than 1 (or -1 = default) is accepted and
//! quietly clamped.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 19;
pub const MIN_VERSION: i16 = 5;
pub const MAX_VERSION: i16 = 7;

#[derive(Debug)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreatableTopic>,
    #[allow(dead_code)] // we apply changes synchronously; honoring this requires async work
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug)]
pub struct CreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

impl CreateTopicsRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let n = r.read_uvarint()?;
        let count = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name = r.read_compact_string()?;
            let num_partitions = r.read_i32()?;
            let replication_factor = r.read_i16()?;

            // assignments: compact_array — we ignore the body but must
            // walk past it so the cursor lines up.
            let assign_n = r.read_uvarint()?;
            let assign_count = if assign_n == 0 { 0 } else { assign_n - 1 };
            for _ in 0..assign_count {
                let _partition_index = r.read_i32()?;
                let broker_n = r.read_uvarint()?;
                let broker_count = if broker_n == 0 { 0 } else { broker_n - 1 };
                for _ in 0..broker_count {
                    let _broker_id = r.read_i32()?;
                }
                r.read_tagged_fields()?;
            }

            // configs: compact_array — also walked past.
            let cfg_n = r.read_uvarint()?;
            let cfg_count = if cfg_n == 0 { 0 } else { cfg_n - 1 };
            for _ in 0..cfg_count {
                let _name = r.read_compact_string()?;
                let _value = r.read_compact_nullable_string()?;
                r.read_tagged_fields()?;
            }
            r.read_tagged_fields()?;
            topics.push(CreatableTopic { name, num_partitions, replication_factor });
        }
        let timeout_ms = r.read_i32()?;
        let validate_only = r.read_bool()?;
        r.read_tagged_fields()?;
        Ok(Self { topics, timeout_ms, validate_only })
    }
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, version: i16, req: CreateTopicsRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.topics.len() as u32);

    for t in req.topics {
        let partitions = if t.num_partitions <= 0 { 1 } else { t.num_partitions as u32 };
        let (error_code, error_message): (i16, Option<&str>) = if req.validate_only {
            (error_codes::NONE, None)
        } else {
            match broker.get_or_open(&t.name, partitions) {
                Ok(_) => (error_codes::NONE, None),
                Err(crate::error::BrokerError::Topic(qx_topic::TopicError::PartitionCountMismatch { .. })) => {
                    (error_codes::TOPIC_ALREADY_EXISTS, Some("partition count differs from existing"))
                }
                Err(_) => (error_codes::UNKNOWN_SERVER_ERROR, Some("internal error opening topic")),
            }
        };

        w.write_compact_string(&t.name);
        if version >= 7 {
            // topic_id (uuid). Deterministic from name so subsequent responses match.
            write_topic_uuid(w, &t.name);
        }
        w.write_i16(error_code);
        w.write_compact_nullable_string(error_message);
        // v5+ returns the canonical partition / replication counts and configs.
        w.write_i32(partitions as i32);
        w.write_i16(if t.replication_factor <= 0 { 1 } else { t.replication_factor });
        // configs: write null compact array (uvarint(0)) — clients accept this.
        w.write_uvarint(0);
        w.write_empty_tagged_fields();
    }

    w.write_empty_tagged_fields();
}

fn write_topic_uuid(w: &mut Writer, name: &str) {
    // Same construction as Metadata uses, keeping topic_ids stable across
    // responses for the same topic name.
    let h1 = xxhash_rust::xxh3::xxh3_64_with_seed(name.as_bytes(), 0xA1B2_C3D4_E5F6_0708);
    let h2 = xxhash_rust::xxh3::xxh3_64_with_seed(name.as_bytes(), 0x1122_3344_5566_7788);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&h1.to_be_bytes());
    bytes[8..].copy_from_slice(&h2.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    w.write_bytes_raw(&bytes);
}
