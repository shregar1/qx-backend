//! DeleteRecords (api_key = 21), v2.
//!
//! Marks records at `offset < requested_offset` as deleted for each
//! `(topic, partition)`. We honor this as a logical low-watermark
//! advance: Fetch refuses reads below it, ListOffsets returns it for
//! the "earliest" sentinel. Physical record removal is deferred — the
//! records continue to exist in redb until a future compaction pass
//! drops them. This matches Kafka's eventual-deletion semantics (the
//! request returns success once the watermark moves, not after disk
//! reclamation).
//!
//! Request layout (v2, flexible):
//!
//! ```text
//! topics compact_array<{
//!   name compact_string
//!   partitions compact_array<{
//!     index   i32
//!     offset  i64   // -1 = delete everything up to high_watermark
//!     tagged_fields
//!   }>
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
//!   name compact_string
//!   partitions compact_array<{
//!     index            i32
//!     low_watermark    i64
//!     error_code       i16
//!     tagged_fields
//!   }>
//!   tagged_fields
//! }>
//! tagged_fields
//! ```

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 21;
pub const MIN_VERSION: i16 = 2;
pub const MAX_VERSION: i16 = 2;

#[derive(Debug)]
pub struct DeleteRecordsRequest {
    pub topics: Vec<TopicDeleteRecords>,
    #[allow(dead_code)] // no per-request timeout enforced
    pub timeout_ms: i32,
}

#[derive(Debug)]
pub struct TopicDeleteRecords {
    pub name: String,
    pub partitions: Vec<PartitionDeleteRecords>,
}

#[derive(Debug)]
pub struct PartitionDeleteRecords {
    pub index: i32,
    pub offset: i64,
}

impl DeleteRecordsRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let n = r.read_uvarint()?;
        let topic_count = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            let name = r.read_compact_string()?;
            let pn = r.read_uvarint()?;
            let part_count = if pn == 0 { 0 } else { pn - 1 };
            let mut partitions = Vec::with_capacity(part_count as usize);
            for _ in 0..part_count {
                let index = r.read_i32()?;
                let offset = r.read_i64()?;
                r.read_tagged_fields()?;
                partitions.push(PartitionDeleteRecords { index, offset });
            }
            r.read_tagged_fields()?;
            topics.push(TopicDeleteRecords { name, partitions });
        }
        let timeout_ms = r.read_i32()?;
        r.read_tagged_fields()?;
        Ok(Self { topics, timeout_ms })
    }
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, req: DeleteRecordsRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.topics.len() as u32);

    for t in req.topics {
        w.write_compact_string(&t.name);
        w.write_compact_array_len(t.partitions.len() as u32);

        let topic_handle = broker.lookup(&t.name);
        for part in t.partitions {
            let (low_watermark, error_code): (i64, i16) = match &topic_handle {
                None => (-1, error_codes::UNKNOWN_TOPIC_OR_PARTITION),
                Some(topic) => {
                    if part.index < 0 || (part.index as u32) >= topic.partition_count() {
                        (-1, error_codes::UNKNOWN_TOPIC_OR_PARTITION)
                    } else {
                        let high = topic
                            .partition_end_offset(part.index as u32)
                            .unwrap_or(0) as i64;
                        let target = if part.offset == -1 { high } else { part.offset };
                        if target < 0 || target > high {
                            (-1, error_codes::OFFSET_OUT_OF_RANGE)
                        } else {
                            let new_low = broker.advance_low_watermark(
                                &t.name,
                                part.index as u32,
                                target as u64,
                            );
                            (new_low as i64, error_codes::NONE)
                        }
                    }
                }
            };
            w.write_i32(part.index);
            w.write_i64(low_watermark);
            w.write_i16(error_code);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
}
