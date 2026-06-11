//! ListOffsets (api_key = 2).
//!
//! Clients call this to discover the earliest / latest offset of a
//! partition (and, for timestamp-based queries, to find the offset of a
//! record at a given time — not implemented yet). v1+ added `timestamp`
//! to the response; v7 is flexible.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 2;
pub const MIN_VERSION: i16 = 1;
pub const MAX_VERSION: i16 = 7;

#[derive(Debug)]
pub struct ListOffsetsRequest {
    #[allow(dead_code)] // we only have one broker; replica_id ignored
    pub replica_id: i32,
    #[allow(dead_code)] // transactional isolation not implemented yet
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsTopic>,
}

#[derive(Debug)]
pub struct ListOffsetsTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

#[derive(Debug)]
pub struct ListOffsetsPartition {
    pub index: i32,
    /// `-1` = latest, `-2` = earliest, any other = millis-since-epoch
    /// timestamp (not yet supported).
    pub timestamp: i64,
}

impl ListOffsetsRequest {
    pub fn decode(version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let replica_id = r.read_i32()?;
        let isolation_level = if version >= 2 { r.read_i8()? } else { 0 };
        let topics = if version >= 6 {
            decode_topics_flexible(&mut r)?
        } else {
            decode_topics_legacy(&mut r, version)?
        };
        if version >= 6 {
            r.read_tagged_fields()?;
        }
        Ok(Self { replica_id, isolation_level, topics })
    }
}

fn decode_topics_flexible(r: &mut Reader<'_>) -> WireResult<Vec<ListOffsetsTopic>> {
    let n = r.read_uvarint()?;
    let count = if n == 0 { 0 } else { n - 1 };
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut partitions = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            let index = r.read_i32()?;
            let _current_leader_epoch = r.read_i32()?;
            let timestamp = r.read_i64()?;
            r.read_tagged_fields()?;
            partitions.push(ListOffsetsPartition { index, timestamp });
        }
        r.read_tagged_fields()?;
        out.push(ListOffsetsTopic { name, partitions });
    }
    Ok(out)
}

fn decode_topics_legacy(r: &mut Reader<'_>, version: i16) -> WireResult<Vec<ListOffsetsTopic>> {
    let count = r.read_i32()?;
    let mut out = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let name = r.read_string()?;
        let pcount = r.read_i32()?;
        let mut partitions = Vec::with_capacity(pcount.max(0) as usize);
        for _ in 0..pcount {
            let index = r.read_i32()?;
            if version >= 4 {
                let _current_leader_epoch = r.read_i32()?;
            }
            let timestamp = r.read_i64()?;
            if version <= 0 {
                let _max_num_offsets = r.read_i32()?;
            }
            partitions.push(ListOffsetsPartition { index, timestamp });
        }
        out.push(ListOffsetsTopic { name, partitions });
    }
    Ok(out)
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, version: i16, req: ListOffsetsRequest) {
    if version >= 2 {
        w.write_i32(0); // throttle_time_ms
    }
    let flexible = version >= 6;

    if flexible {
        w.write_compact_array_len(req.topics.len() as u32);
    } else {
        w.write_i32(req.topics.len() as i32);
    }

    for topic in req.topics {
        if flexible {
            w.write_compact_string(&topic.name);
            w.write_compact_array_len(topic.partitions.len() as u32);
        } else {
            w.write_string(&topic.name);
            w.write_i32(topic.partitions.len() as i32);
        }

        for part in topic.partitions {
            let (err, timestamp, offset) = resolve_offset(broker, &topic.name, &part);
            w.write_i32(part.index);
            w.write_i16(err);
            if version >= 1 {
                w.write_i64(timestamp);
            }
            w.write_i64(offset);
            if version >= 4 {
                w.write_i32(0); // leader_epoch
            }
            if flexible {
                w.write_empty_tagged_fields();
            }
        }
        if flexible {
            w.write_empty_tagged_fields();
        }
    }

    if flexible {
        w.write_empty_tagged_fields();
    }
}

fn resolve_offset(
    broker: &Broker,
    topic_name: &str,
    part: &ListOffsetsPartition,
) -> (i16, i64, i64) {
    let Some(topic) = broker.lookup(topic_name) else {
        return (error_codes::UNKNOWN_TOPIC_OR_PARTITION, -1, -1);
    };
    if part.index < 0 || (part.index as u32) >= topic.partition_count() {
        return (error_codes::NOT_LEADER_OR_FOLLOWER, -1, -1);
    }
    let end = match topic.partition_end_offset(part.index as u32) {
        Ok(e) => e as i64,
        Err(_) => return (error_codes::UNKNOWN_SERVER_ERROR, -1, -1),
    };
    let low = broker.low_watermark(topic_name, part.index as u32) as i64;
    match part.timestamp {
        -2 => (error_codes::NONE, -1, low),       // earliest: respects DeleteRecords
        -1 => (error_codes::NONE, -1, end),       // latest: high watermark
        _ => {
            // Timestamp lookup not implemented — pretend no record matches.
            (error_codes::NONE, -1, -1)
        }
    }
}
