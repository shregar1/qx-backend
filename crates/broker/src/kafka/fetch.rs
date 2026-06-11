//! Fetch (api_key = 1), v12.
//!
//! Returns one record batch per partition starting at the client's
//! requested offset. We re-encode the records as a fresh magic=2 batch
//! with CRC32C, which is the simplest correct path: we don't store batches
//! on disk, we store individual records, so we have to construct a batch
//! over them on the fly.
//!
//! No compression on the response yet — the response attributes always
//! say "uncompressed." Modern clients accept that.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 1;
pub const MIN_VERSION: i16 = 12;
pub const MAX_VERSION: i16 = 12;

/// Hard cap so a single response can't balloon the broker's memory.
const MAX_BYTES_HARD_CAP: i32 = 32 * 1024 * 1024;

#[derive(Debug)]
pub struct FetchRequest {
    #[allow(dead_code)]
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    /// 0 = READ_UNCOMMITTED (default), 1 = READ_COMMITTED.
    /// READ_COMMITTED caps the response at the partition's Last Stable
    /// Offset and reports any aborted-txn ranges so the client can drop
    /// them client-side.
    pub isolation_level: i8,
    #[allow(dead_code)]
    pub session_id: i32,
    #[allow(dead_code)]
    pub session_epoch: i32,
    pub topics: Vec<FetchTopic>,
    #[allow(dead_code)]
    pub forgotten: Vec<ForgottenTopic>,
    #[allow(dead_code)]
    pub rack_id: String,
}

#[derive(Debug)]
pub struct FetchTopic {
    pub name: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug)]
pub struct FetchPartition {
    pub index: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug)]
pub struct ForgottenTopic {
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub partitions: Vec<i32>,
}

impl FetchRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let replica_id = r.read_i32()?;
        let max_wait_ms = r.read_i32()?;
        let min_bytes = r.read_i32()?;
        let max_bytes = r.read_i32()?;
        let isolation_level = r.read_i8()?;
        let session_id = r.read_i32()?;
        let session_epoch = r.read_i32()?;

        // topics
        let n = r.read_uvarint()?;
        let topic_count = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            let name = r.read_compact_string()?;
            let pn = r.read_uvarint()?;
            let pcount = if pn == 0 { 0 } else { pn - 1 };
            let mut partitions = Vec::with_capacity(pcount as usize);
            for _ in 0..pcount {
                let index = r.read_i32()?;
                let _current_leader_epoch = r.read_i32()?;
                let fetch_offset = r.read_i64()?;
                let _last_fetched_epoch = r.read_i32()?;
                let _log_start_offset = r.read_i64()?;
                let partition_max_bytes = r.read_i32()?;
                r.read_tagged_fields()?;
                partitions.push(FetchPartition {
                    index,
                    fetch_offset,
                    partition_max_bytes,
                });
            }
            r.read_tagged_fields()?;
            topics.push(FetchTopic { name, partitions });
        }

        // forgotten_topics
        let fn_n = r.read_uvarint()?;
        let f_count = if fn_n == 0 { 0 } else { fn_n - 1 };
        let mut forgotten = Vec::with_capacity(f_count as usize);
        for _ in 0..f_count {
            let name = r.read_compact_string()?;
            let pn = r.read_uvarint()?;
            let pcount = if pn == 0 { 0 } else { pn - 1 };
            let mut partitions = Vec::with_capacity(pcount as usize);
            for _ in 0..pcount {
                partitions.push(r.read_i32()?);
            }
            r.read_tagged_fields()?;
            forgotten.push(ForgottenTopic { name, partitions });
        }

        let rack_id = r.read_compact_string()?;
        r.read_tagged_fields()?;

        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten,
            rack_id,
        })
    }
}

/// Handle the fetch and write the response body into `w`. The response
/// header is already written by the dispatcher.
pub fn handle_and_encode(w: &mut Writer, broker: &Broker, req: FetchRequest, codec: u8) {
    let max_bytes = req.max_bytes.min(MAX_BYTES_HARD_CAP).max(0) as usize;
    let read_committed = req.isolation_level == 1;

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE); // top-level error
    w.write_i32(0); // session_id (always 0; we don't implement incremental)

    let mut bytes_so_far = 0usize;

    w.write_compact_array_len(req.topics.len() as u32);
    for topic in req.topics {
        w.write_compact_string(&topic.name);
        w.write_compact_array_len(topic.partitions.len() as u32);
        for part in topic.partitions {
            encode_one_partition(
                w,
                broker,
                &topic.name,
                &part,
                &mut bytes_so_far,
                max_bytes,
                codec,
                read_committed,
            );
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
}

fn encode_one_partition(
    w: &mut Writer,
    broker: &Broker,
    topic_name: &str,
    part: &FetchPartition,
    bytes_so_far: &mut usize,
    overall_max_bytes: usize,
    codec: u8,
    read_committed: bool,
) {
    w.write_i32(part.index);

    let topic = match broker.lookup(topic_name) {
        Some(t) => t,
        None => {
            write_partition_error(w, error_codes::UNKNOWN_TOPIC_OR_PARTITION);
            return;
        }
    };
    if part.index < 0 || (part.index as u32) >= topic.partition_count() {
        write_partition_error(w, error_codes::NOT_LEADER_OR_FOLLOWER);
        return;
    }
    let end = match topic.partition_end_offset(part.index as u32) {
        Ok(e) => e,
        Err(_) => {
            write_partition_error(w, error_codes::UNKNOWN_SERVER_ERROR);
            return;
        }
    };

    if part.fetch_offset < 0 {
        write_partition_error(w, error_codes::OFFSET_OUT_OF_RANGE);
        return;
    }
    let fetch_offset = part.fetch_offset as u64;
    if fetch_offset > end {
        write_partition_error(w, error_codes::OFFSET_OUT_OF_RANGE);
        return;
    }
    let log_start = broker.low_watermark(topic_name, part.index as u32);
    if fetch_offset < log_start {
        write_partition_error(w, error_codes::OFFSET_OUT_OF_RANGE);
        return;
    }

    // Compute Last Stable Offset (LSO) by consulting the txn registry.
    // If there are open transactions touching this partition, LSO is the
    // smallest in-flight first_offset; otherwise it equals the high
    // watermark.
    let registry = broker.txn_registry();
    let lso = registry
        .last_stable_offset(topic_name, part.index)
        .unwrap_or(end);

    // For READ_COMMITTED we cap the visible window at LSO.
    let effective_end = if read_committed { lso } else { end };

    // Aborted-txn ranges to surface. Only present for READ_COMMITTED.
    let aborted_entries: Vec<_> = if read_committed {
        registry.aborted_in_range(
            topic_name,
            part.index,
            part.fetch_offset,
            effective_end as i64,
        )
    } else {
        Vec::new()
    };

    // Successful partition header.
    w.write_i16(error_codes::NONE);
    w.write_i64(end as i64);        // high_watermark
    w.write_i64(lso as i64);        // last_stable_offset
    w.write_i64(log_start as i64);  // log_start_offset

    // aborted_transactions: compact_array of (producer_id, first_offset).
    w.write_compact_array_len(aborted_entries.len() as u32);
    for entry in &aborted_entries {
        w.write_i64(entry.producer_id);
        w.write_i64(entry.first_offset);
        w.write_empty_tagged_fields();
    }

    // preferred_read_replica = -1: "consume from the broker you contacted".
    // Any non-`-1` value tells librdkafka "go fetch from that replica
    // instead" and it will *skip the records in this response* — even if
    // we return data. That manifests as a hung kcat -C.
    w.write_i32(-1);

    if fetch_offset >= effective_end {
        // Up to date (or no readable records yet under READ_COMMITTED) —
        // return empty records bytes (compact_bytes len=1 = empty).
        w.write_uvarint(1);
        w.write_empty_tagged_fields();
        return;
    }

    // Pull records up to either the partition_max_bytes cap, the overall
    // max_bytes cap, or the end of the log.
    let part_cap = part.partition_max_bytes.max(0) as usize;
    let remaining_global = overall_max_bytes.saturating_sub(*bytes_so_far);
    let effective_cap = part_cap.min(remaining_global.max(1));

    let mut payloads: Vec<bytes::Bytes> = Vec::new();
    let mut accumulated = 0usize;
    let mut id = fetch_offset;
    while id < effective_end {
        let payload = match topic.read_record(part.index as u32, id) {
            Ok(Some(p)) => p,
            Ok(None) => break,
            Err(_) => break,
        };
        // Cheap upper-bound on the per-record on-the-wire cost: payload
        // plus a generous record-overhead constant. If we'd blow the cap
        // and we already have at least one record, stop.
        let projected = accumulated + payload.len() + 32;
        if !payloads.is_empty() && projected > effective_cap {
            break;
        }
        accumulated = projected;
        payloads.push(payload);
        id += 1;
    }

    let batch_bytes = encode_record_batch(fetch_offset as i64, &payloads, codec);
    *bytes_so_far += batch_bytes.len();
    w.write_uvarint(batch_bytes.len() as u32 + 1);
    w.write_bytes_raw(&batch_bytes);
    w.write_empty_tagged_fields();
}

fn write_partition_error(w: &mut Writer, code: i16) {
    w.write_i16(code);
    w.write_i64(-1); // high_watermark
    w.write_i64(-1); // last_stable_offset
    w.write_i64(-1); // log_start_offset
    w.write_compact_array_len(0); // aborted_transactions
    w.write_i32(-1); // preferred_read_replica
    w.write_uvarint(0); // records: null
    w.write_empty_tagged_fields();
}

/// Encode N records as a single magic=2 record batch with CRC32C and the
/// given compression codec applied to the records section.
fn encode_record_batch(base_offset: i64, payloads: &[bytes::Bytes], codec: u8) -> Vec<u8> {
    if payloads.is_empty() {
        return Vec::new();
    }

    // ---- records section: record_count + each record ----
    let mut records_section = Writer::new();
    records_section.write_i32(payloads.len() as i32);
    for (i, value) in payloads.iter().enumerate() {
        let mut rec = Writer::new();
        rec.write_i8(0); // attributes
        write_zigzag_varlong(&mut rec, 0); // timestamp_delta
        write_zigzag_varint(&mut rec, i as i32); // offset_delta
        write_zigzag_varint(&mut rec, -1); // key length = -1 (null key)
        write_zigzag_varint(&mut rec, value.len() as i32); // value length
        rec.write_bytes_raw(value);
        write_zigzag_varint(&mut rec, 0); // header count
        let rec_bytes = rec.into_bytes();
        write_zigzag_varint(&mut records_section, rec_bytes.len() as i32);
        records_section.write_bytes_raw(&rec_bytes);
    }
    let records_raw = records_section.into_bytes();
    let records_encoded = match codec {
        1 => compress_gzip(&records_raw),
        2 => compress_snappy(&records_raw),
        3 => compress_lz4(&records_raw),
        4 => compress_zstd(&records_raw),
        _ => records_raw,
    };

    // ---- inner body (everything after the crc field) ----
    let mut body = Writer::new();
    body.write_i16(codec as i16); // attributes: low 3 bits = codec, rest 0
    body.write_i32((payloads.len() as i32) - 1); // last_offset_delta
    body.write_i64(0); // base_timestamp
    body.write_i64(0); // max_timestamp
    body.write_i64(-1); // producer_id
    body.write_i16(-1); // producer_epoch
    body.write_i32(-1); // base_sequence
    body.write_bytes_raw(&records_encoded);

    let body_bytes = body.into_bytes();
    let crc = crc32c::crc32c(&body_bytes);

    // ---- outer batch ----
    let mut out = Writer::new();
    out.write_i64(base_offset);
    out.write_i32((4 + 1 + 4 + body_bytes.len()) as i32); // batch_length
    out.write_i32(0); // partition_leader_epoch
    out.write_i8(2); // magic
    out.write_u32(crc);
    out.write_bytes_raw(&body_bytes);
    out.into_bytes()
}

fn compress_gzip(raw: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(raw).expect("gzip encode in-memory");
    enc.finish().expect("gzip finish")
}

fn compress_snappy(raw: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(raw)
        .expect("snappy encode in-memory")
}

fn compress_lz4(raw: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut out = Vec::new();
    let mut enc = lz4_flex::frame::FrameEncoder::new(&mut out);
    enc.write_all(raw).expect("lz4 encode in-memory");
    enc.finish().expect("lz4 finish");
    out
}

fn compress_zstd(raw: &[u8]) -> Vec<u8> {
    zstd::encode_all(raw, 3).expect("zstd encode in-memory")
}

fn write_zigzag_varint(w: &mut Writer, v: i32) {
    let u = ((v << 1) ^ (v >> 31)) as u32;
    w.write_uvarint(u);
}

fn write_zigzag_varlong(w: &mut Writer, v: i64) {
    let mut u = ((v << 1) ^ (v >> 63)) as u64;
    while u >= 0x80 {
        w.write_i8((((u & 0x7f) as u8) | 0x80) as i8);
        u >>= 7;
    }
    w.write_i8(u as i8);
}
