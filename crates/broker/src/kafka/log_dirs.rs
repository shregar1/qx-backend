//! DescribeLogDirs (35, v3) + AlterReplicaLogDirs (34, v2).
//!
//! `kafka-log-dirs.sh` and the AdminClient.describeLogDirs() call hit
//! this. qx-broker has exactly one data directory (the one we were
//! opened with), so the response is simple: every topic-partition for
//! the request lives under that root.
//!
//! AlterReplicaLogDirs is the "move this partition to that dir"
//! operation. We have only one dir — return KAFKA_STORAGE_ERROR for any
//! attempt to specify a different one.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

// ─────────────────────── DescribeLogDirs (35) v3 ───────────────────────

pub const DESCRIBE_LOG_DIRS_KEY: i16 = 35;
pub const DESCRIBE_LOG_DIRS_MIN: i16 = 3;
pub const DESCRIBE_LOG_DIRS_MAX: i16 = 3;

/// Request body (flexible):
///
/// ```text
/// topics compact_nullable_array<{
///   topic compact_string
///   partitions compact_array<i32>
///   tagged_fields
/// }>
/// tagged_fields
/// ```
///
/// `None` for topics = "describe every topic".
pub fn handle_describe_log_dirs(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let request_specific = n != 0;
    let topic_count = if n == 0 { 0 } else { n - 1 };
    let mut requested: Vec<(String, Vec<i32>)> = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let topic = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            parts.push(r.read_i32()?);
        }
        r.read_tagged_fields()?;
        requested.push((topic, parts));
    }
    r.read_tagged_fields()?;

    // Figure out what we're describing.
    let mut topics_to_report: Vec<(String, Vec<u32>)> = Vec::new();
    if request_specific {
        for (name, parts) in requested {
            if let Some(topic) = broker.lookup(&name) {
                let pc = topic.partition_count();
                let parts: Vec<u32> = if parts.is_empty() {
                    (0..pc).collect()
                } else {
                    parts.into_iter().filter_map(|p| {
                        if p >= 0 && (p as u32) < pc { Some(p as u32) } else { None }
                    }).collect()
                };
                topics_to_report.push((name, parts));
            } else {
                topics_to_report.push((name, vec![]));
            }
        }
    } else {
        for (name, count) in broker.topic_names_and_counts() {
            topics_to_report.push((name, (0..count).collect()));
        }
    }

    w.write_i32(0); // throttle_time_ms
    // One log_dir per response — we have exactly one root.
    w.write_compact_array_len(1);
    w.write_i16(error_codes::NONE);
    let dir = broker.dir().to_string_lossy().to_string();
    w.write_compact_string(&dir);
    w.write_compact_array_len(topics_to_report.len() as u32);
    for (name, parts) in topics_to_report {
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        for pidx in parts {
            w.write_i32(pidx as i32);
            // We don't track per-partition byte size — return -1 to signal
            // "unknown". Real Kafka clients tolerate this.
            w.write_i64(-1); // partition_size
            w.write_i64(0);  // offset_lag — partition is its own leader
            w.write_bool(false); // is_future_replica
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    // v3 adds total_bytes + usable_bytes — return -1 for unknown.
    w.write_i64(-1);
    w.write_i64(-1);
    w.write_empty_tagged_fields();
    w.write_empty_tagged_fields();
    Ok(())
}

// ────────────────────── AlterReplicaLogDirs (34) v2 ────────────────────

pub const ALTER_REPLICA_LOG_DIRS_KEY: i16 = 34;
pub const ALTER_REPLICA_LOG_DIRS_MIN: i16 = 2;
pub const ALTER_REPLICA_LOG_DIRS_MAX: i16 = 2;

/// Single-broker, single-dir — we have nowhere to move partitions to.
/// Decode the request to consume the bytes cleanly, then reject every
/// partition with KAFKA_STORAGE_ERROR.
pub fn handle_alter_replica_log_dirs(
    w: &mut Writer,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let dn = r.read_uvarint()?;
    let dir_count = if dn == 0 { 0 } else { dn - 1 };
    let mut topic_layout: Vec<(String, Vec<i32>)> = Vec::new();
    for _ in 0..dir_count {
        let _path = r.read_compact_string()?;
        let tn = r.read_uvarint()?;
        let tcount = if tn == 0 { 0 } else { tn - 1 };
        for _ in 0..tcount {
            let name = r.read_compact_string()?;
            let pn = r.read_uvarint()?;
            let pcount = if pn == 0 { 0 } else { pn - 1 };
            let mut parts = Vec::with_capacity(pcount as usize);
            for _ in 0..pcount {
                parts.push(r.read_i32()?);
            }
            r.read_tagged_fields()?;
            topic_layout.push((name, parts));
        }
        r.read_tagged_fields()?;
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    // Dedupe topic names — the request can repeat them across dirs but
    // the response groups by topic.
    let mut seen: std::collections::BTreeMap<String, Vec<i32>> =
        std::collections::BTreeMap::new();
    for (name, parts) in topic_layout {
        seen.entry(name).or_default().extend(parts);
    }
    w.write_compact_array_len(seen.len() as u32);
    for (name, parts) in seen {
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        for p in parts {
            w.write_i32(p);
            // 56 = KAFKA_STORAGE_ERROR
            w.write_i16(56);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}
