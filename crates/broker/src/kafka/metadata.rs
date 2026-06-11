//! Metadata (api_key = 3).
//!
//! We implement v9-v12 (the flexible-format versions). Earlier versions are
//! technically still in spec but no modern client negotiates them when the
//! broker advertises v9+ as the minimum.
//!
//! Single-broker mode for now: every partition lists node 0 as leader and
//! sole replica. Multi-broker metadata will come once the Raft cluster
//! membership is exposed.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;

pub const API_KEY: i16 = 3;
pub const MIN_VERSION: i16 = 9;
pub const MAX_VERSION: i16 = 12;

/// A topic the client is asking about in a Metadata request.
///
/// `name` is `None` when the client sent only `topic_id` (v10+ feature).
/// We don't implement topic-id resolution yet — we just treat any id-only
/// request as `UNKNOWN_TOPIC_OR_PARTITION` for that entry.
#[derive(Debug)]
pub struct MetadataRequestTopic {
    #[allow(dead_code)] // lookup-by-id not implemented yet
    pub topic_id: [u8; 16],
    pub name: Option<String>,
}

#[derive(Debug)]
pub struct MetadataRequest {
    /// `None` means "all topics". Empty `Some(vec![])` means "no topics".
    pub topics: Option<Vec<MetadataRequestTopic>>,
    #[allow(dead_code)] // auto-create requires a topic catalog write path
    pub allow_auto_topic_creation: bool,
    #[allow(dead_code)] // ACLs not implemented
    pub include_topic_authorized_operations: bool,
}

impl MetadataRequest {
    pub fn decode(version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);

        // Compact nullable array of topics.
        let raw = r.read_uvarint()?;
        let topics = if raw == 0 {
            None
        } else {
            let n = (raw - 1) as usize;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let topic_id = if version >= 10 {
                    let bytes = read_uuid(&mut r)?;
                    bytes
                } else {
                    [0u8; 16]
                };
                let name = r.read_compact_nullable_string()?;
                r.read_tagged_fields()?;
                out.push(MetadataRequestTopic { topic_id, name });
            }
            Some(out)
        };

        let allow_auto_topic_creation = r.read_bool()?;
        // include_cluster_authorized_operations is present in v9-v10, gone in v11+.
        if version >= 9 && version <= 10 {
            let _ = r.read_bool()?;
        }
        let include_topic_authorized_operations = r.read_bool()?;
        r.read_tagged_fields()?;

        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_topic_authorized_operations,
        })
    }
}

fn read_uuid(r: &mut Reader<'_>) -> WireResult<[u8; 16]> {
    let mut id = [0u8; 16];
    for i in 0..16 {
        id[i] = r.read_i8()? as u8;
    }
    Ok(id)
}

/// View of a single broker advertised in the cluster.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// View of a single topic for Metadata responses.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub partition_count: u32,
}

/// Inputs the encoder needs from the broker.
pub struct MetadataView {
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: Vec<BrokerInfo>,
    pub topics: Vec<TopicInfo>,
    /// Names requested but not present. Each will get
    /// UNKNOWN_TOPIC_OR_PARTITION.
    pub missing: Vec<String>,
}

/// Encode a Metadata response body for v9-v12.
pub fn encode_response(w: &mut Writer, version: i16, view: &MetadataView) {
    // throttle_time_ms
    w.write_i32(0);

    // brokers
    w.write_compact_array_len(view.brokers.len() as u32);
    for b in &view.brokers {
        w.write_i32(b.node_id);
        w.write_compact_string(&b.host);
        w.write_i32(b.port);
        w.write_compact_nullable_string(b.rack.as_deref());
        w.write_empty_tagged_fields();
    }

    // cluster_id
    w.write_compact_nullable_string(Some(view.cluster_id.as_str()));
    // controller_id
    w.write_i32(view.controller_id);

    // topics
    let total = view.topics.len() + view.missing.len();
    w.write_compact_array_len(total as u32);

    for topic in &view.topics {
        w.write_i16(error_codes::NONE);
        // Name is required (non-null) up through v11; nullable in v12+.
        if version >= 12 {
            w.write_compact_nullable_string(Some(&topic.name));
        } else {
            w.write_compact_string(&topic.name);
        }
        if version >= 10 {
            write_topic_uuid(w, &topic.name);
        }
        // is_internal
        w.write_bool(false);

        // partitions
        w.write_compact_array_len(topic.partition_count);
        for p in 0..topic.partition_count {
            w.write_i16(error_codes::NONE);
            w.write_i32(p as i32); // partition_index
            w.write_i32(view.controller_id); // leader_id (single-broker)
            w.write_i32(0); // leader_epoch
            w.write_compact_array_len(1);
            w.write_i32(view.controller_id); // replicas
            w.write_compact_array_len(1);
            w.write_i32(view.controller_id); // isr
            w.write_compact_array_len(0); // offline_replicas
            w.write_empty_tagged_fields();
        }
        // topic_authorized_operations — INT32_MIN means "not present"
        w.write_i32(i32::MIN);
        w.write_empty_tagged_fields();
    }

    for name in &view.missing {
        w.write_i16(error_codes::UNKNOWN_TOPIC_OR_PARTITION);
        if version >= 12 {
            w.write_compact_nullable_string(Some(name));
        } else {
            w.write_compact_string(name);
        }
        if version >= 10 {
            write_topic_uuid(w, name);
        }
        w.write_bool(false);
        w.write_compact_array_len(0);
        w.write_i32(i32::MIN);
        w.write_empty_tagged_fields();
    }

    // cluster_authorized_operations is present in v9-v10, gone in v11+.
    if version >= 9 && version <= 10 {
        w.write_i32(i32::MIN);
    }

    w.write_empty_tagged_fields();
}

/// Deterministic 16-byte UUID derived from the topic name.
///
/// Kafka requires topic IDs to be stable across metadata responses for a
/// given topic. We hash the name with a fixed seed so a given name always
/// maps to the same id. A real implementation would persist generated UUIDs;
/// hashing is fine until we add a topic catalog.
fn write_topic_uuid(w: &mut Writer, name: &str) {
    let h1 = xxhash_rust::xxh3::xxh3_64_with_seed(name.as_bytes(), 0xA1B2_C3D4_E5F6_0708);
    let h2 = xxhash_rust::xxh3::xxh3_64_with_seed(name.as_bytes(), 0x1122_3344_5566_7788);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&h1.to_be_bytes());
    bytes[8..].copy_from_slice(&h2.to_be_bytes());
    // Per RFC 4122 §4.4: set version (4) and variant (10xx) bits so the id
    // is a well-formed v4 UUID — some Kafka clients are picky here.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    w.write_bytes_raw(&bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_topics_means_all() {
        // raw varint 0 → null array. allow_auto=false, no include_cluster
        // for v11+, include_topic=false, empty tagged fields.
        let buf = [0u8, 0, 0, 0];
        let req = MetadataRequest::decode(11, &buf).unwrap();
        assert!(req.topics.is_none());
        assert_eq!(req.allow_auto_topic_creation, false);
    }

    #[test]
    fn response_encodes_for_each_version() {
        let view = MetadataView {
            cluster_id: "qx".to_string(),
            controller_id: 0,
            brokers: vec![BrokerInfo {
                node_id: 0,
                host: "127.0.0.1".to_string(),
                port: 9092,
                rack: None,
            }],
            topics: vec![TopicInfo { name: "events".into(), partition_count: 3 }],
            missing: vec![],
        };
        for v in MIN_VERSION..=MAX_VERSION {
            let mut w = Writer::new();
            encode_response(&mut w, v, &view);
            assert!(!w.as_slice().is_empty(), "version {v} produced empty body");
        }
    }
}
