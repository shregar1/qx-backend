//! DescribeConfigs (api_key = 32), v4.
//!
//! Required by `kafka-topics --describe`, `kafka-configs --describe`, and
//! several admin client paths. We return a plausible set of topic configs
//! (retention, cleanup policy, etc.) and a tiny set of broker configs.
//!
//! Resource types we recognize: 2 = topic, 4 = broker. Anything else gets
//! `INVALID_REQUEST`.
//!
//! Config source codes (per spec):
//!   1 = DYNAMIC_TOPIC_CONFIG
//!   4 = STATIC_BROKER_CONFIG
//!   5 = DEFAULT_CONFIG

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 32;
pub const MIN_VERSION: i16 = 4;
pub const MAX_VERSION: i16 = 4;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

const SOURCE_DYNAMIC_TOPIC: i8 = 1;
const SOURCE_STATIC_BROKER: i8 = 4;
const SOURCE_DEFAULT: i8 = 5;

const CONFIG_TYPE_STRING: i8 = 2;
const CONFIG_TYPE_LONG: i8 = 5;
const CONFIG_TYPE_BOOLEAN: i8 = 1;

#[derive(Debug)]
pub struct DescribeConfigsRequest {
    pub resources: Vec<DescribeConfigsResource>,
    #[allow(dead_code)]
    pub include_synonyms: bool,
    #[allow(dead_code)]
    pub include_documentation: bool,
}

#[derive(Debug)]
pub struct DescribeConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    /// `None` = "all configs", `Some(empty)` = "no configs". We return all
    /// in both cases since the filtered case is rarely useful.
    #[allow(dead_code)]
    pub configuration_keys: Option<Vec<String>>,
}

impl DescribeConfigsRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let n = r.read_uvarint()?;
        let count = if n == 0 { 0 } else { n - 1 };
        let mut resources = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let resource_type = r.read_i8()?;
            let resource_name = r.read_compact_string()?;
            let kn = r.read_uvarint()?;
            let configuration_keys = if kn == 0 {
                None
            } else {
                let kc = kn - 1;
                let mut keys = Vec::with_capacity(kc as usize);
                for _ in 0..kc {
                    keys.push(r.read_compact_string()?);
                }
                Some(keys)
            };
            r.read_tagged_fields()?;
            resources.push(DescribeConfigsResource {
                resource_type,
                resource_name,
                configuration_keys,
            });
        }
        let include_synonyms = r.read_bool()?;
        let include_documentation = r.read_bool()?;
        r.read_tagged_fields()?;
        Ok(Self {
            resources,
            include_synonyms,
            include_documentation,
        })
    }
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, req: DescribeConfigsRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.resources.len() as u32);

    for resource in req.resources {
        match resource.resource_type {
            RESOURCE_TYPE_TOPIC => write_topic_result(w, broker, &resource.resource_name),
            RESOURCE_TYPE_BROKER => write_broker_result(w, &resource.resource_name),
            other => write_invalid_result(w, other, &resource.resource_name),
        }
    }
    w.write_empty_tagged_fields();
}

fn write_topic_result(w: &mut Writer, broker: &Broker, name: &str) {
    let exists = broker.lookup(name).is_some();
    if !exists {
        w.write_i16(error_codes::UNKNOWN_TOPIC_OR_PARTITION);
        w.write_compact_nullable_string(Some("topic not found"));
        w.write_i8(RESOURCE_TYPE_TOPIC);
        w.write_compact_string(name);
        w.write_compact_array_len(0);
        w.write_empty_tagged_fields();
        return;
    }

    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);
    w.write_i8(RESOURCE_TYPE_TOPIC);
    w.write_compact_string(name);

    // A small set of plausible topic configs. qx's storage is append-only
    // with no compaction/retention today, so retention is "forever" and
    // cleanup.policy is "delete" (no compaction).
    let configs: &[(&str, &str, i8, i8)] = &[
        ("cleanup.policy", "delete", SOURCE_DEFAULT, CONFIG_TYPE_STRING),
        ("retention.ms", "-1", SOURCE_DEFAULT, CONFIG_TYPE_LONG),
        ("retention.bytes", "-1", SOURCE_DEFAULT, CONFIG_TYPE_LONG),
        ("segment.bytes", "1073741824", SOURCE_DEFAULT, CONFIG_TYPE_LONG),
        ("max.message.bytes", "1048588", SOURCE_DEFAULT, CONFIG_TYPE_LONG),
        ("min.insync.replicas", "1", SOURCE_DEFAULT, CONFIG_TYPE_LONG),
        ("compression.type", "producer", SOURCE_DEFAULT, CONFIG_TYPE_STRING),
        ("message.timestamp.type", "CreateTime", SOURCE_DEFAULT, CONFIG_TYPE_STRING),
    ];

    w.write_compact_array_len(configs.len() as u32);
    for (k, v, source, ty) in configs {
        write_config_entry(w, k, Some(v), *source, *ty);
    }
    w.write_empty_tagged_fields();
}

fn write_broker_result(w: &mut Writer, name: &str) {
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);
    w.write_i8(RESOURCE_TYPE_BROKER);
    w.write_compact_string(name);

    let configs: &[(&str, &str, i8, i8)] = &[
        ("broker.id", name, SOURCE_STATIC_BROKER, CONFIG_TYPE_LONG),
        ("num.partitions", "1", SOURCE_STATIC_BROKER, CONFIG_TYPE_LONG),
        ("default.replication.factor", "1", SOURCE_STATIC_BROKER, CONFIG_TYPE_LONG),
        ("auto.create.topics.enable", "false", SOURCE_STATIC_BROKER, CONFIG_TYPE_BOOLEAN),
        ("compression.type", "producer", SOURCE_STATIC_BROKER, CONFIG_TYPE_STRING),
    ];
    w.write_compact_array_len(configs.len() as u32);
    for (k, v, source, ty) in configs {
        write_config_entry(w, k, Some(v), *source, *ty);
    }
    w.write_empty_tagged_fields();
}

fn write_invalid_result(w: &mut Writer, kind: i8, name: &str) {
    w.write_i16(error_codes::INVALID_REQUEST);
    w.write_compact_nullable_string(Some("unsupported resource_type"));
    w.write_i8(kind);
    w.write_compact_string(name);
    w.write_compact_array_len(0);
    w.write_empty_tagged_fields();
}

fn write_config_entry(
    w: &mut Writer,
    name: &str,
    value: Option<&str>,
    source: i8,
    config_type: i8,
) {
    w.write_compact_string(name);
    w.write_compact_nullable_string(value);
    w.write_bool(false); // read_only
    w.write_i8(source);
    w.write_bool(false); // is_sensitive
    w.write_compact_array_len(0); // synonyms: empty
    w.write_i8(config_type);
    w.write_compact_nullable_string(None); // documentation
    w.write_empty_tagged_fields();
}
