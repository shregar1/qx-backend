//! AlterConfigs (api_key = 33), v2 (the legacy non-incremental variant).
//!
//! Modern admin clients prefer IncrementalAlterConfigs (44), but plenty
//! of tools still hit v2. We accept the request, log nothing, and
//! return success. We have no per-topic configurable knobs that map
//! cleanly onto Kafka's bag of strings yet (retention, cleanup.policy,
//! min.insync.replicas, etc.), so honoring the request silently is the
//! honest call — the response says "applied" but really there's no
//! state to mutate. Anything substantive (compaction policy, retention
//! window) needs work in qx-topic first.
//!
//! Request layout (v2, flexible):
//!
//! ```text
//! resources compact_array<{
//!   resource_type i8
//!   resource_name compact_string
//!   configs compact_array<{
//!     name  compact_string
//!     value compact_nullable_string
//!     tagged_fields
//!   }>
//!   tagged_fields
//! }>
//! validate_only bool
//! tagged_fields
//! ```
//!
//! Response:
//! ```text
//! throttle_time_ms i32
//! responses compact_array<{
//!   error_code     i16
//!   error_message  compact_nullable_string
//!   resource_type  i8
//!   resource_name  compact_string
//!   tagged_fields
//! }>
//! tagged_fields
//! ```

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 33;
pub const MIN_VERSION: i16 = 2;
pub const MAX_VERSION: i16 = 2;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

#[derive(Debug)]
pub struct AlterConfigsRequest {
    pub resources: Vec<AlterConfigsResource>,
    #[allow(dead_code)]
    pub validate_only: bool,
}

#[derive(Debug)]
pub struct AlterConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    #[allow(dead_code)]
    pub configs: Vec<(String, Option<String>)>,
}

impl AlterConfigsRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let n = r.read_uvarint()?;
        let resource_count = if n == 0 { 0 } else { n - 1 };
        let mut resources = Vec::with_capacity(resource_count as usize);
        for _ in 0..resource_count {
            let resource_type = r.read_i8()?;
            let resource_name = r.read_compact_string()?;
            let cn = r.read_uvarint()?;
            let cfg_count = if cn == 0 { 0 } else { cn - 1 };
            let mut configs = Vec::with_capacity(cfg_count as usize);
            for _ in 0..cfg_count {
                let name = r.read_compact_string()?;
                let value = r.read_compact_nullable_string()?;
                r.read_tagged_fields()?;
                configs.push((name, value));
            }
            r.read_tagged_fields()?;
            resources.push(AlterConfigsResource {
                resource_type,
                resource_name,
                configs,
            });
        }
        let validate_only = r.read_bool()?;
        r.read_tagged_fields()?;
        Ok(Self { resources, validate_only })
    }
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, req: AlterConfigsRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.resources.len() as u32);

    for res in req.resources {
        let (error_code, error_message): (i16, Option<&str>) = match res.resource_type {
            RESOURCE_TYPE_TOPIC => {
                if broker.lookup(&res.resource_name).is_some() {
                    // No-op apply. We don't reject unknown keys — Kafka
                    // accepts any string for forward-compat with newer
                    // clients that send keys we don't model yet.
                    (error_codes::NONE, None)
                } else {
                    (
                        error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                        Some("topic not found"),
                    )
                }
            }
            RESOURCE_TYPE_BROKER => {
                // Broker-level alters are accepted without effect.
                (error_codes::NONE, None)
            }
            _ => (error_codes::INVALID_REQUEST, Some("unsupported resource type")),
        };
        w.write_i16(error_code);
        w.write_compact_nullable_string(error_message);
        w.write_i8(res.resource_type);
        w.write_compact_string(&res.resource_name);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
}
