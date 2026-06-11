//! DescribeCluster (api_key = 60), v0.
//!
//! Modern admin clients (kafka-cluster, AdminClient.describeCluster) hit
//! this on startup. v0 body (flexible):
//!
//! ```text
//! include_cluster_authorized_operations bool
//! tagged_fields
//! ```
//!
//! Response:
//! ```text
//! throttle_time_ms i32
//! error_code i16
//! error_message compact_nullable_string
//! cluster_id compact_string
//! controller_id i32
//! brokers compact_array<{
//!   broker_id i32
//!   host compact_string
//!   port i32
//!   rack compact_nullable_string
//!   tagged_fields
//! }>
//! cluster_authorized_operations i32
//! tagged_fields
//! ```

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;

pub const API_KEY: i16 = 60;
pub const MIN_VERSION: i16 = 0;
pub const MAX_VERSION: i16 = 0;

#[derive(Debug)]
pub struct DescribeClusterRequest {
    #[allow(dead_code)]
    pub include_cluster_authorized_operations: bool,
}

impl DescribeClusterRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        let include = r.read_bool()?;
        r.read_tagged_fields()?;
        Ok(Self {
            include_cluster_authorized_operations: include,
        })
    }
}

pub fn handle_and_encode(
    w: &mut Writer,
    config: &super::server::KafkaConfig,
) {
    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None); // error_message
    // Stable per-process cluster_id. Real Kafka returns a uuid string;
    // we just pin to our advertised host so it's deterministic.
    let cluster_id = format!("qx-{}", config.advertised_host);
    w.write_compact_string(&cluster_id);
    w.write_i32(config.node_id); // controller_id — single broker is its own controller
    // brokers: just us, single-broker today.
    w.write_compact_array_len(1);
    w.write_i32(config.node_id);
    w.write_compact_string(&config.advertised_host);
    w.write_i32(config.advertised_port);
    w.write_compact_nullable_string(None); // rack
    w.write_empty_tagged_fields();
    // -2147483648 (i32::MIN) means "authorized operations not present"
    // per the protocol. We don't have ACLs, so signal absence.
    w.write_i32(i32::MIN);
    w.write_empty_tagged_fields();
}
