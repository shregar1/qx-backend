//! ApiVersions (api_key = 18).
//!
//! Always the first request a Kafka client sends — the broker advertises
//! every API it speaks, with min/max supported versions, so the client can
//! pick a compatible serialization.
//!
//! We deliberately advertise *only* the APIs we actually implement. A modern
//! client that asks for, say, Produce, will then get
//! `UNSUPPORTED_VERSION` back if it tries — that's an honest signal.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;

pub const API_KEY: i16 = 18;

/// One entry of `ApiVersions` advertised back to the client.
#[derive(Debug, Clone, Copy)]
pub struct SupportedApi {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// The set of APIs qx advertises today. As we light up more handlers, add
/// entries here — clients will only call APIs that appear in this list.
pub fn supported_apis() -> &'static [SupportedApi] {
    const APIS: &[SupportedApi] = &[
        SupportedApi { api_key: 0, min_version: 9, max_version: 9 },   // Produce
        SupportedApi { api_key: 1, min_version: 12, max_version: 12 }, // Fetch
        SupportedApi { api_key: 2, min_version: 1, max_version: 7 },   // ListOffsets
        SupportedApi { api_key: 3, min_version: 9, max_version: 12 },  // Metadata
        SupportedApi { api_key: 8, min_version: 8, max_version: 8 },   // OffsetCommit
        SupportedApi { api_key: 9, min_version: 8, max_version: 8 },   // OffsetFetch
        SupportedApi { api_key: 10, min_version: 3, max_version: 3 },  // FindCoordinator
        SupportedApi { api_key: 11, min_version: 9, max_version: 9 },  // JoinGroup
        SupportedApi { api_key: 12, min_version: 4, max_version: 4 },  // Heartbeat
        SupportedApi { api_key: 13, min_version: 4, max_version: 4 },  // LeaveGroup
        SupportedApi { api_key: 14, min_version: 5, max_version: 5 },  // SyncGroup
        SupportedApi { api_key: 15, min_version: 5, max_version: 5 },  // DescribeGroups
        SupportedApi { api_key: 16, min_version: 4, max_version: 4 },  // ListGroups
        SupportedApi { api_key: 17, min_version: 0, max_version: 1 },  // SaslHandshake
        SupportedApi { api_key: 18, min_version: 0, max_version: 3 },  // ApiVersions
        SupportedApi { api_key: 19, min_version: 5, max_version: 7 },  // CreateTopics
        SupportedApi { api_key: 20, min_version: 6, max_version: 6 },  // DeleteTopics
        SupportedApi { api_key: 21, min_version: 2, max_version: 2 },  // DeleteRecords
        SupportedApi { api_key: 22, min_version: 4, max_version: 4 },  // InitProducerId
        SupportedApi { api_key: 23, min_version: 4, max_version: 4 },  // OffsetForLeaderEpoch
        SupportedApi { api_key: 24, min_version: 3, max_version: 3 },  // AddPartitionsToTxn
        SupportedApi { api_key: 25, min_version: 3, max_version: 3 },  // AddOffsetsToTxn
        SupportedApi { api_key: 26, min_version: 3, max_version: 3 },  // EndTxn
        SupportedApi { api_key: 27, min_version: 0, max_version: 0 },  // WriteTxnMarkers
        SupportedApi { api_key: 28, min_version: 3, max_version: 3 },  // TxnOffsetCommit
        SupportedApi { api_key: 29, min_version: 3, max_version: 3 },  // DescribeAcls
        SupportedApi { api_key: 30, min_version: 3, max_version: 3 },  // CreateAcls
        SupportedApi { api_key: 31, min_version: 3, max_version: 3 },  // DeleteAcls
        SupportedApi { api_key: 32, min_version: 4, max_version: 4 },  // DescribeConfigs
        SupportedApi { api_key: 33, min_version: 2, max_version: 2 },  // AlterConfigs
        SupportedApi { api_key: 34, min_version: 2, max_version: 2 },  // AlterReplicaLogDirs
        SupportedApi { api_key: 35, min_version: 3, max_version: 3 },  // DescribeLogDirs
        SupportedApi { api_key: 36, min_version: 0, max_version: 2 },  // SaslAuthenticate
        SupportedApi { api_key: 37, min_version: 3, max_version: 3 },  // CreatePartitions
        SupportedApi { api_key: 44, min_version: 1, max_version: 1 },  // IncrementalAlterConfigs
        SupportedApi { api_key: 45, min_version: 0, max_version: 0 },  // AlterPartitionReassignments
        SupportedApi { api_key: 46, min_version: 0, max_version: 0 },  // ListPartitionReassignments
        SupportedApi { api_key: 47, min_version: 0, max_version: 0 },  // OffsetDelete
        SupportedApi { api_key: 48, min_version: 1, max_version: 1 },  // DescribeClientQuotas
        SupportedApi { api_key: 49, min_version: 1, max_version: 1 },  // AlterClientQuotas
        SupportedApi { api_key: 50, min_version: 0, max_version: 0 },  // DescribeUserScramCredentials
        SupportedApi { api_key: 51, min_version: 0, max_version: 0 },  // AlterUserScramCredentials
        SupportedApi { api_key: 55, min_version: 1, max_version: 1 },  // DescribeQuorum
        SupportedApi { api_key: 60, min_version: 0, max_version: 0 },  // DescribeCluster
        SupportedApi { api_key: 61, min_version: 0, max_version: 0 },  // DescribeProducers
        SupportedApi { api_key: 65, min_version: 0, max_version: 0 },  // DescribeTransactions
        SupportedApi { api_key: 66, min_version: 0, max_version: 0 },  // ListTransactions
    ];
    APIS
}

/// `ApiVersions` request — the body is empty in v0-v2 and carries a
/// `client_software_name` / `client_software_version` pair in v3+.
#[derive(Debug, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    pub fn decode(version: i16, buf: &[u8]) -> WireResult<Self> {
        if version < 3 {
            return Ok(Self::default());
        }
        let mut r = Reader::new(buf);
        let name = r.read_compact_string()?;
        let ver = r.read_compact_string()?;
        r.read_tagged_fields()?;
        Ok(Self {
            client_software_name: Some(name),
            client_software_version: Some(ver),
        })
    }
}

/// Encode the `ApiVersions` response body for the given version.
///
/// Layout:
///   v0:  error_code(i16) + array<api_versions>
///   v1+: + throttle_time_ms(i32)
///   v3+: flexible (compact_array, tagged_fields)
pub fn encode_response(w: &mut Writer, version: i16) {
    let apis = supported_apis();
    w.write_i16(error_codes::NONE);

    if version >= 3 {
        w.write_compact_array_len(apis.len() as u32);
        for api in apis {
            w.write_i16(api.api_key);
            w.write_i16(api.min_version);
            w.write_i16(api.max_version);
            w.write_empty_tagged_fields();
        }
        w.write_i32(0); // throttle_time_ms
        w.write_empty_tagged_fields();
        return;
    }

    w.write_array_len(apis.len() as i32);
    for api in apis {
        w.write_i16(api.api_key);
        w.write_i16(api.min_version);
        w.write_i16(api.max_version);
    }
    if version >= 1 {
        w.write_i32(0); // throttle_time_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_v3_layout_smoke() {
        // Quick smoke: ensure we can produce a buffer without panicking and
        // that the first two bytes are the error code (0).
        let mut w = Writer::new();
        encode_response(&mut w, 3);
        let b = w.into_bytes();
        assert_eq!(&b[..2], &[0, 0]);
        assert!(b.len() > 4);
    }

    #[test]
    fn supported_apis_contains_metadata_and_api_versions() {
        let keys: Vec<i16> = supported_apis().iter().map(|a| a.api_key).collect();
        assert!(keys.contains(&3));
        assert!(keys.contains(&18));
    }
}
