//! Kafka request/response headers.
//!
//! Header version selection is per `(ApiKey, ApiVersion)`. The rules we
//! follow here cover everything we currently implement (ApiVersions,
//! Metadata) and are easy to extend:
//!
//! - Request header v1: legacy non-flexible APIs — `client_id` only.
//! - Request header v2: flexible APIs — `client_id` + tagged fields.
//! - Response header v0: legacy — `correlation_id` only.
//! - Response header v1: flexible — `correlation_id` + tagged fields.
//!
//! **Special case**: the `ApiVersions` *response* always uses response
//! header v0 regardless of negotiated version. This is intentional in the
//! Kafka protocol so that a client can parse the response before it knows
//! whether the broker supports flexible encoding.

use super::codec::{Reader, WireResult, Writer};

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    #[allow(dead_code)] // surfaced once auth/quotas land
    pub client_id: Option<String>,
}

impl RequestHeader {
    /// Decode the leading portion of a frame. Caller must already know
    /// whether the API/version uses a flexible (v2) request header — but
    /// since the api_key + api_version live in the first 4 bytes of the
    /// header itself, we peek those first and then decide.
    pub fn decode(buf: &[u8]) -> WireResult<(Self, usize)> {
        let mut r = Reader::new(buf);
        let api_key = r.read_i16()?;
        let api_version = r.read_i16()?;
        let correlation_id = r.read_i32()?;
        let client_id = r.read_nullable_string()?;
        let flexible = request_header_is_flexible(api_key, api_version);
        if flexible {
            r.read_tagged_fields()?;
        }
        let consumed = buf.len() - r.remaining();
        Ok((
            Self {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            consumed,
        ))
    }
}

/// True if the request header for this (api_key, api_version) carries the
/// trailing tagged-fields appendage (i.e. is "flexible" / header v2).
pub fn request_header_is_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        // Produce: flexible from v9
        0 => api_version >= 9,
        // Fetch: flexible from v12
        1 => api_version >= 12,
        // ListOffsets: flexible from v6
        2 => api_version >= 6,
        // Metadata: flexible from v9
        3 => api_version >= 9,
        // OffsetCommit: flexible from v8
        8 => api_version >= 8,
        // OffsetFetch: flexible from v6
        9 => api_version >= 6,
        // FindCoordinator: flexible from v3
        10 => api_version >= 3,
        // JoinGroup: flexible from v6
        11 => api_version >= 6,
        // Heartbeat: flexible from v4
        12 => api_version >= 4,
        // LeaveGroup: flexible from v4
        13 => api_version >= 4,
        // SyncGroup: flexible from v4
        14 => api_version >= 4,
        // SaslHandshake: NEVER flexible (v0 and v1 are both legacy)
        17 => false,
        // ApiVersions: flexible from v3
        18 => api_version >= 3,
        // CreateTopics: flexible from v5
        19 => api_version >= 5,
        // InitProducerId: flexible from v2
        22 => api_version >= 2,
        // AddPartitionsToTxn: flexible from v3
        24 => api_version >= 3,
        // EndTxn: flexible from v3
        26 => api_version >= 3,
        // WriteTxnMarkers: NEVER flexible (v0 is the only version)
        27 => false,
        // TxnOffsetCommit: flexible from v3
        28 => api_version >= 3,
        // DescribeConfigs: flexible from v4
        32 => api_version >= 4,
        // SaslAuthenticate: flexible from v2
        36 => api_version >= 2,
        // Default: assume flexible-by-default for any unknown API.
        _ => true,
    }
}

/// True if the response body / response header for this (api_key,
/// api_version) is flexible.
///
/// **Beware the `ApiVersions` exception**: even though the *body* is flexible
/// at v3+, the *response header* itself is always v0. Use
/// [`response_header_is_flexible`] for the header decision and
/// [`response_body_is_flexible`] for the body.
pub fn response_body_is_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        0 => api_version >= 9,
        1 => api_version >= 12,
        2 => api_version >= 6,
        3 => api_version >= 9,
        8 => api_version >= 8,
        9 => api_version >= 6,
        10 => api_version >= 3,
        11 => api_version >= 6,
        12 => api_version >= 4,
        13 => api_version >= 4,
        14 => api_version >= 4,
        17 => false, // SaslHandshake — never flexible
        18 => api_version >= 3,
        19 => api_version >= 5,
        22 => api_version >= 2,
        24 => api_version >= 3,
        26 => api_version >= 3,
        27 => false,
        28 => api_version >= 3,
        32 => api_version >= 4,
        36 => api_version >= 2, // SaslAuthenticate — flexible from v2
        _ => true,
    }
}

pub fn response_header_is_flexible(api_key: i16, api_version: i16) -> bool {
    if api_key == 18 {
        // ApiVersions response header is always v0.
        return false;
    }
    response_body_is_flexible(api_key, api_version)
}

/// Write a response header (v0 or v1) with the given correlation id.
pub fn write_response_header(w: &mut Writer, correlation_id: i32, flexible: bool) {
    w.write_i32(correlation_id);
    if flexible {
        w.write_empty_tagged_fields();
    }
}
