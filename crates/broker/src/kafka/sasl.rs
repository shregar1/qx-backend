//! SaslHandshake (17) + SaslAuthenticate (36) — PLAIN mechanism only.
//!
//! Flow a client follows when `KafkaConfig::sasl_plain_users` is configured:
//!
//! 1. `ApiVersions` to learn we speak SASL.
//! 2. `SaslHandshake` to negotiate the mechanism. We advertise only
//!    `PLAIN`; anything else returns `UNSUPPORTED_SASL_MECHANISM`.
//! 3. `SaslAuthenticate` carries the PLAIN payload
//!    (`\0<username>\0<password>`). We look the credentials up against
//!    `sasl_plain_users` and either flip the connection to authenticated
//!    or return `SASL_AUTHENTICATION_FAILED`.
//!
//! Until the connection is authenticated the dispatcher refuses
//! everything except `ApiVersions`, `SaslHandshake`, and
//! `SaslAuthenticate`. Once authenticated, the connection is open to
//! every API for its lifetime.
//!
//! Wire layouts:
//!
//! - **SaslHandshake v1 (non-flexible)**
//!   - Request: `mechanism string`
//!   - Response: `error_code i16, enabled_mechanisms array<string>`
//! - **SaslAuthenticate v2 (flexible)**
//!   - Request: `auth_bytes compact_bytes, tagged_fields`
//!   - Response: `error_code i16, error_message compact_nullable_string,
//!     auth_bytes compact_bytes, session_lifetime_ms i64, tagged_fields`

use std::collections::HashMap;

use super::codec::{Reader, WireResult, Writer};
use super::scram::{self, ScramCredential, ScramRoundResult, ScramState};

// 33 = SASL_AUTHENTICATION_FAILED, 34 = SASL_AUTHENTICATION_FAILED in newer
// builds, 35 = UNSUPPORTED_VERSION. We surface real Kafka codes.
const ERROR_NONE: i16 = 0;
const ERROR_ILLEGAL_SASL_STATE: i16 = 34;
const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;

pub const SASL_HANDSHAKE_KEY: i16 = 17;
pub const SASL_HANDSHAKE_MIN: i16 = 0;
pub const SASL_HANDSHAKE_MAX: i16 = 1;

pub const SASL_AUTHENTICATE_KEY: i16 = 36;
pub const SASL_AUTHENTICATE_MIN: i16 = 0;
pub const SASL_AUTHENTICATE_MAX: i16 = 2;

/// Per-connection auth state. The dispatcher hands this to handlers as
/// `&mut` so SaslHandshake / SaslAuthenticate can mutate it, and consults
/// `.authenticated` before routing data-plane requests.
#[derive(Debug, Default)]
pub struct ConnectionState {
    pub authenticated: bool,
    pub selected_mechanism: Option<String>,
    /// Authenticated principal (the username that authenticated). `None`
    /// for anonymous listeners. Surfaces into ACL checks once enforcement
    /// is wired through dispatch.
    pub principal: Option<String>,
    /// SCRAM mid-conversation state. Cleared back to default on success
    /// or failure.
    pub scram: ScramState,
}

impl ConnectionState {
    /// New state for a fresh connection. `sasl_required` flips the initial
    /// `authenticated` flag: if no SASL config is set, the connection is
    /// already considered authenticated (anonymous listener).
    pub fn new(sasl_required: bool) -> Self {
        Self {
            authenticated: !sasl_required,
            selected_mechanism: None,
            principal: None,
            scram: ScramState::default(),
        }
    }
}

// ───────────────────────────── SaslHandshake ────────────────────────────

pub fn handle_sasl_handshake(
    w: &mut Writer,
    state: &mut ConnectionState,
    api_version: i16,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    // Both v0 and v1 use regular non-compact strings, no tagged_fields.
    let mechanism = r.read_string()?;

    let supported = &["PLAIN", "SCRAM-SHA-256"];
    let code = if supported.iter().any(|m| *m == mechanism) {
        state.selected_mechanism = Some(mechanism);
        ERROR_NONE
    } else {
        ERROR_UNSUPPORTED_SASL_MECHANISM
    };

    w.write_i16(code);
    w.write_array_len(supported.len() as i32);
    for m in supported {
        w.write_string(m);
    }
    let _ = api_version;
    Ok(())
}

// ──────────────────────────── SaslAuthenticate ──────────────────────────

pub fn handle_sasl_authenticate(
    w: &mut Writer,
    state: &mut ConnectionState,
    api_version: i16,
    body: &[u8],
    plain_users: Option<&HashMap<String, String>>,
    scram_users: Option<&HashMap<String, ScramCredential>>,
) -> WireResult<()> {
    let auth_bytes_opt = if api_version >= 2 {
        let mut r = Reader::new(body);
        let b = r.read_compact_bytes()?;
        r.read_tagged_fields()?;
        b
    } else {
        let mut r = Reader::new(body);
        r.read_bytes()?
    };

    let mechanism = state.selected_mechanism.clone();
    let mut response_bytes: Vec<u8> = Vec::new();
    let (code, message): (i16, Option<String>) = match mechanism.as_deref() {
        Some("PLAIN") => match auth_bytes_opt {
            None => (
                ERROR_SASL_AUTHENTICATION_FAILED,
                Some("empty auth bytes".to_string()),
            ),
            Some(bytes) => match parse_plain(&bytes) {
                None => (
                    ERROR_SASL_AUTHENTICATION_FAILED,
                    Some("malformed PLAIN auth bytes".to_string()),
                ),
                Some((user, pass)) => match plain_users {
                    None => (
                        ERROR_SASL_AUTHENTICATION_FAILED,
                        Some("no PLAIN credentials configured".to_string()),
                    ),
                    Some(map) => match map.get(&user) {
                        Some(expected)
                            if constant_time_eq(expected.as_bytes(), pass.as_bytes()) =>
                        {
                            state.authenticated = true;
                            state.principal = Some(user);
                            (ERROR_NONE, None)
                        }
                        _ => (
                            ERROR_SASL_AUTHENTICATION_FAILED,
                            Some("invalid credentials".to_string()),
                        ),
                    },
                },
            },
        },
        Some("SCRAM-SHA-256") => {
            let empty: HashMap<String, ScramCredential> = HashMap::new();
            let map = scram_users.unwrap_or(&empty);
            match auth_bytes_opt {
                None => (
                    ERROR_SASL_AUTHENTICATION_FAILED,
                    Some("empty SCRAM auth bytes".to_string()),
                ),
                Some(bytes) => {
                    let result = scram::handle_round(&mut state.scram, map, &bytes);
                    match result {
                        ScramRoundResult::Continue { server_message } => {
                            response_bytes = server_message;
                            (ERROR_NONE, None)
                        }
                        ScramRoundResult::Success { server_message } => {
                            response_bytes = server_message;
                            state.authenticated = true;
                            state.principal = state.scram.username.clone();
                            (ERROR_NONE, None)
                        }
                        ScramRoundResult::Failure { server_message } => {
                            // Surface the SCRAM "e=..." error code as the message
                            // string for librdkafka to log.
                            let msg = String::from_utf8_lossy(&server_message).into_owned();
                            (ERROR_SASL_AUTHENTICATION_FAILED, Some(msg))
                        }
                    }
                }
            }
        }
        _ => (
            ERROR_ILLEGAL_SASL_STATE,
            Some("call SaslHandshake first".to_string()),
        ),
    };

    if api_version >= 2 {
        w.write_i16(code);
        w.write_compact_nullable_string(message.as_deref());
        w.write_compact_bytes(Some(&response_bytes));
        w.write_i64(0); // session_lifetime_ms (0 = no reauth required)
        w.write_empty_tagged_fields();
    } else {
        w.write_i16(code);
        w.write_nullable_string(message.as_deref());
        // Non-flexible bytes: i32 length + payload.
        w.write_i32(response_bytes.len() as i32);
        if !response_bytes.is_empty() {
            w.write_bytes_raw(&response_bytes);
        }
        if api_version >= 1 {
            w.write_i64(0); // session_lifetime_ms (v1+)
        }
    }
    Ok(())
}

/// Parse the SASL/PLAIN payload: `\0<authzid?>\0<username>\0<password>`.
/// `authzid` is typically empty for Kafka; we just ignore it.
fn parse_plain(bytes: &[u8]) -> Option<(String, String)> {
    let mut parts = bytes.split(|&b| b == 0);
    let _authzid = parts.next()?;
    let username = std::str::from_utf8(parts.next()?).ok()?.to_string();
    let password = std::str::from_utf8(parts.next()?).ok()?.to_string();
    if parts.next().is_some() {
        return None;
    }
    Some((username, password))
}

/// Length-tolerant equality check that always touches the full input so
/// timing leaks don't carry the secret. Returns `false` on length
/// mismatch (which is by itself revealing only the length, not the
/// bytes).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// True if this `(api_key, version)` is allowed before SASL authentication
/// completes. ApiVersions + the two SASL APIs only.
pub fn allowed_pre_auth(api_key: i16) -> bool {
    api_key == 18 // ApiVersions
        || api_key == SASL_HANDSHAKE_KEY
        || api_key == SASL_AUTHENTICATE_KEY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_typical() {
        let bytes = b"\0alice\0sesame";
        assert_eq!(
            parse_plain(bytes),
            Some(("alice".to_string(), "sesame".to_string()))
        );
    }

    #[test]
    fn parse_plain_with_authzid() {
        let bytes = b"alice\0alice\0sesame";
        assert_eq!(
            parse_plain(bytes),
            Some(("alice".to_string(), "sesame".to_string()))
        );
    }

    #[test]
    fn parse_plain_missing_password() {
        let bytes = b"\0alice";
        assert!(parse_plain(bytes).is_none());
    }
}
