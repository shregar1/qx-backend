//! SCRAM-SHA-256 server implementation (RFC 5802).
//!
//! Two-round SASL mechanism:
//!
//! ```text
//! Client → "n,,n=<user>,r=<client_nonce>"
//! Server ← "r=<combined_nonce>,s=<base64_salt>,i=<iterations>"
//! Client → "c=biws,r=<combined_nonce>,p=<base64_client_proof>"
//! Server ← "v=<base64_server_signature>"   on success
//!         "e=<error>"                       on failure
//! ```
//!
//! Per RFC 5802, the server stores only `(salt, iterations, StoredKey,
//! ServerKey)` per user — no plaintext password and no `SaltedPassword`.
//! Our `ScramCredential` carries exactly those four. The broker config
//! lets operators ship plaintext at deploy time; we derive credentials
//! at startup with PBKDF2 and never keep the plaintext past that.
//!
//! Differences from RFC for sanity:
//!   - We require `n,,...` GS2 header (no channel binding). Real clients
//!     send `y,,` only when they would otherwise need channel binding,
//!     which Kafka doesn't.
//!   - Optional extensions in client-first/server-first are ignored.

use std::collections::HashMap;
use std::fmt::Write as _;

use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

const SHA256_LEN: usize = 32;
const DEFAULT_ITERATIONS: u32 = 4096;
const NONCE_BYTES: usize = 18;
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Stored credential per user: enough to verify a client proof and emit
/// the server signature, but not enough to impersonate the client to
/// another SCRAM server.
#[derive(Debug, Clone)]
pub struct ScramCredential {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; SHA256_LEN],
    pub server_key: [u8; SHA256_LEN],
}

impl ScramCredential {
    /// Derive a fresh credential from a plaintext password, using a
    /// random 16-byte salt and `DEFAULT_ITERATIONS` rounds of PBKDF2.
    /// Called by the broker at startup for any user the operator
    /// supplied in plaintext.
    pub fn from_password(password: &str) -> Self {
        let mut salt = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        Self::derive(password, salt, DEFAULT_ITERATIONS)
    }

    /// Construct from a known `(password, salt, iterations)` triple.
    /// Used by tests and by any future credentials-file loader.
    pub fn derive(password: &str, salt: Vec<u8>, iterations: u32) -> Self {
        let mut salted = [0u8; SHA256_LEN];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            password.as_bytes(),
            &salt,
            iterations,
            &mut salted,
        );
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac(&salted, b"Server Key");
        Self {
            salt,
            iterations,
            stored_key,
            server_key,
        }
    }
}

/// Per-connection SCRAM negotiation state.
#[derive(Debug, Default)]
pub struct ScramState {
    pub stage: ScramStage,
    /// Username from client-first.
    pub username: Option<String>,
    /// Concatenated client_nonce + server_nonce we sent in server-first.
    pub combined_nonce: Option<String>,
    /// Raw client-first-bare ("n=...,r=...") for `AuthMessage`.
    pub client_first_bare: Option<String>,
    /// Raw server-first ("r=...,s=...,i=...") for `AuthMessage`.
    pub server_first: Option<String>,
    /// Resolved credential after username lookup, so round 2 doesn't
    /// re-hash through the password store.
    pub credential: Option<ScramCredential>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ScramStage {
    #[default]
    Initial,
    AwaitingClientFinal,
    Done,
    Failed,
}

/// Top-level dispatch for the auth_bytes inside a SaslAuthenticate
/// request. Returns the server response bytes plus whether the auth has
/// completed (success or failure). The caller mutates connection-level
/// `authenticated` based on the success bool.
pub enum ScramRoundResult {
    /// Send these bytes back to the client; auth continues with a
    /// second round.
    Continue { server_message: Vec<u8> },
    /// Auth succeeded. Flip the connection to authenticated.
    Success { server_message: Vec<u8> },
    /// Auth failed. Send the error message (or empty) and treat the
    /// connection as unauthenticated.
    Failure { server_message: Vec<u8> },
}

pub fn handle_round(
    state: &mut ScramState,
    users: &HashMap<String, ScramCredential>,
    auth_bytes: &[u8],
) -> ScramRoundResult {
    match state.stage {
        ScramStage::Initial => handle_client_first(state, users, auth_bytes),
        ScramStage::AwaitingClientFinal => handle_client_final(state, auth_bytes),
        ScramStage::Done | ScramStage::Failed => ScramRoundResult::Failure {
            server_message: error_msg("invalid-state"),
        },
    }
}

fn handle_client_first(
    state: &mut ScramState,
    users: &HashMap<String, ScramCredential>,
    auth_bytes: &[u8],
) -> ScramRoundResult {
    let Ok(s) = std::str::from_utf8(auth_bytes) else {
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("invalid-encoding"),
        };
    };
    // GS2 header is everything up to the first `,,` — typically "n,," or
    // "y,," (no channel binding) or "p=<cb>,,...". We only accept "n,,".
    let body = match s.strip_prefix("n,,") {
        Some(rest) => rest,
        None => {
            state.stage = ScramStage::Failed;
            return ScramRoundResult::Failure {
                server_message: error_msg("channel-binding-not-supported"),
            };
        }
    };

    let mut username: Option<&str> = None;
    let mut client_nonce: Option<&str> = None;
    for attr in body.split(',') {
        match attr.split_once('=') {
            Some(("n", v)) => username = Some(v),
            Some(("r", v)) => client_nonce = Some(v),
            _ => {}
        }
    }
    let (Some(user), Some(cnonce)) = (username, client_nonce) else {
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("invalid-encoding"),
        };
    };

    // Look up the user. Per RFC 5802 §7 (error condition), we should
    // still complete a round with a synthetic credential to avoid
    // username-existence side channels. We do that.
    let credential = users
        .get(user)
        .cloned()
        .unwrap_or_else(|| ScramCredential::derive(user, vec![0u8; 16], DEFAULT_ITERATIONS));

    let server_nonce = random_printable_nonce();
    let combined_nonce = format!("{cnonce}{server_nonce}");
    let salt_b64 = B64.encode(&credential.salt);
    let server_first = format!("r={combined_nonce},s={salt_b64},i={}", credential.iterations);

    state.stage = ScramStage::AwaitingClientFinal;
    state.username = Some(user.to_string());
    state.combined_nonce = Some(combined_nonce);
    state.client_first_bare = Some(body.to_string());
    state.server_first = Some(server_first.clone());
    state.credential = Some(credential);

    ScramRoundResult::Continue {
        server_message: server_first.into_bytes(),
    }
}

fn handle_client_final(state: &mut ScramState, auth_bytes: &[u8]) -> ScramRoundResult {
    let Ok(s) = std::str::from_utf8(auth_bytes) else {
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("invalid-encoding"),
        };
    };

    let mut channel_binding: Option<&str> = None;
    let mut nonce: Option<&str> = None;
    let mut proof: Option<&str> = None;
    for attr in s.split(',') {
        match attr.split_once('=') {
            // The proof itself contains '=' inside base64; `split_once` keeps
            // only the head so this is safe.
            Some(("c", v)) => channel_binding = Some(v),
            Some(("r", v)) => nonce = Some(v),
            Some(("p", v)) => proof = Some(v),
            _ => {}
        }
    }

    let (Some(cb), Some(combined), Some(proof_b64)) = (channel_binding, nonce, proof) else {
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("invalid-encoding"),
        };
    };

    if cb != "biws" {
        // "biws" = base64("n,,") — the only channel binding we accept.
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("channel-bindings-dont-match"),
        };
    }

    let expected_nonce = state.combined_nonce.as_deref().unwrap_or("");
    if combined != expected_nonce {
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("invalid-nonce"),
        };
    }

    let client_proof = match B64.decode(proof_b64) {
        Ok(b) if b.len() == SHA256_LEN => b,
        _ => {
            state.stage = ScramStage::Failed;
            return ScramRoundResult::Failure {
                server_message: error_msg("invalid-proof"),
            };
        }
    };

    let credential = state.credential.clone().unwrap();
    let client_first_bare = state.client_first_bare.clone().unwrap();
    let server_first = state.server_first.clone().unwrap();

    // Client-final-no-proof: everything up to `,p=` literally.
    let p_index = s.find(",p=").unwrap_or(s.len());
    let client_final_no_proof = &s[..p_index];

    let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");

    let client_signature = hmac(&credential.stored_key, auth_message.as_bytes());
    let recovered_client_key = xor(&client_proof, &client_signature);
    let recovered_stored_key = sha256(&recovered_client_key);

    if !constant_time_eq(&recovered_stored_key, &credential.stored_key) {
        state.stage = ScramStage::Failed;
        return ScramRoundResult::Failure {
            server_message: error_msg("invalid-proof"),
        };
    }

    let server_signature = hmac(&credential.server_key, auth_message.as_bytes());
    let server_final = format!("v={}", B64.encode(server_signature));
    state.stage = ScramStage::Done;
    ScramRoundResult::Success {
        server_message: server_final.into_bytes(),
    }
}

fn error_msg(code: &str) -> Vec<u8> {
    let mut s = String::new();
    write!(&mut s, "e={code}").unwrap();
    s.into_bytes()
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; SHA256_LEN] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; SHA256_LEN] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

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

fn random_printable_nonce() -> String {
    let mut bytes = [0u8; NONCE_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    // Filter to ASCII printable (33..127) minus ',' since SCRAM uses ','
    // as a delimiter. Easy: base64 the random bytes.
    B64.encode(bytes).replace(',', "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_succeeds_with_correct_password() {
        let cred = ScramCredential::from_password("sesame");
        let mut users = HashMap::new();
        users.insert("alice".to_string(), cred.clone());
        let mut state = ScramState::default();

        // Round 1: client-first.
        let r1 = handle_round(&mut state, &users, b"n,,n=alice,r=abc123");
        let server_first = match r1 {
            ScramRoundResult::Continue { server_message } => server_message,
            _ => panic!("expected Continue"),
        };
        let server_first_str = String::from_utf8(server_first).unwrap();
        assert!(server_first_str.starts_with("r=abc123"));

        // Pull combined_nonce out of the server-first.
        let combined = state.combined_nonce.clone().unwrap();
        assert!(combined.starts_with("abc123"));

        // Compute the client proof exactly like a real client would.
        let salted = pbkdf2_hash("sesame", &cred.salt, cred.iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_first_bare = "n=alice,r=abc123";
        let client_final_no_proof = format!("c=biws,r={combined}");
        let auth_message = format!(
            "{client_first_bare},{},{client_final_no_proof}",
            state.server_first.as_ref().unwrap()
        );
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof = xor(&client_key, &client_signature);
        let proof_b64 = B64.encode(proof);
        let client_final = format!("{client_final_no_proof},p={proof_b64}");

        let r2 = handle_round(&mut state, &users, client_final.as_bytes());
        let server_final = match r2 {
            ScramRoundResult::Success { server_message } => server_message,
            _ => panic!("expected Success"),
        };
        let server_final_str = String::from_utf8(server_final).unwrap();
        assert!(server_final_str.starts_with("v="));
    }

    fn pbkdf2_hash(password: &str, salt: &[u8], iterations: u32) -> [u8; SHA256_LEN] {
        let mut out = [0u8; SHA256_LEN];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut out);
        out
    }

    #[test]
    fn wrong_password_fails() {
        let mut users = HashMap::new();
        users.insert(
            "alice".to_string(),
            ScramCredential::from_password("sesame"),
        );
        let mut state = ScramState::default();

        let _ = handle_round(&mut state, &users, b"n,,n=alice,r=xyz789");
        let combined = state.combined_nonce.clone().unwrap();

        // Submit a totally wrong proof.
        let bogus_proof = B64.encode([0u8; SHA256_LEN]);
        let client_final = format!("c=biws,r={combined},p={bogus_proof}");
        let r2 = handle_round(&mut state, &users, client_final.as_bytes());
        assert!(matches!(r2, ScramRoundResult::Failure { .. }));
        assert_eq!(state.stage, ScramStage::Failed);
    }
}
