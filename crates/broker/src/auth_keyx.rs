//! Keyx-backed API-key validation for the qx broker.
//!
//! Ported from `lynxx/external/core/gateway.lynxx.vexarr.com/src/integrations/keyx.rs`
//! and trimmed to qx's needs:
//!
//!   * lynxx tracks license tier / quotas; qx just needs a yes/no
//!     verdict on whether a bearer is currently valid.
//!   * We keep the Vexarr-envelope shape, the 4xx/5xx error split,
//!     and the prod base-URL pattern.
//!   * On top of lynxx's design we add a small in-memory TTL cache
//!     so we don't call keyx on every single RPC the gRPC
//!     interceptor sees — the broker handles thousands of RPCs/sec
//!     and a network round-trip per request would dominate.
//!
//! Wire format (POST `{base}/v1/keys/verify`):
//! ```http
//! Authorization: Bearer {api_key}
//! Content-Length: 0
//! ```
//!
//! Response envelope:
//! ```json
//! {
//!   "status": "SUCCESS" | "FAILED",
//!   "responseMessage": "...",
//!   "data": { "valid": true, "customer_urn": "cus_abc", ... }
//! }
//! ```
//!
//! - `status=SUCCESS` + `data.valid=true`  → Ok(Verdict { customer_urn })
//! - `status=SUCCESS` + `data.valid=false` → Err(Rejected)
//! - HTTP 4xx                              → Err(Rejected) — authoritative
//! - HTTP 5xx                              → Err(Http)     — transient
//! - Network error                         → Err(Transport)
//! - Body parse failure                    → Err(Malformed)

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum KeyxError {
    #[error("keyx request failed: {0}")]
    Transport(String),
    #[error("keyx returned HTTP {status}: {body}")]
    Http { status: u16, body: String },
    #[error("keyx rejected the api key: {0}")]
    Rejected(String),
    #[error("keyx response malformed: {0}")]
    Malformed(String),
}

/// Successful validation payload — what the broker logs / surfaces.
#[derive(Debug, Clone)]
pub struct Verdict {
    /// Stable urn for the customer/principal the key belongs to;
    /// good enough for an audit-log line, not used for routing.
    pub customer_urn: String,
}

/// Per-key cache slot. `Hit`s last `KeyxValidator.cache_ttl`;
/// `Miss`es last `KeyxValidator.negative_cache_ttl` so we don't
/// hammer keyx with the same bad key from a misbehaving client.
#[derive(Clone)]
enum CacheSlot {
    Hit { verdict: Verdict, until: Instant },
    Miss { reason: String, until: Instant },
}

/// Cache stats for tests / observability.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub upstream_calls: u64,
}

/// HTTP client + cache for the keyx `verify` endpoint. Cheap to
/// clone (wraps an `Arc`-shaped `reqwest::Client` internally), but
/// the live broker keeps a single one behind `Arc<KeyxValidator>`.
#[derive(Clone)]
pub struct KeyxValidator {
    inner: reqwest::Client,
    base_url: String,
    cache_ttl: Duration,
    negative_cache_ttl: Duration,
    cache: std::sync::Arc<Mutex<HashMap<String, CacheSlot>>>,
    stats: std::sync::Arc<Mutex<CacheStats>>,
}

/// Default cache: 60s for hits, 5s for misses. The hit TTL is the
/// keyx revocation latency floor — operators wanting faster
/// revocation should tune `cache_ttl_secs` down.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);
const DEFAULT_NEG_CACHE_TTL: Duration = Duration::from_secs(5);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

impl KeyxValidator {
    /// Production constructor — talks to whatever `base_url` was
    /// passed and uses the default cache/timeout knobs.
    pub fn new(base_url: impl Into<String>) -> Result<Self, KeyxError> {
        Self::with_options(base_url, DEFAULT_CACHE_TTL, DEFAULT_NEG_CACHE_TTL)
    }

    /// Test-only / advanced: override the cache TTLs. The HTTP
    /// timeout stays at the 10s default — that's a connect-bound
    /// guard, not a tunable.
    pub fn with_options(
        base_url: impl Into<String>,
        cache_ttl: Duration,
        negative_cache_ttl: Duration,
    ) -> Result<Self, KeyxError> {
        let inner = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(|e| KeyxError::Transport(e.to_string()))?;
        Ok(Self {
            inner,
            base_url: base_url.into(),
            cache_ttl,
            negative_cache_ttl,
            cache: std::sync::Arc::new(Mutex::new(HashMap::new())),
            stats: std::sync::Arc::new(Mutex::new(CacheStats::default())),
        })
    }

    /// Validate `api_key` against keyx. Returns a [`Verdict`] on
    /// success; the caller maps every error variant to a gRPC
    /// `Unauthenticated` for the wire reply but logs the variant
    /// verbatim so operators can tell rejection from transient
    /// upstream failure.
    pub async fn validate(&self, api_key: &str) -> Result<Verdict, KeyxError> {
        // Cache hit / miss check before we touch the network.
        if let Some(slot) = self.lookup(api_key) {
            match slot {
                CacheSlot::Hit { verdict, .. } => {
                    self.bump_stats(|s| s.hits += 1);
                    return Ok(verdict);
                }
                CacheSlot::Miss { reason, .. } => {
                    self.bump_stats(|s| s.hits += 1);
                    return Err(KeyxError::Rejected(reason));
                }
            }
        }
        self.bump_stats(|s| {
            s.misses += 1;
            s.upstream_calls += 1;
        });

        let url = format!("{}/v1/keys/verify", self.base_url.trim_end_matches('/'));
        let resp = self
            .inner
            .post(&url)
            .bearer_auth(api_key)
            .header("content-length", "0")
            .send()
            .await
            .map_err(|e| KeyxError::Transport(e.to_string()))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| KeyxError::Transport(e.to_string()))?;

        // 4xx = authoritative rejection — pin the negative cache so
        // a misbehaving client retrying the same bad bearer doesn't
        // DOS keyx for us.
        if status.is_client_error() {
            let msg = parse_envelope_message(&text).unwrap_or_else(|| {
                if text.is_empty() {
                    format!("keyx returned {}", status.as_u16())
                } else {
                    text.clone()
                }
            });
            self.store_miss(api_key, msg.clone());
            return Err(KeyxError::Rejected(msg));
        }
        if !status.is_success() {
            // 5xx — don't cache; let the next caller retry.
            return Err(KeyxError::Http {
                status: status.as_u16(),
                body: text,
            });
        }

        let envelope: ApiEnvelope<VerifyData> =
            serde_json::from_str(&text).map_err(|e| KeyxError::Malformed(format!("{e}: {text}")))?;

        match envelope.status {
            EnvelopeStatus::Success => {
                if !envelope.data.valid {
                    let msg = if envelope.response_message.is_empty() {
                        "key is not active".to_string()
                    } else {
                        envelope.response_message
                    };
                    self.store_miss(api_key, msg.clone());
                    return Err(KeyxError::Rejected(msg));
                }
                let verdict = Verdict {
                    customer_urn: envelope.data.customer_urn,
                };
                self.store_hit(api_key, verdict.clone());
                Ok(verdict)
            }
            EnvelopeStatus::Failed => {
                let msg = envelope.response_message;
                self.store_miss(api_key, msg.clone());
                Err(KeyxError::Rejected(msg))
            }
        }
    }

    /// Drop every cached entry. Wired to qx's SIGHUP path so an
    /// operator who rotates the keyx URL or wants to force re-
    /// validation can do so without restarting the broker.
    pub fn flush_cache(&self) {
        self.cache.lock().unwrap().clear();
    }

    pub fn cache_stats(&self) -> CacheStats {
        *self.stats.lock().unwrap()
    }

    fn lookup(&self, key: &str) -> Option<CacheSlot> {
        let now = Instant::now();
        let mut cache = self.cache.lock().unwrap();
        match cache.get(key).cloned() {
            Some(slot) => {
                let still_valid = match &slot {
                    CacheSlot::Hit { until, .. } => *until > now,
                    CacheSlot::Miss { until, .. } => *until > now,
                };
                if still_valid {
                    Some(slot)
                } else {
                    cache.remove(key);
                    None
                }
            }
            None => None,
        }
    }

    fn store_hit(&self, key: &str, verdict: Verdict) {
        let until = Instant::now() + self.cache_ttl;
        self.cache
            .lock()
            .unwrap()
            .insert(key.to_string(), CacheSlot::Hit { verdict, until });
    }

    fn store_miss(&self, key: &str, reason: String) {
        let until = Instant::now() + self.negative_cache_ttl;
        self.cache
            .lock()
            .unwrap()
            .insert(key.to_string(), CacheSlot::Miss { reason, until });
    }

    fn bump_stats(&self, f: impl FnOnce(&mut CacheStats)) {
        f(&mut self.stats.lock().unwrap())
    }
}

fn parse_envelope_message(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    v.get("responseMessage")
        .and_then(|m| m.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum EnvelopeStatus {
    Success,
    Failed,
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope<T> {
    status: EnvelopeStatus,
    #[serde(rename = "responseMessage", default)]
    response_message: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct VerifyData {
    #[serde(default)]
    valid: bool,
    #[serde(default, alias = "customer_id")]
    customer_urn: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn envelope_ok() -> serde_json::Value {
        serde_json::json!({
            "transactionUrn": "urn:keyx:txn:abc",
            "status": "SUCCESS",
            "responseMessage": "OK",
            "responseKey": "SUCCESS",
            "errors": [],
            "data": { "valid": true, "customer_urn": "cus_abc" },
        })
    }

    fn envelope_revoked() -> serde_json::Value {
        serde_json::json!({
            "status": "SUCCESS",
            "responseMessage": "key revoked",
            "data": { "valid": false },
        })
    }

    #[tokio::test]
    async fn ok_caches_subsequent_calls() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/keys/verify"))
            .and(header("authorization", "Bearer qx_live_xxx"))
            .respond_with(ResponseTemplate::new(200).set_body_json(envelope_ok()))
            .expect(1) // pinned: second call must NOT hit keyx
            .mount(&server)
            .await;

        let v = KeyxValidator::new(server.uri()).unwrap();
        let first = v.validate("qx_live_xxx").await.unwrap();
        assert_eq!(first.customer_urn, "cus_abc");
        let second = v.validate("qx_live_xxx").await.unwrap();
        assert_eq!(second.customer_urn, "cus_abc");

        let stats = v.cache_stats();
        assert_eq!(stats.upstream_calls, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn rejected_4xx_pins_negative_cache() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "status": "FAILED",
                "responseMessage": "invalid service api key",
                "data": null,
            })))
            .expect(1) // second call uses the negative cache
            .mount(&server)
            .await;

        let v = KeyxValidator::new(server.uri()).unwrap();
        let err1 = v.validate("bad").await.unwrap_err();
        let err2 = v.validate("bad").await.unwrap_err();
        for err in [err1, err2] {
            match err {
                KeyxError::Rejected(m) => assert_eq!(m, "invalid service api key"),
                other => panic!("expected Rejected, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn revoked_success_envelope_is_rejected_and_cached() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(envelope_revoked()))
            .expect(1)
            .mount(&server)
            .await;

        let v = KeyxValidator::new(server.uri()).unwrap();
        let err = v.validate("revoked").await.unwrap_err();
        assert!(matches!(err, KeyxError::Rejected(ref m) if m == "key revoked"));
        // negative-cache hit
        let err2 = v.validate("revoked").await.unwrap_err();
        assert!(matches!(err2, KeyxError::Rejected(_)));
    }

    #[tokio::test]
    async fn http_5xx_does_not_cache() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("down"))
            .expect(2) // both calls must hit keyx — no negative cache on 5xx
            .mount(&server)
            .await;

        let v = KeyxValidator::new(server.uri()).unwrap();
        let err = v.validate("any").await.unwrap_err();
        let err2 = v.validate("any").await.unwrap_err();
        for err in [err, err2] {
            match err {
                KeyxError::Http { status, .. } => assert_eq!(status, 503),
                other => panic!("expected Http, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn malformed_body_surfaces_distinctly() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let v = KeyxValidator::new(server.uri()).unwrap();
        let err = v.validate("x").await.unwrap_err();
        assert!(matches!(err, KeyxError::Malformed(_)));
    }

    #[tokio::test]
    async fn flush_cache_forces_re_validation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(envelope_ok()))
            .expect(2) // pre + post flush
            .mount(&server)
            .await;

        let v = KeyxValidator::new(server.uri()).unwrap();
        v.validate("qx_live_xxx").await.unwrap();
        v.flush_cache();
        v.validate("qx_live_xxx").await.unwrap();
        assert_eq!(v.cache_stats().upstream_calls, 2);
    }

    #[tokio::test]
    async fn cache_ttl_expires() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(envelope_ok()))
            .expect(2)
            .mount(&server)
            .await;

        // 50ms hit TTL — second call after the sleep must re-fetch.
        let v = KeyxValidator::with_options(
            server.uri(),
            Duration::from_millis(50),
            Duration::from_millis(50),
        )
        .unwrap();
        v.validate("qx_live_xxx").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        v.validate("qx_live_xxx").await.unwrap();
        assert_eq!(v.cache_stats().upstream_calls, 2);
    }
}
