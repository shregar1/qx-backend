//! qx — QX broker daemon.
//!
//! Loads a TOML config (default `qx.toml`), starts the gRPC broker server
//! and (optionally) the Prometheus metrics HTTP endpoint, and logs what it's
//! serving. Shuts down cleanly on SIGINT.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use qx_broker::{
    Broker, BrokerConfig, ClusterConfig, KafkaConfig, KeyxValidator, TlsConfig, bind_with_retry,
};

/// Diff key extracted from a parsed `[tls]` block. We compare these
/// in qx's SIGHUP loop instead of the raw `toml::Value` so a
/// reformat of the file (whitespace, key-order) doesn't trigger a
/// pointless cert-rotation.
type TlsFingerprint = Option<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)>;
use serde::Deserialize;

#[derive(Parser)]
#[command(name = "qx", about = "QX — broker, dashboard, admin API")]
struct Args {
    /// Path to TOML config.
    #[arg(short, long, default_value = "qx.toml")]
    config: PathBuf,
}

#[derive(Deserialize)]
struct Config {
    server: ServerCfg,
    metrics: Option<MetricsCfg>,
    auth: Option<AuthCfg>,
    keyx: Option<KeyxCfg>,
    tls: Option<TlsCfg>,
    kafka: Option<KafkaCfg>,
    cluster: Option<ClusterCfg>,
    workflow: Option<WorkflowCfg>,
}

#[derive(Deserialize, Clone, PartialEq, Eq)]
struct KeyxCfg {
    /// Base URL of the keyx instance, e.g. `https://auth.vexarr.com`
    /// for the GCP-hosted production. Required when the `[keyx]`
    /// block is present.
    base_url: String,
    /// How long a successful validation is cached, in seconds.
    /// Default 60s — this is also the keyx revocation latency
    /// floor; turn it down if you want faster revocation at the
    /// cost of more upstream calls.
    cache_ttl_secs: Option<u64>,
    /// How long a 4xx rejection is negatively cached, in seconds.
    /// Default 5s — short, just enough to absorb a misbehaving
    /// client retrying the same bad key in a tight loop.
    negative_cache_ttl_secs: Option<u64>,
}

#[derive(Deserialize)]
struct WorkflowCfg {
    /// Initial capacity for the engine's encode scratch (bytes).
    /// Sized to cover header + payload + result for the typical
    /// task; default 1 KiB. Setting this to your p99 task size
    /// eliminates the first-call growth cost on a fresh engine.
    encode_buf_capacity: Option<usize>,
    /// Refuse `cancel(id)` when the descendant set grows past this
    /// many tasks. `0` (or omitted) disables the guard.
    cancel_max_descendants: Option<usize>,
    /// redb page cache size for every workflow Engine, in bytes.
    /// Omitted = redb default.
    redb_cache_bytes: Option<usize>,
    /// Per-workflow overrides — `[[workflow.override]]` arrays in
    /// TOML. Each entry's `name` field picks the workflow to apply
    /// the inner values to; everything else mirrors the top-level
    /// fields above. Lets one broker host both a tiny-task and a
    /// big-blob workflow without sizing the engine for the larger.
    #[serde(rename = "override", default)]
    overrides: Vec<WorkflowOverrideCfg>,
}

#[derive(Deserialize)]
struct WorkflowOverrideCfg {
    name: String,
    encode_buf_capacity: Option<usize>,
    cancel_max_descendants: Option<usize>,
    redb_cache_bytes: Option<usize>,
}

#[derive(Deserialize)]
struct ClusterCfg {
    /// Stable id for this broker in the cluster (0..N-1).
    node_id: u64,
    /// `[[cluster.voters]]` entries — one per voting broker.
    voters: Vec<VoterCfg>,
    /// Optional TLS material for the inter-broker Raft transport.
    /// `cert_file` + `key_file` follow the same shape as the
    /// top-level `[tls]` block. `client_ca_file` is the bundle the
    /// outgoing Raft connections trust; defaults to `cert_file`
    /// (self-signed mesh).
    raft_tls: Option<RaftTlsCfg>,
}

#[derive(Deserialize)]
struct VoterCfg {
    node_id: u64,
    /// `host:port` the peer broker listens on for inter-broker
    /// Raft RPCs (this is independent of the client-facing
    /// `server.addr` / `kafka.addr` listeners).
    addr: String,
    /// `host:port` the peer broker listens on for client gRPC. When
    /// set, this broker will transparently forward a Produce to that
    /// peer when openraft reports it as the current leader.
    client_addr: Option<String>,
}

#[derive(Deserialize)]
struct RaftTlsCfg {
    cert_file: PathBuf,
    key_file: PathBuf,
    client_ca_file: Option<PathBuf>,
}

#[derive(Deserialize)]
struct ServerCfg {
    /// e.g. "0.0.0.0:50051"
    addr: String,
    /// Where QX persists topics, workflows, etc.
    data_dir: PathBuf,
}

#[derive(Deserialize)]
struct MetricsCfg {
    /// e.g. "0.0.0.0:9090"
    addr: String,
}

#[derive(Deserialize)]
struct AuthCfg {
    /// Bearer token required on every RPC.
    token: String,
}

#[derive(Deserialize)]
struct TlsCfg {
    cert_file: PathBuf,
    key_file: PathBuf,
}

#[derive(Deserialize)]
struct KafkaCfg {
    /// e.g. "0.0.0.0:9092"
    addr: String,
    /// Host advertised to clients in Metadata responses.
    advertised_host: Option<String>,
    /// Port advertised to clients. Defaults to the listen port.
    advertised_port: Option<i32>,
    cluster_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let toml_str = std::fs::read_to_string(&args.config).map_err(|e| {
        format!("reading {}: {e}", args.config.display())
    })?;
    let config: Config = toml::from_str(&toml_str)?;

    let mut broker_config = BrokerConfig::default();
    if let Some(auth) = config.auth.as_ref() {
        broker_config.auth_token = Some(auth.token.clone());
    }
    if let Some(keyx) = config.keyx.as_ref() {
        let v = build_keyx_validator(keyx).map_err(|e| {
            format!("invalid [keyx] config: {e}")
        })?;
        broker_config.keyx_validator = Some(std::sync::Arc::new(v));
    }
    if let Some(tls) = config.tls.as_ref() {
        let cert_pem = std::fs::read(&tls.cert_file)?;
        let key_pem = std::fs::read(&tls.key_file)?;
        broker_config.tls = Some(TlsConfig {
            cert_pem,
            key_pem,
            client_ca_pem: None,
        });
    }
    if let Some(cluster) = config.cluster.as_ref() {
        let raft_tls = if let Some(rt) = &cluster.raft_tls {
            let cert_pem = std::fs::read(&rt.cert_file)?;
            let key_pem = std::fs::read(&rt.key_file)?;
            let client_ca_pem = if let Some(p) = &rt.client_ca_file {
                Some(std::fs::read(p)?)
            } else {
                Some(cert_pem.clone())
            };
            Some(TlsConfig {
                cert_pem,
                key_pem,
                client_ca_pem,
            })
        } else {
            None
        };
        broker_config.cluster = Some(ClusterConfig {
            node_id: cluster.node_id,
            voters: cluster
                .voters
                .iter()
                .map(|v| (v.node_id, v.addr.clone()))
                .collect(),
            raft_tls,
            client_addrs: cluster
                .voters
                .iter()
                .filter_map(|v| {
                    v.client_addr
                        .as_ref()
                        .map(|a| (v.node_id, a.clone()))
                })
                .collect(),
        });
    }

    let (top_defaults, parsed_overrides) =
        compute_workflow_knobs(config.workflow.as_ref());
    broker_config.workflow_options = top_defaults;
    broker_config.workflow_overrides = parsed_overrides;
    if let Some(wf) = config.workflow.as_ref() {
        // Sanity-check: warn for overrides that don't match any
        // workflow already on disk. The override will still apply if
        // a workflow with that name is created later, but at startup
        // a typo is almost certainly an operator mistake — log it
        // loudly instead of silently going inert.
        let known = list_known_workflows(&config.server.data_dir);
        for ov in &wf.overrides {
            if !known.contains(&ov.name) {
                eprintln!(
                    "qx: WARNING workflow override '{}' does not match any existing workflow under {}/workflows — override will apply if/when that workflow is created. Known workflows: [{}]",
                    ov.name,
                    config.server.data_dir.display(),
                    known.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
        }
    }

    eprintln!("qx: data_dir = {}", config.server.data_dir.display());
    if broker_config.keyx_validator.is_some() {
        let url = config.keyx.as_ref().map(|k| k.base_url.as_str()).unwrap_or("?");
        eprintln!("qx: keyx validation enabled → {url}");
    }
    if broker_config.auth_token.is_some() {
        eprintln!("qx: bearer-token auth enabled");
    }
    if broker_config.tls.is_some() {
        eprintln!("qx: TLS enabled");
    }
    if let Some(opts) = broker_config.workflow_options.as_ref() {
        eprintln!(
            "qx: workflow engine — encode_buf={} cancel_cap={} redb_cache={}",
            opts.encode_buf_capacity,
            opts.cancel_max_descendants,
            opts.redb_cache_bytes
                .map(|n| n.to_string())
                .unwrap_or_else(|| "default".to_string()),
        );
    }

    let broker = Broker::open_with_config(&config.server.data_dir, broker_config);

    // If cluster mode is configured, bootstrap this broker's Raft
    // node, install the matching `RaftReplicator`, and (when this is
    // node 0) initialise the cluster membership. Failing here is a
    // fatal startup error.
    if let Err(e) = broker.start_cluster().await {
        return Err(format!("starting Raft cluster: {e}").into());
    }
    if broker.cluster_node_id().is_some() {
        eprintln!(
            "qx: cluster mode enabled — node_id = {}",
            broker.cluster_node_id().unwrap()
        );
    }

    let grpc_addr: SocketAddr = config.server.addr.parse()?;
    let grpc_listener = bind_with_retry(&grpc_addr.to_string()).await?;
    eprintln!("qx: gRPC listening on {}", grpc_listener.local_addr()?);
    let grpc_task = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.serve_with_listener(grpc_listener).await })
    };

    // The dashboard + Prometheus metrics + admin API all share one
    // HTTP listener. We start it unconditionally now — the broker
    // ships an embedded node dashboard (`include_dir!` baked into
    // the binary), so "qx is running" implies "the dashboard is
    // reachable". Operators who want a different bind address
    // override it under `[metrics]`; operators who genuinely don't
    // want any HTTP surface can set `[metrics].addr = "off"` to
    // skip the listener entirely.
    const DEFAULT_DASHBOARD_ADDR: &str = "127.0.0.1:9090";
    let dashboard_addr: Option<String> = match config.metrics.as_ref() {
        Some(m) if m.addr.eq_ignore_ascii_case("off") => None,
        Some(m) => Some(m.addr.clone()),
        None => Some(DEFAULT_DASHBOARD_ADDR.to_string()),
    };
    let metrics_task = if let Some(addr_str) = dashboard_addr {
        let metrics_addr: SocketAddr = addr_str.parse()?;
        let metrics_listener = bind_with_retry(&metrics_addr.to_string()).await?;
        let bound = metrics_listener.local_addr()?;
        eprintln!("qx: dashboard at http://{bound}/");
        eprintln!("qx:  ├─ Prometheus metrics: http://{bound}/metrics");
        eprintln!("qx:  └─ admin API:          http://{bound}/api/*");
        if config.metrics.is_none() {
            eprintln!(
                "qx: (using default dashboard bind — set [metrics].addr in {} to override, or [metrics].addr=\"off\" to disable)",
                args.config.display()
            );
        }
        let broker = broker.clone();
        Some(tokio::spawn(async move {
            broker.serve_metrics_with_listener(metrics_listener).await
        }))
    } else {
        eprintln!("qx: dashboard disabled by [metrics].addr=\"off\"");
        None
    };

    let kafka_task = if let Some(kafka) = config.kafka {
        let kafka_addr: SocketAddr = kafka.addr.parse()?;
        let kafka_listener = bind_with_retry(&kafka_addr.to_string()).await?;
        let bound = kafka_listener.local_addr()?;
        eprintln!("qx: Kafka wire-protocol listening on {bound}");
        let kafka_config = KafkaConfig {
            advertised_host: kafka
                .advertised_host
                .unwrap_or_else(|| bound.ip().to_string()),
            advertised_port: kafka.advertised_port.unwrap_or(bound.port() as i32),
            cluster_id: kafka.cluster_id.unwrap_or_else(|| "qx-cluster".to_string()),
            node_id: 0,
            ..Default::default()
        };
        let broker = broker.clone();
        Some(tokio::spawn(async move {
            broker
                .serve_kafka_with_listener(kafka_listener, kafka_config)
                .await
        }))
    } else {
        None
    };

    // SIGHUP reloads the TOML config and pushes changed
    // `[workflow]` and `[auth]` knobs into the running broker:
    //
    //   * `[auth].token`        → `Broker::update_auth_token`
    //   * `[workflow]` defaults → `Broker::update_workflow_defaults`
    //   * `[[workflow.override]]` → `Broker::update_workflow_overrides`
    //     followed by an eager `warm_workflow` per evicted engine.
    //
    // We diff each block against the previously-applied TOML value
    // before touching the broker: a SIGHUP sent after editing an
    // unrelated section (server.addr, kafka, …) becomes a no-op
    // rather than churning the engine cache. Listener addresses,
    // TLS material, and cluster membership are *not* reloaded here
    // because changing them mid-flight would require rebinding
    // sockets or re-bootstrapping the Raft node — those still
    // require a restart.
    let sighup_task = {
        let broker = broker.clone();
        let config_path = args.config.clone();
        let data_dir = config.server.data_dir.clone();
        // Seed prev-values from the toml we just parsed at startup
        // so the very first SIGHUP after a no-op edit short-circuits.
        let initial_raw: toml::Value =
            toml::from_str(&toml_str).unwrap_or(toml::Value::Table(Default::default()));
        let prev_workflow_value: Option<toml::Value> = initial_raw.get("workflow").cloned();
        let prev_auth_value: Option<toml::Value> = initial_raw.get("auth").cloned();
        let prev_keyx_value: Option<toml::Value> = initial_raw.get("keyx").cloned();
        // Snapshot the cert/key bytes the broker already loaded
        // so the *first* SIGHUP after a no-op edit short-circuits
        // without re-reading from disk. `broker_config` has been
        // moved into the broker by this point, so we recompute
        // the fingerprint by re-reading the same files.
        let prev_tls_fingerprint: TlsFingerprint =
            read_tls_fingerprint(&initial_raw).unwrap_or(None);
        let prev_voter_set: Option<std::collections::BTreeSet<u64>> =
            extract_voter_set(&initial_raw);
        #[cfg(unix)]
        let task = tokio::spawn(sighup_loop(
            broker,
            config_path,
            data_dir,
            prev_workflow_value,
            prev_auth_value,
            prev_tls_fingerprint,
            prev_voter_set,
            prev_keyx_value,
        ));
        #[cfg(not(unix))]
        let task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            // Non-unix targets don't get SIGHUP; the spawn is a no-op.
            let _ = (
                broker,
                config_path,
                data_dir,
                prev_workflow_value,
                prev_auth_value,
                prev_tls_fingerprint,
                prev_voter_set,
                prev_keyx_value,
            );
            std::future::pending::<()>().await;
        });
        task
    };

    tokio::select! {
        result = grpc_task => {
            eprintln!("qx: gRPC server exited: {result:?}");
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("qx: shutdown signal received");
        }
    }
    sighup_task.abort();
    if let Some(t) = metrics_task {
        t.abort();
    }
    if let Some(t) = kafka_task {
        t.abort();
    }
    Ok(())
}

/// Pull `[[cluster.voters]]` out of the raw `toml::Value` as the
/// ordered set of node ids. We compare this set against the
/// previously-applied one on SIGHUP and submit the diff to
/// openraft. Returns `None` when the `[cluster]` block is absent
/// entirely (single-node mode).
fn extract_voter_set(raw: &toml::Value) -> Option<std::collections::BTreeSet<u64>> {
    let cluster = raw.get("cluster").and_then(|v| v.as_table())?;
    let voters = cluster.get("voters").and_then(|v| v.as_array())?;
    let mut out = std::collections::BTreeSet::new();
    for v in voters {
        if let Some(id) = v.as_table().and_then(|t| t.get("node_id")).and_then(|v| v.as_integer()) {
            if id >= 0 {
                out.insert(id as u64);
            }
        }
    }
    Some(out)
}

/// Read the cert/key (and optional client-CA) bytes referenced by
/// the `[tls]` block in a raw `toml::Value`. Returns `None` when
/// the block is absent, and bubbles file-read errors so the SIGHUP
/// loop can log a parse rejection without applying anything.
fn read_tls_fingerprint(raw: &toml::Value) -> Result<TlsFingerprint, std::io::Error> {
    let Some(tls) = raw.get("tls").and_then(|v| v.as_table()) else {
        return Ok(None);
    };
    let cert_path = tls
        .get("cert_file")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "[tls].cert_file missing or not a string",
            )
        })?;
    let key_path = tls
        .get("key_file")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "[tls].key_file missing or not a string",
            )
        })?;
    let cert = std::fs::read(cert_path)?;
    let key = std::fs::read(key_path)?;
    let ca = match tls.get("client_ca_file").and_then(|v| v.as_str()) {
        Some(p) => Some(std::fs::read(p)?),
        None => None,
    };
    Ok(Some((cert, key, ca)))
}

/// Build a `KeyxValidator` from a parsed `[keyx]` block. Returns
/// a string error so qx's SIGHUP handler can log the rejection
/// and keep the previous validator (if any) in place.
fn build_keyx_validator(cfg: &KeyxCfg) -> std::result::Result<KeyxValidator, String> {
    let cache_ttl = std::time::Duration::from_secs(cfg.cache_ttl_secs.unwrap_or(60));
    let neg_ttl = std::time::Duration::from_secs(cfg.negative_cache_ttl_secs.unwrap_or(5));
    KeyxValidator::with_options(&cfg.base_url, cache_ttl, neg_ttl)
        .map_err(|e| e.to_string())
}

/// Translate the deserialised `[workflow]` block into the two
/// runtime shapes the broker consumes: a broker-wide default
/// `EngineOptions` (or `None` when the block is omitted entirely)
/// and a per-name `HashMap` of overrides. Overrides inherit any
/// field they don't set from the top-level `[workflow]` block, so
/// operators typically tune one knob per workflow without
/// re-declaring the rest. Used at startup and from the SIGHUP loop.
fn compute_workflow_knobs(
    wf: Option<&WorkflowCfg>,
) -> (
    Option<qx_workflow::EngineOptions>,
    std::collections::HashMap<String, qx_workflow::EngineOptions>,
) {
    let mut overrides = std::collections::HashMap::new();
    let Some(wf) = wf else {
        return (None, overrides);
    };
    let defaults = qx_workflow::EngineOptions::default();
    let top = qx_workflow::EngineOptions {
        encode_buf_capacity: wf
            .encode_buf_capacity
            .unwrap_or(defaults.encode_buf_capacity),
        cancel_max_descendants: wf
            .cancel_max_descendants
            .unwrap_or(defaults.cancel_max_descendants),
        redb_cache_bytes: wf.redb_cache_bytes.or(defaults.redb_cache_bytes),
    };
    for ov in &wf.overrides {
        overrides.insert(
            ov.name.clone(),
            qx_workflow::EngineOptions {
                encode_buf_capacity: ov
                    .encode_buf_capacity
                    .unwrap_or(top.encode_buf_capacity),
                cancel_max_descendants: ov
                    .cancel_max_descendants
                    .unwrap_or(top.cancel_max_descendants),
                redb_cache_bytes: ov.redb_cache_bytes.or(top.redb_cache_bytes),
            },
        );
    }
    (Some(top), overrides)
}

/// Long-running SIGHUP reload loop. Diff-cached: the previously-
/// applied `[workflow]` and `[auth]` block values are compared
/// against each tick's freshly-parsed TOML, and the broker is only
/// touched when one of them actually changed. Stays in scope for
/// the entire qx lifetime; aborted on shutdown.
#[cfg(unix)]
async fn sighup_loop(
    broker: std::sync::Arc<Broker>,
    config_path: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    mut prev_workflow_value: Option<toml::Value>,
    mut prev_auth_value: Option<toml::Value>,
    mut prev_tls_fingerprint: TlsFingerprint,
    mut prev_voter_set: Option<std::collections::BTreeSet<u64>>,
    mut prev_keyx_value: Option<toml::Value>,
) {
    use tokio::signal::unix::{SignalKind, signal};
    // Dedup set of workflow names currently being warm-retried in a
    // background task. Lets two HUPs in quick succession queue the
    // same name only once, and lets a retry observe that a later
    // HUP changed plans.
    let retry_inflight: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let mut hup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("qx: SIGHUP handler install failed: {e}");
            return;
        }
    };
    while hup.recv().await.is_some() {
        eprintln!("qx: SIGHUP — reloading {}", config_path.display());
        let toml_str = match std::fs::read_to_string(&config_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "qx: SIGHUP reload failed reading {}: {e} — keeping previous config",
                    config_path.display()
                );
                continue;
            }
        };
        let raw: toml::Value = match toml::from_str(&toml_str) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "qx: SIGHUP reload failed parsing TOML: {e} — keeping previous config"
                );
                continue;
            }
        };
        let new_workflow_value = raw.get("workflow").cloned();
        let new_auth_value = raw.get("auth").cloned();
        let workflow_changed = new_workflow_value != prev_workflow_value;
        let auth_changed = new_auth_value != prev_auth_value;
        let new_keyx_value = raw.get("keyx").cloned();
        let keyx_changed = new_keyx_value != prev_keyx_value;
        // TLS diff happens on file *contents*, not on the toml
        // value of the `[tls]` block — the operator typically
        // edits the cert/key files referenced by the TOML rather
        // than the TOML itself. So we read the referenced files
        // up-front; mismatched paths or unreadable files mean the
        // rotation is rejected and the previous cert stays live.
        let new_tls_fingerprint = match read_tls_fingerprint(&raw) {
            Ok(fp) => fp,
            Err(e) => {
                eprintln!(
                    "qx: SIGHUP reload failed reading TLS material: {e} — keeping previous TLS"
                );
                None
            }
        };
        let tls_changed = new_tls_fingerprint != prev_tls_fingerprint;
        let new_voter_set = extract_voter_set(&raw);
        let voters_changed = new_voter_set != prev_voter_set;
        if !workflow_changed
            && !auth_changed
            && !tls_changed
            && !voters_changed
            && !keyx_changed
        {
            eprintln!(
                "qx: SIGHUP reload — no [workflow]/[auth]/[tls]/[cluster.voters]/[keyx] changes; skipping recompute"
            );
            continue;
        }
        // At least one block changed; pay the typed-deserialise cost
        // once and apply each changed knob.
        let parsed: Config = match toml::from_str(&toml_str) {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "qx: SIGHUP reload failed re-deserialising Config: {e} — keeping previous config"
                );
                continue;
            }
        };
        if auth_changed {
            let token = parsed.auth.as_ref().map(|a| a.token.clone());
            let action = if token.is_some() { "rotated" } else { "disabled" };
            broker.update_auth_token(token);
            eprintln!("qx: SIGHUP reload — auth token {action}");
            prev_auth_value = new_auth_value;
        }
        if keyx_changed {
            match parsed.keyx.as_ref() {
                Some(cfg) => match build_keyx_validator(cfg) {
                    Ok(v) => {
                        broker.update_keyx_validator(Some(std::sync::Arc::new(v)));
                        eprintln!(
                            "qx: SIGHUP reload — keyx validator rebuilt against {}",
                            cfg.base_url
                        );
                        prev_keyx_value = new_keyx_value;
                    }
                    Err(e) => {
                        eprintln!(
                            "qx: SIGHUP reload — keyx rebuild rejected: {e} (keeping previous validator)"
                        );
                    }
                },
                None => {
                    broker.update_keyx_validator(None);
                    eprintln!("qx: SIGHUP reload — keyx disabled");
                    prev_keyx_value = new_keyx_value;
                }
            }
        }
        if voters_changed {
            match new_voter_set.clone() {
                Some(set) if !set.is_empty() => {
                    eprintln!(
                        "qx: SIGHUP reload — submitting voter change to [{}]",
                        set.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ")
                    );
                    match broker.change_cluster_membership(set).await {
                        Ok(()) => {
                            eprintln!("qx: SIGHUP reload — voter set updated");
                            prev_voter_set = new_voter_set.clone();
                        }
                        Err(e) => {
                            eprintln!(
                                "qx: SIGHUP reload — voter change rejected: {e} (keeping previous membership; the change must be issued against the current Raft leader, and every new voter must be reachable)"
                            );
                        }
                    }
                }
                Some(_) => {
                    eprintln!(
                        "qx: SIGHUP reload — empty voter set ignored (would collapse the cluster)"
                    );
                }
                None => {
                    eprintln!(
                        "qx: SIGHUP reload — [cluster] removed at runtime; cannot tear down a live Raft node, ignoring"
                    );
                }
            }
        }
        if tls_changed {
            // Reuse the fingerprint we read above (it contains the
            // raw PEM bytes so we don't re-read the file). A `None`
            // means TLS is being disabled — but we can only disable
            // on a broker that started with TLS, so the broker has
            // to deal with a `None` post-rotation by stopping new
            // handshakes (existing sessions unaffected).
            let next_tls: Option<TlsConfig> = new_tls_fingerprint
                .as_ref()
                .map(|(cert, key, ca)| TlsConfig {
                    cert_pem: cert.clone(),
                    key_pem: key.clone(),
                    client_ca_pem: ca.clone(),
                });
            match broker.update_tls(next_tls.as_ref()) {
                Ok(()) => {
                    let action = if next_tls.is_some() {
                        "rotated"
                    } else {
                        "disabled (new handshakes will be dropped)"
                    };
                    eprintln!("qx: SIGHUP reload — TLS {action}");
                    prev_tls_fingerprint = new_tls_fingerprint;
                }
                Err(e) => {
                    eprintln!(
                        "qx: SIGHUP reload — TLS rotation rejected: {e:?} — keeping previous cert"
                    );
                }
            }
        }
        if workflow_changed {
            let (defaults, overrides) = compute_workflow_knobs(parsed.workflow.as_ref());
            if let Some(wf) = parsed.workflow.as_ref() {
                // Same misspelling warning as at startup — repeats
                // on every reload because a SIGHUP is exactly when
                // operators add/edit these entries.
                let known = list_known_workflows(&data_dir);
                for ov in &wf.overrides {
                    if !known.contains(&ov.name) {
                        eprintln!(
                            "qx: SIGHUP reload — override '{}' has no matching workflow on disk yet",
                            ov.name
                        );
                    }
                }
            }
            // Defaults-eviction comes first; we union it with the
            // override-eviction set so the warm loop sees every
            // evicted engine. A workflow that was using the broker
            // default AND just got a fresh per-workflow override
            // can be evicted by either path — deduping via a set.
            let evicted_by_defaults = broker.update_workflow_defaults(defaults);
            let changed_overrides = broker.update_workflow_overrides(overrides);
            let mut to_warm: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            to_warm.extend(evicted_by_defaults);
            to_warm.extend(changed_overrides);
            if to_warm.is_empty() {
                eprintln!(
                    "qx: SIGHUP reload — no workflow engines changed"
                );
            } else {
                eprintln!(
                    "qx: SIGHUP reload — {} workflow engine(s) evicted: [{}]",
                    to_warm.len(),
                    to_warm.iter().cloned().collect::<Vec<_>>().join(", ")
                );
                // Eagerly re-open each evicted engine so the next
                // caller doesn't pay the open cost on the hot path.
                // `warm_workflow` returns `false` when an in-flight
                // caller still holds the prior `Arc` (redb's per-
                // file lock then refuses a second open). Pending
                // names are handed to a background retry task with
                // exponential backoff and a deadline — fire-and-
                // forget is gone.
                let mut warmed = Vec::new();
                let mut pending = Vec::new();
                for name in &to_warm {
                    if broker.warm_workflow(name) {
                        warmed.push(name.clone());
                    } else {
                        pending.push(name.clone());
                    }
                }
                if !warmed.is_empty() {
                    eprintln!("qx: SIGHUP reload — warmed [{}]", warmed.join(", "));
                }
                if !pending.is_empty() {
                    let mut to_spawn = Vec::new();
                    {
                        let mut inflight = retry_inflight.lock().unwrap();
                        for name in pending {
                            if inflight.insert(name.clone()) {
                                to_spawn.push(name);
                            }
                        }
                    }
                    if !to_spawn.is_empty() {
                        eprintln!(
                            "qx: SIGHUP reload — queued warm retry for [{}] (still in use)",
                            to_spawn.join(", ")
                        );
                        for name in to_spawn {
                            let broker_for_retry = broker.clone();
                            let inflight_for_retry = retry_inflight.clone();
                            tokio::spawn(warm_with_retry(
                                broker_for_retry,
                                inflight_for_retry,
                                name,
                            ));
                        }
                    }
                }
            }
            prev_workflow_value = new_workflow_value;
        }
    }
}

/// Background warm-retry task. Polls `warm_workflow(name)` with
/// 1s → 2s → 4s → … capped at 5s, giving up after roughly two
/// minutes. The `inflight` set lets later HUPs avoid double-queueing
/// the same name while a retry is already in progress; we always
/// clear the slot when the task exits regardless of outcome.
#[cfg(unix)]
async fn warm_with_retry(
    broker: std::sync::Arc<Broker>,
    inflight: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    name: String,
) {
    use std::time::Duration;
    let _ = warm_with_retry_with(
        broker,
        inflight,
        name,
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::from_secs(120),
    )
    .await;
}

/// Parameterised inner loop for `warm_with_retry`. Lets unit
/// tests inject sub-second timings instead of waiting two minutes
/// for the production timeout to elapse. Returns `true` on
/// success, `false` if the deadline expires first — exposed for
/// tests; the production wrapper drops the bool and logs instead.
async fn warm_with_retry_with(
    broker: std::sync::Arc<Broker>,
    inflight: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    name: String,
    initial_backoff: std::time::Duration,
    max_backoff: std::time::Duration,
    total_deadline: std::time::Duration,
) -> bool {
    let deadline = std::time::Instant::now() + total_deadline;
    let mut backoff = initial_backoff;
    let outcome = loop {
        tokio::time::sleep(backoff).await;
        if broker.warm_workflow(&name) {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        backoff = std::cmp::min(backoff * 2, max_backoff);
    };
    inflight.lock().unwrap().remove(&name);
    if outcome {
        eprintln!("qx: SIGHUP warm-retry — engine '{name}' reopened");
    } else {
        eprintln!(
            "qx: SIGHUP warm-retry — engine '{name}' still locked after deadline; giving up (will reopen lazily on next access)"
        );
    }
    outcome
}

/// Walk `<data_dir>/workflows/*.redb` and return the workflow names
/// already on disk. Used to validate that `[[workflow.override]]`
/// entries reference workflows the operator actually has — a misspelt
/// name would otherwise sit inert until that workflow is created.
fn list_known_workflows(data_dir: &std::path::Path) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    let workflows_dir = data_dir.join("workflows");
    let Ok(entries) = std::fs::read_dir(&workflows_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("redb") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            out.insert(stem.to_string());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voter_set_extracts_sorted_unique_ids() {
        let toml_text = r#"
[server]
addr = "127.0.0.1:0"
data_dir = "/tmp/x"

[cluster]
node_id = 0

[[cluster.voters]]
node_id = 2
addr = "h:1"

[[cluster.voters]]
node_id = 0
addr = "h:0"

[[cluster.voters]]
node_id = 1
addr = "h:1"
"#;
        let raw: toml::Value = toml::from_str(toml_text).unwrap();
        let set = extract_voter_set(&raw).unwrap();
        let v: Vec<u64> = set.into_iter().collect();
        assert_eq!(v, vec![0, 1, 2]);
    }

    #[test]
    fn voter_set_is_none_when_no_cluster_block() {
        let toml_text = r#"
[server]
addr = "127.0.0.1:0"
data_dir = "/tmp/x"
"#;
        let raw: toml::Value = toml::from_str(toml_text).unwrap();
        assert_eq!(extract_voter_set(&raw), None);
    }

    #[test]
    fn voter_set_diff_detects_add_and_remove() {
        let before = r#"
[cluster]
node_id = 0
[[cluster.voters]]
node_id = 0
addr = "h:0"
[[cluster.voters]]
node_id = 1
addr = "h:1"
"#;
        let after = r#"
[cluster]
node_id = 0
[[cluster.voters]]
node_id = 0
addr = "h:0"
[[cluster.voters]]
node_id = 2
addr = "h:2"
"#;
        let before_raw: toml::Value = toml::from_str(before).unwrap();
        let after_raw: toml::Value = toml::from_str(after).unwrap();
        let before_set = extract_voter_set(&before_raw).unwrap();
        let after_set = extract_voter_set(&after_raw).unwrap();
        assert_ne!(before_set, after_set);
        let added: Vec<_> = after_set.difference(&before_set).copied().collect();
        let removed: Vec<_> = before_set.difference(&after_set).copied().collect();
        assert_eq!(added, vec![2]);
        assert_eq!(removed, vec![1]);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn warm_retry_succeeds_once_in_flight_holder_drops() {
        use std::collections::HashSet;
        use std::sync::Mutex as StdMutex;
        use std::time::Duration;
        use qx_broker::{Broker, BrokerConfig};

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = BrokerConfig::default();
        cfg.workflow_overrides.insert(
            "wf".into(),
            qx_workflow::EngineOptions {
                encode_buf_capacity: 256,
                ..Default::default()
            },
        );
        let broker = Broker::open_with_config(tmp.path(), cfg);

        // Open + retain an Arc to the engine — this is the "in
        // flight caller still holding the redb file lock" state
        // that forces warm_workflow to return false.
        let arc_hold = broker.get_or_open_workflow_for_test("wf").unwrap();
        assert!(broker.workflow_is_cached_for_test("wf"));

        // Trigger an override-evict so the broker drops its
        // cache entry; `arc_hold` keeps the engine alive (and the
        // file lock held).
        let mut new = std::collections::HashMap::new();
        new.insert(
            "wf".into(),
            qx_workflow::EngineOptions {
                encode_buf_capacity: 4096,
                ..Default::default()
            },
        );
        let changed = broker.update_workflow_overrides(new);
        assert_eq!(changed, vec!["wf".to_string()]);
        assert!(!broker.workflow_is_cached_for_test("wf"));

        // Immediate warm fails because the lock is still held.
        assert!(
            !broker.warm_workflow("wf"),
            "warm must fail while in-flight holder keeps the file lock"
        );

        // Spawn the retry task with sub-second timings, drop the
        // Arc after a brief delay so the retry's next attempt
        // succeeds.
        let inflight = std::sync::Arc::new(StdMutex::new(HashSet::new()));
        inflight.lock().unwrap().insert("wf".to_string());
        let broker_for_retry = broker.clone();
        let inflight_for_retry = inflight.clone();
        let retry = tokio::spawn(async move {
            warm_with_retry_with(
                broker_for_retry,
                inflight_for_retry,
                "wf".into(),
                Duration::from_millis(50),
                Duration::from_millis(200),
                Duration::from_secs(5),
            )
            .await
        });

        // Hold the engine for ~150ms — at least one retry attempt
        // must fail before we release.
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(arc_hold);

        let outcome = retry.await.unwrap();
        assert!(outcome, "retry must succeed once the Arc is dropped");
        assert!(
            broker.workflow_is_cached_for_test("wf"),
            "warm should have re-cached the engine"
        );
        // inflight slot is cleared regardless of outcome.
        assert!(inflight.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn warm_retry_gives_up_after_deadline() {
        // Holder never releases — the retry must hit the deadline
        // and clean up `inflight` so a later HUP can re-queue.
        use std::collections::HashSet;
        use std::sync::Mutex as StdMutex;
        use std::time::Duration;
        use qx_broker::{Broker, BrokerConfig};

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = BrokerConfig::default();
        cfg.workflow_overrides.insert(
            "wf".into(),
            qx_workflow::EngineOptions {
                encode_buf_capacity: 256,
                ..Default::default()
            },
        );
        let broker = Broker::open_with_config(tmp.path(), cfg);
        let _hold = broker.get_or_open_workflow_for_test("wf").unwrap();

        let mut new = std::collections::HashMap::new();
        new.insert(
            "wf".into(),
            qx_workflow::EngineOptions {
                encode_buf_capacity: 4096,
                ..Default::default()
            },
        );
        broker.update_workflow_overrides(new);

        let inflight = std::sync::Arc::new(StdMutex::new(HashSet::new()));
        inflight.lock().unwrap().insert("wf".to_string());

        let outcome = warm_with_retry_with(
            broker.clone(),
            inflight.clone(),
            "wf".into(),
            Duration::from_millis(30),
            Duration::from_millis(60),
            Duration::from_millis(150),
        )
        .await;
        assert!(!outcome, "retry must report failure when the deadline expires");
        assert!(inflight.lock().unwrap().is_empty());
    }

    #[test]
    fn tls_fingerprint_unchanged_for_repeat_call() {
        // Same TOML pointing at the same file bytes returns
        // identical fingerprints — the diff-cache short-circuit
        // depends on this.
        let tmp = tempfile::TempDir::new().unwrap();
        let cert_path = tmp.path().join("c.pem");
        let key_path = tmp.path().join("k.pem");
        std::fs::write(&cert_path, b"PEM_CERT").unwrap();
        std::fs::write(&key_path, b"PEM_KEY").unwrap();
        let toml_text = format!(
            r#"
[tls]
cert_file = "{c}"
key_file = "{k}"
"#,
            c = cert_path.display(),
            k = key_path.display(),
        );
        let raw: toml::Value = toml::from_str(&toml_text).unwrap();
        let a = read_tls_fingerprint(&raw).unwrap();
        let b = read_tls_fingerprint(&raw).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.as_ref().map(|t| t.0.as_slice()), Some(b"PEM_CERT".as_slice()));
    }
}
