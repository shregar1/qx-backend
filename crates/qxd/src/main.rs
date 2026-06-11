//! qxd — QX broker daemon.
//!
//! Loads a TOML config (default `qxd.toml`), starts the gRPC broker server
//! and (optionally) the Prometheus metrics HTTP endpoint, and logs what it's
//! serving. Shuts down cleanly on SIGINT.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use qx_broker::{Broker, BrokerConfig, ClusterConfig, KafkaConfig, TlsConfig, bind_with_retry};
use serde::Deserialize;

#[derive(Parser)]
#[command(name = "qxd", about = "QX broker daemon")]
struct Args {
    /// Path to TOML config.
    #[arg(short, long, default_value = "qxd.toml")]
    config: PathBuf,
}

#[derive(Deserialize)]
struct Config {
    server: ServerCfg,
    metrics: Option<MetricsCfg>,
    auth: Option<AuthCfg>,
    tls: Option<TlsCfg>,
    kafka: Option<KafkaCfg>,
    cluster: Option<ClusterCfg>,
    workflow: Option<WorkflowCfg>,
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

    if let Some(wf) = config.workflow.as_ref() {
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
        // Per-workflow overrides inherit anything they don't set
        // from the broker-wide `[workflow]` defaults — operators
        // typically tune one knob per workflow without re-declaring
        // the rest.
        for ov in &wf.overrides {
            broker_config.workflow_overrides.insert(
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
        broker_config.workflow_options = Some(top);

        // Sanity-check: warn for overrides that don't match any
        // workflow already on disk. The override will still apply if
        // a workflow with that name is created later, but at startup
        // a typo is almost certainly an operator mistake — log it
        // loudly instead of silently going inert.
        let known = list_known_workflows(&config.server.data_dir);
        for ov in &wf.overrides {
            if !known.contains(&ov.name) {
                eprintln!(
                    "qxd: WARNING workflow override '{}' does not match any existing workflow under {}/workflows — override will apply if/when that workflow is created. Known workflows: [{}]",
                    ov.name,
                    config.server.data_dir.display(),
                    known.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
        }
    }

    eprintln!("qxd: data_dir = {}", config.server.data_dir.display());
    if broker_config.auth_token.is_some() {
        eprintln!("qxd: bearer-token auth enabled");
    }
    if broker_config.tls.is_some() {
        eprintln!("qxd: TLS enabled");
    }
    if let Some(opts) = broker_config.workflow_options.as_ref() {
        eprintln!(
            "qxd: workflow engine — encode_buf={} cancel_cap={} redb_cache={}",
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
            "qxd: cluster mode enabled — node_id = {}",
            broker.cluster_node_id().unwrap()
        );
    }

    let grpc_addr: SocketAddr = config.server.addr.parse()?;
    let grpc_listener = bind_with_retry(&grpc_addr.to_string()).await?;
    eprintln!("qxd: gRPC listening on {}", grpc_listener.local_addr()?);
    let grpc_task = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.serve_with_listener(grpc_listener).await })
    };

    let metrics_task = if let Some(metrics) = config.metrics {
        let metrics_addr: SocketAddr = metrics.addr.parse()?;
        let metrics_listener = bind_with_retry(&metrics_addr.to_string()).await?;
        eprintln!(
            "qxd: metrics listening on http://{}/metrics",
            metrics_listener.local_addr()?
        );
        let broker = broker.clone();
        Some(tokio::spawn(async move {
            broker.serve_metrics_with_listener(metrics_listener).await
        }))
    } else {
        None
    };

    let kafka_task = if let Some(kafka) = config.kafka {
        let kafka_addr: SocketAddr = kafka.addr.parse()?;
        let kafka_listener = bind_with_retry(&kafka_addr.to_string()).await?;
        let bound = kafka_listener.local_addr()?;
        eprintln!("qxd: Kafka wire-protocol listening on {bound}");
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

    // SIGHUP reloads the TOML config and pushes any changed
    // `[[workflow.override]]` entries through
    // `Broker::update_workflow_overrides`. Spawned as a long-running
    // task so it survives multiple reloads — aborted alongside the
    // other background tasks on shutdown.
    let sighup_task = {
        let broker = broker.clone();
        let config_path = args.config.clone();
        let data_dir = config.server.data_dir.clone();
        #[cfg(unix)]
        let task = tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let mut hup = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("qxd: SIGHUP handler install failed: {e}");
                    return;
                }
            };
            while hup.recv().await.is_some() {
                eprintln!("qxd: SIGHUP — reloading workflow overrides from {}", config_path.display());
                match reload_workflow_overrides(&config_path, &data_dir) {
                    Ok(new_overrides) => {
                        let changed = broker.update_workflow_overrides(new_overrides);
                        if changed.is_empty() {
                            eprintln!("qxd: SIGHUP reload — no workflow overrides changed");
                        } else {
                            eprintln!(
                                "qxd: SIGHUP reload — {} workflow engine(s) will reopen on next access: [{}]",
                                changed.len(),
                                changed.join(", ")
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("qxd: SIGHUP reload failed: {e} — keeping previous overrides");
                    }
                }
            }
        });
        #[cfg(not(unix))]
        let task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            // Non-unix targets don't get SIGHUP; the spawn is a no-op.
            let _ = (broker, config_path, data_dir);
            std::future::pending::<()>().await;
        });
        task
    };

    tokio::select! {
        result = grpc_task => {
            eprintln!("qxd: gRPC server exited: {result:?}");
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("qxd: shutdown signal received");
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

/// Re-parse the TOML config and pull out the
/// `[[workflow.override]]` entries as a fresh
/// `HashMap<name, EngineOptions>`. Used by the SIGHUP handler to feed
/// `Broker::update_workflow_overrides` without restarting the
/// process. Only the `[workflow]` block is honoured here — every
/// other config knob (server.addr, tls, cluster, etc.) is intentionally
/// not hot-reloadable because changing them mid-flight would require
/// rebinding listeners or re-bootstrapping the Raft node.
fn reload_workflow_overrides(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
) -> std::result::Result<
    std::collections::HashMap<String, qx_workflow::EngineOptions>,
    Box<dyn std::error::Error>,
> {
    let toml_str = std::fs::read_to_string(config_path)
        .map_err(|e| format!("reading {}: {e}", config_path.display()))?;
    let parsed: Config = toml::from_str(&toml_str)?;
    let mut out = std::collections::HashMap::new();
    if let Some(wf) = parsed.workflow.as_ref() {
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
            out.insert(
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
        // Same misspelling warning as at startup — repeats on every
        // reload because a SIGHUP is exactly when operators change
        // these entries.
        let known = list_known_workflows(data_dir);
        for ov in &wf.overrides {
            if !known.contains(&ov.name) {
                eprintln!(
                    "qxd: SIGHUP reload — override '{}' has no matching workflow on disk yet",
                    ov.name
                );
            }
        }
    }
    Ok(out)
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
