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

    eprintln!("qxd: data_dir = {}", config.server.data_dir.display());
    if broker_config.auth_token.is_some() {
        eprintln!("qxd: bearer-token auth enabled");
    }
    if broker_config.tls.is_some() {
        eprintln!("qxd: TLS enabled");
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

    tokio::select! {
        result = grpc_task => {
            eprintln!("qxd: gRPC server exited: {result:?}");
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("qxd: shutdown signal received");
        }
    }
    if let Some(t) = metrics_task {
        t.abort();
    }
    if let Some(t) = kafka_task {
        t.abort();
    }
    Ok(())
}
