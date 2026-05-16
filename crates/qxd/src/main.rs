//! qxd — QX broker daemon.
//!
//! Loads a TOML config (default `qxd.toml`), starts the gRPC broker server
//! and (optionally) the Prometheus metrics HTTP endpoint, and logs what it's
//! serving. Shuts down cleanly on SIGINT.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use qx_broker::{Broker, BrokerConfig, TlsConfig};
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
        broker_config.tls = Some(TlsConfig { cert_pem, key_pem });
    }

    eprintln!("qxd: data_dir = {}", config.server.data_dir.display());
    if broker_config.auth_token.is_some() {
        eprintln!("qxd: bearer-token auth enabled");
    }
    if broker_config.tls.is_some() {
        eprintln!("qxd: TLS enabled");
    }

    let broker = Broker::open_with_config(&config.server.data_dir, broker_config);

    let grpc_addr: SocketAddr = config.server.addr.parse()?;
    let grpc_listener = tokio::net::TcpListener::bind(grpc_addr).await?;
    eprintln!("qxd: gRPC listening on {}", grpc_listener.local_addr()?);
    let grpc_task = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.serve_with_listener(grpc_listener).await })
    };

    let metrics_task = if let Some(metrics) = config.metrics {
        let metrics_addr: SocketAddr = metrics.addr.parse()?;
        let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
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
    Ok(())
}
