//! Raft gRPC transport.
//!
//! `RaftGrpcService` is the tonic server that wraps a local `Arc<Raft>` and
//! invokes the corresponding `raft.append_entries`, `raft.vote`, or
//! `raft.install_snapshot` on incoming RPCs. `RaftGrpcFactory` produces
//! `RaftGrpcNetwork` clients that openraft uses to talk to peers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::AnyError;
use openraft::BasicNode;
use openraft::Raft;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::types::{NodeId, TypeConfig};

pub mod proto {
    tonic::include_proto!("qx.raft.v1");
}

pub struct RaftGrpcService {
    raft: Arc<Raft<TypeConfig>>,
}

impl RaftGrpcService {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self { raft }
    }

    pub async fn serve(
        self,
        addr: SocketAddr,
    ) -> std::result::Result<(), tonic::transport::Error> {
        Server::builder()
            .add_service(proto::raft_server::RaftServer::new(self))
            .serve(addr)
            .await
    }

    pub async fn serve_with_listener(
        self,
        listener: TcpListener,
    ) -> std::result::Result<(), tonic::transport::Error> {
        self.serve_with_listener_tls(listener, None).await
    }

    pub async fn serve_with_listener_tls(
        self,
        listener: TcpListener,
        tls: Option<RaftServerTls>,
    ) -> std::result::Result<(), tonic::transport::Error> {
        // rustls 0.23 needs a process-wide CryptoProvider before any TLS
        // handshake. We install `ring` lazily; broker startup also calls
        // this, so two binaries that link both still cooperate.
        install_default_crypto_provider();

        let stream = TcpListenerStream::new(listener);
        let mut builder = Server::builder();
        if let Some(t) = tls {
            let identity = tonic::transport::Identity::from_pem(&t.cert_pem, &t.key_pem);
            builder = builder
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
        }
        builder
            .add_service(proto::raft_server::RaftServer::new(self))
            .serve_with_incoming(stream)
            .await
    }
}

#[tonic::async_trait]
impl proto::raft_server::Raft for RaftGrpcService {
    async fn append_entries(
        &self,
        req: Request<proto::RaftBytes>,
    ) -> std::result::Result<Response<proto::RaftBytes>, Status> {
        let parsed: AppendEntriesRequest<TypeConfig> = serde_json::from_slice(&req.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("decode: {e}")))?;
        let resp = self
            .raft
            .append_entries(parsed)
            .await
            .map_err(|e| Status::internal(format!("raft: {e}")))?;
        let bytes = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("encode: {e}")))?;
        Ok(Response::new(proto::RaftBytes { data: bytes }))
    }

    async fn vote(
        &self,
        req: Request<proto::RaftBytes>,
    ) -> std::result::Result<Response<proto::RaftBytes>, Status> {
        let parsed: VoteRequest<NodeId> = serde_json::from_slice(&req.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("decode: {e}")))?;
        let resp = self
            .raft
            .vote(parsed)
            .await
            .map_err(|e| Status::internal(format!("raft: {e}")))?;
        let bytes = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("encode: {e}")))?;
        Ok(Response::new(proto::RaftBytes { data: bytes }))
    }

    async fn install_snapshot(
        &self,
        req: Request<proto::RaftBytes>,
    ) -> std::result::Result<Response<proto::RaftBytes>, Status> {
        let parsed: InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice(&req.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode: {e}")))?;
        let resp = self
            .raft
            .install_snapshot(parsed)
            .await
            .map_err(|e| Status::internal(format!("raft: {e}")))?;
        let bytes = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("encode: {e}")))?;
        Ok(Response::new(proto::RaftBytes { data: bytes }))
    }
}

#[derive(Clone)]
pub struct RaftServerTls {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

#[derive(Clone)]
pub struct RaftClientTls {
    pub ca_pem: Vec<u8>,
    pub domain: String,
}

#[derive(Clone)]
pub struct RaftGrpcFactory {
    pub addrs: HashMap<NodeId, String>,
    pub client_tls: Option<RaftClientTls>,
}

pub struct RaftGrpcNetwork {
    _target: NodeId,
    addr: String,
    client_tls: Option<RaftClientTls>,
}

impl RaftNetworkFactory<TypeConfig> for RaftGrpcFactory {
    type Network = RaftGrpcNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        let addr = self
            .addrs
            .get(&target)
            .cloned()
            .unwrap_or_else(|| String::from(""));
        RaftGrpcNetwork {
            _target: target,
            addr,
            client_tls: self.client_tls.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for RaftGrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let mut client = connect(&self.addr, &self.client_tls).await.map_err(unreachable)?;
        let bytes = serde_json::to_vec(&rpc).map_err(unreachable)?;
        let resp = client
            .append_entries(proto::RaftBytes { data: bytes })
            .await
            .map_err(unreachable)?;
        let parsed: AppendEntriesResponse<NodeId> =
            serde_json::from_slice(&resp.into_inner().data).map_err(unreachable)?;
        Ok(parsed)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let mut client = connect(&self.addr, &self.client_tls).await.map_err(unreachable)?;
        let bytes = serde_json::to_vec(&rpc).map_err(unreachable)?;
        let resp = client
            .install_snapshot(proto::RaftBytes { data: bytes })
            .await
            .map_err(unreachable)?;
        let parsed: InstallSnapshotResponse<NodeId> =
            serde_json::from_slice(&resp.into_inner().data).map_err(unreachable)?;
        Ok(parsed)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        let mut client = connect(&self.addr, &self.client_tls).await.map_err(unreachable)?;
        let bytes = serde_json::to_vec(&rpc).map_err(unreachable)?;
        let resp = client
            .vote(proto::RaftBytes { data: bytes })
            .await
            .map_err(unreachable)?;
        let parsed: VoteResponse<NodeId> =
            serde_json::from_slice(&resp.into_inner().data).map_err(unreachable)?;
        Ok(parsed)
    }
}

async fn connect(
    addr: &str,
    tls: &Option<RaftClientTls>,
) -> std::result::Result<proto::raft_client::RaftClient<Channel>, tonic::transport::Error> {
    let mut endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())?;
    if let Some(t) = tls {
        install_default_crypto_provider();
        let config = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(t.ca_pem.clone()))
            .domain_name(t.domain.clone());
        endpoint = endpoint.tls_config(config)?;
    }
    let channel = endpoint.connect().await?;
    Ok(proto::raft_client::RaftClient::new(channel))
}

/// Install the `ring` CryptoProvider once per process for rustls 0.23.
/// Idempotent: subsequent calls (or other crates that already installed
/// a provider) silently no-op via the swallowed `Err`.
fn install_default_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

fn unreachable<E, T>(e: E) -> RPCError<NodeId, BasicNode, T>
where
    E: std::error::Error + Send + Sync + 'static,
    T: std::error::Error,
{
    RPCError::Unreachable(Unreachable::new(&AnyError::new(&e)))
}
