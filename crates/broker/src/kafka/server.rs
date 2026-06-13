//! Kafka-wire-protocol TCP listener.
//!
//! Frame layout on the wire is `i32 size + payload`. Inside the payload sits
//! the request header followed by the request body. We dispatch on
//! `(api_key, api_version)` to a handler, then prepend the response header
//! and the size and send it back.
//!
//! Only `ApiVersions` and `Metadata` are wired today — anything else closes
//! the connection.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::admin_stubs;
use super::alter_configs;
use super::api_versions;
use super::codec::{WireError, Writer};
use super::create_partitions;
use super::create_topics;
use super::delete_records;
use super::delete_topics;
use super::describe_cluster;
use super::describe_configs;
use super::describe_quorum;
use super::fetch;
use super::group_apis;
use super::incremental_alter_configs;
use super::log_dirs;
use super::offset_delete;
use super::offset_for_leader_epoch;
use super::sasl;
use super::transactions;
use super::txn_introspect;
use super::header::{response_header_is_flexible, write_response_header, RequestHeader};
use super::list_offsets;
use super::metadata::{self, BrokerInfo, MetadataView, TopicInfo};
use super::produce;
use crate::error::Result;
use crate::server::Broker;

const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// Per-listener configuration for the Kafka endpoint.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// What we advertise to clients as the broker's reachable host.
    /// In a real deployment this is what `KAFKA_ADVERTISED_LISTENERS` would
    /// resolve to; for local dev `127.0.0.1` is fine.
    pub advertised_host: String,
    /// What we advertise as the broker's port. Usually the same port we
    /// listen on, but kept separate so deployments behind a proxy or
    /// container port-map can advertise a different one.
    pub advertised_port: i32,
    /// Cluster id reported in Metadata responses.
    pub cluster_id: String,
    /// node_id for this broker (used as both the Metadata-advertised id
    /// and the FindCoordinator response id).
    pub node_id: i32,
    /// Compression to apply to Fetch responses. 0=none, 1=gzip, 2=snappy,
    /// 3=lz4, 4=zstd. Defaults to none.
    pub fetch_compression: u8,
    /// Other brokers the cluster knows about, advertised in Metadata so
    /// clients see a multi-broker cluster shape. Today these are purely
    /// cosmetic — qx doesn't actually replicate writes to them.
    pub peer_brokers: Vec<PeerBroker>,
    /// Username → password map for SASL/PLAIN authentication. When
    /// `Some`, connections must complete a `SaslHandshake(PLAIN)` +
    /// `SaslAuthenticate` exchange before any data-plane API is allowed.
    /// `None` (the default) leaves the listener anonymous.
    pub sasl_plain_users: Option<std::collections::HashMap<String, String>>,
    /// Username → SCRAM-SHA-256 credential map. Coexists with PLAIN —
    /// the broker advertises both mechanisms and routes per client
    /// selection. Credentials are derived from plaintext at config-load
    /// time and the broker stores only `(salt, iterations, StoredKey,
    /// ServerKey)` — never the plaintext.
    pub sasl_scram_users: Option<std::collections::HashMap<String, super::scram::ScramCredential>>,
    /// Optional TLS for the Kafka listener. When `Some`, every accepted
    /// connection is wrapped in a `tokio-rustls` server stream. PEM-format
    /// cert + key strings; same shape as `BrokerConfig::tls` so an
    /// operator can reuse a single keypair.
    pub tls: Option<crate::server::TlsConfig>,
    /// Group-commit window for the per-partition Produce writer (in
    /// microseconds). 0 = no artificial wait — every request commits
    /// as soon as the inbox drains (lowest latency for a single
    /// producer). Non-zero amortizes the fsync over more requests when
    /// concurrent producers hit the same partition; throughput climbs,
    /// per-request latency rises by up to this window. 200-500 µs is
    /// typical for "I want sub-ms with realistic concurrency".
    pub produce_batch_window_micros: u64,
}

/// Description of another broker in the (advertised) cluster. Used only
/// for Metadata responses today; real cluster membership / replication
/// happens via the `qx-raft` crate, which isn't wired in yet.
#[derive(Debug, Clone)]
pub struct PeerBroker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            advertised_host: "127.0.0.1".to_string(),
            advertised_port: 9092,
            cluster_id: "qx-cluster".to_string(),
            node_id: 0,
            fetch_compression: 0,
            peer_brokers: Vec::new(),
            sasl_plain_users: None,
            sasl_scram_users: None,
            tls: None,
            produce_batch_window_micros: 0,
        }
    }
}

impl Broker {
    /// Serve the Kafka wire protocol on `listener`. Runs until the listener
    /// errors or the task is cancelled. When `config.tls` is `Some`, every
    /// accepted TCP connection is upgraded to TLS before the dispatch
    /// loop sees it.
    pub async fn serve_kafka_with_listener(
        self: Arc<Self>,
        listener: TcpListener,
        config: KafkaConfig,
    ) -> Result<()> {
        let tls_acceptor = match &config.tls {
            Some(t) => Some(build_tls_acceptor(t)?),
            None => None,
        };
        let config = Arc::new(config);
        loop {
            tokio::select! {
                _ = crate::server::wait_for_shutdown(self.shutdown_token()) => {
                    return Ok(());
                }
                accept = listener.accept() => {
                    let (stream, peer) = accept?;
                    let broker = self.clone();
                    let broker_for_log = broker.clone();
                    let config = config.clone();
                    let conn_shutdown = self.shutdown_token();
                    let acceptor = tls_acceptor.clone();
                    tokio::spawn(async move {
                        let result = match acceptor {
                            None => handle_connection(broker, stream, config, conn_shutdown).await,
                            Some(acc) => match acc.accept(stream).await {
                                Ok(tls_stream) => {
                                    handle_connection(broker, tls_stream, config, conn_shutdown).await
                                }
                                Err(e) => Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("tls handshake: {e}"),
                                )),
                            },
                        };
                        if let Err(e) = result {
                            let msg = format!("kafka: connection from {peer} ended: {e}");
                            eprintln!("{msg}");
                            broker_for_log.emit_event(
                                crate::admin_api::EventLevel::Warn,
                                "kafka",
                                msg,
                            );
                        }
                    });
                }
            }
        }
    }

    /// Convenience: bind and serve in one call.
    pub async fn serve_kafka(
        self: Arc<Self>,
        addr: std::net::SocketAddr,
        config: KafkaConfig,
    ) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve_kafka_with_listener(listener, config).await
    }
}

fn build_tls_acceptor(
    tls: &crate::server::TlsConfig,
) -> Result<tokio_rustls::TlsAcceptor> {
    let cfg = crate::server::build_tls_server_config(tls)?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(cfg)))
}

async fn handle_connection<S>(
    broker: Arc<Broker>,
    mut stream: S,
    config: Arc<KafkaConfig>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let sasl_required =
        config.sasl_plain_users.is_some() || config.sasl_scram_users.is_some();
    let mut state = sasl::ConnectionState::new(sasl_required);
    loop {
        let mut size_buf = [0u8; 4];
        let read = tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            r = stream.read_exact(&mut size_buf) => r,
        };
        if let Err(e) = read {
            // EOF on a clean boundary is normal — clients close the socket.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(e);
        }
        let size = i32::from_be_bytes(size_buf);
        if size <= 0 || size as usize > MAX_FRAME_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid kafka frame size {size}"),
            ));
        }
        let mut frame = vec![0u8; size as usize];
        stream.read_exact(&mut frame).await?;
        // `Bytes::from(Vec<u8>)` is zero-copy — it takes ownership of
        // the allocation. Now every downstream slice is a refcount
        // bump rather than a memcpy.
        let frame: bytes::Bytes = frame.into();

        let response = match dispatch(&broker, &config, &mut state, frame).await {
            Ok(bytes) => bytes,
            Err(e) => {
                let msg = format!("kafka: malformed frame: {e}");
                eprintln!("{msg}");
                broker.emit_event(
                    crate::admin_api::EventLevel::Error,
                    "kafka",
                    msg,
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ));
            }
        };

        let mut out = Vec::with_capacity(4 + response.len());
        out.extend_from_slice(&(response.len() as i32).to_be_bytes());
        out.extend_from_slice(&response);
        stream.write_all(&out).await?;
        stream.flush().await?;
    }
}

async fn dispatch(
    broker: &Broker,
    config: &KafkaConfig,
    state: &mut sasl::ConnectionState,
    frame: bytes::Bytes,
) -> std::result::Result<Vec<u8>, WireError> {
    let (header, header_len) = RequestHeader::decode(frame.as_ref())?;
    // `body_bytes` is a refcounted view into `frame` — used for the
    // Produce decoder (which slices further into it). `body` is the
    // plain `&[u8]` view that every other handler expects.
    let body_bytes: bytes::Bytes = frame.slice(header_len..);
    let body: &[u8] = body_bytes.as_ref();

    let mut w = Writer::new();
    let header_flexible = response_header_is_flexible(header.api_key, header.api_version);
    write_response_header(&mut w, header.correlation_id, header_flexible);

    // Auth gate: before SaslAuthenticate succeeds we only let ApiVersions
    // and the SASL APIs through. Anything else gets terminated — the
    // client should not see protocol-level errors here, just a dropped
    // connection (real Kafka does the same).
    if !state.authenticated && !sasl::allowed_pre_auth(header.api_key) {
        return Err(WireError::Eof {
            what: "request issued before SASL authentication",
        });
    }

    match header.api_key {
        api_versions::API_KEY => {
            let _ = api_versions::ApiVersionsRequest::decode(header.api_version, body);
            api_versions::encode_response(&mut w, sanitize_api_versions_version(header.api_version));
        }
        sasl::SASL_HANDSHAKE_KEY => {
            if header.api_version < sasl::SASL_HANDSHAKE_MIN
                || header.api_version > sasl::SASL_HANDSHAKE_MAX
            {
                return Err(WireError::Eof { what: "unsupported sasl_handshake version" });
            }
            sasl::handle_sasl_handshake(&mut w, state, header.api_version, body)?;
        }
        sasl::SASL_AUTHENTICATE_KEY => {
            if header.api_version < sasl::SASL_AUTHENTICATE_MIN
                || header.api_version > sasl::SASL_AUTHENTICATE_MAX
            {
                return Err(WireError::Eof { what: "unsupported sasl_authenticate version" });
            }
            sasl::handle_sasl_authenticate(
                &mut w,
                state,
                header.api_version,
                body,
                config.sasl_plain_users.as_ref(),
                config.sasl_scram_users.as_ref(),
            )?;
        }
        metadata::API_KEY => {
            if header.api_version < metadata::MIN_VERSION
                || header.api_version > metadata::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported metadata version" });
            }
            let req = metadata::MetadataRequest::decode(header.api_version, body)?;
            let view = build_metadata_view(broker, config, &req);
            metadata::encode_response(&mut w, header.api_version, &view);
        }
        produce::API_KEY => {
            if header.api_version < produce::MIN_VERSION
                || header.api_version > produce::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported produce version" });
            }
            // Pass the refcounted body so the decoder can slice each
            // partition's record-batch bytes out as `Bytes` views
            // without allocating.
            let req = produce::ProduceRequest::decode(header.api_version, body_bytes.clone())?;
            let window = std::time::Duration::from_micros(config.produce_batch_window_micros);
            produce::handle_and_encode(&mut w, broker, req, window).await;
        }
        create_topics::API_KEY => {
            if header.api_version < create_topics::MIN_VERSION
                || header.api_version > create_topics::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported create_topics version" });
            }
            let req = create_topics::CreateTopicsRequest::decode(header.api_version, body)?;
            create_topics::handle_and_encode(&mut w, broker, header.api_version, req);
        }
        delete_topics::API_KEY => {
            if header.api_version < delete_topics::MIN_VERSION
                || header.api_version > delete_topics::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported delete_topics version" });
            }
            let req = delete_topics::DeleteTopicsRequest::decode(header.api_version, body)?;
            delete_topics::handle_and_encode(&mut w, broker, req).await;
        }
        delete_records::API_KEY => {
            if header.api_version < delete_records::MIN_VERSION
                || header.api_version > delete_records::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported delete_records version" });
            }
            let req = delete_records::DeleteRecordsRequest::decode(header.api_version, body)?;
            delete_records::handle_and_encode(&mut w, broker, req);
        }
        list_offsets::API_KEY => {
            if header.api_version < list_offsets::MIN_VERSION
                || header.api_version > list_offsets::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported list_offsets version" });
            }
            let req = list_offsets::ListOffsetsRequest::decode(header.api_version, body)?;
            list_offsets::handle_and_encode(&mut w, broker, header.api_version, req);
        }
        fetch::API_KEY => {
            if header.api_version < fetch::MIN_VERSION
                || header.api_version > fetch::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported fetch version" });
            }
            let req = fetch::FetchRequest::decode(header.api_version, body)?;
            fetch::handle_and_encode(&mut w, broker, req, config.fetch_compression);
        }
        group_apis::FIND_COORDINATOR_KEY => {
            if header.api_version < group_apis::FIND_COORDINATOR_MIN
                || header.api_version > group_apis::FIND_COORDINATOR_MAX
            {
                return Err(WireError::Eof { what: "unsupported find_coordinator version" });
            }
            group_apis::handle_find_coordinator(&mut w, config, body)?;
        }
        group_apis::JOIN_GROUP_KEY => {
            if header.api_version < group_apis::JOIN_GROUP_MIN
                || header.api_version > group_apis::JOIN_GROUP_MAX
            {
                return Err(WireError::Eof { what: "unsupported join_group version" });
            }
            group_apis::handle_join_group(&mut w, broker, body).await?;
        }
        group_apis::SYNC_GROUP_KEY => {
            if header.api_version < group_apis::SYNC_GROUP_MIN
                || header.api_version > group_apis::SYNC_GROUP_MAX
            {
                return Err(WireError::Eof { what: "unsupported sync_group version" });
            }
            group_apis::handle_sync_group(&mut w, broker, body).await?;
        }
        group_apis::HEARTBEAT_KEY => {
            if header.api_version < group_apis::HEARTBEAT_MIN
                || header.api_version > group_apis::HEARTBEAT_MAX
            {
                return Err(WireError::Eof { what: "unsupported heartbeat version" });
            }
            group_apis::handle_heartbeat(&mut w, broker, body).await?;
        }
        group_apis::LEAVE_GROUP_KEY => {
            if header.api_version < group_apis::LEAVE_GROUP_MIN
                || header.api_version > group_apis::LEAVE_GROUP_MAX
            {
                return Err(WireError::Eof { what: "unsupported leave_group version" });
            }
            group_apis::handle_leave_group(&mut w, broker, body).await?;
        }
        group_apis::OFFSET_COMMIT_KEY => {
            if header.api_version < group_apis::OFFSET_COMMIT_MIN
                || header.api_version > group_apis::OFFSET_COMMIT_MAX
            {
                return Err(WireError::Eof { what: "unsupported offset_commit version" });
            }
            group_apis::handle_offset_commit(&mut w, broker, body).await?;
        }
        group_apis::OFFSET_FETCH_KEY => {
            if header.api_version < group_apis::OFFSET_FETCH_MIN
                || header.api_version > group_apis::OFFSET_FETCH_MAX
            {
                return Err(WireError::Eof { what: "unsupported offset_fetch version" });
            }
            group_apis::handle_offset_fetch(&mut w, broker, body).await?;
        }
        group_apis::LIST_GROUPS_KEY => {
            if header.api_version < group_apis::LIST_GROUPS_MIN
                || header.api_version > group_apis::LIST_GROUPS_MAX
            {
                return Err(WireError::Eof { what: "unsupported list_groups version" });
            }
            group_apis::handle_list_groups(&mut w, broker, body).await?;
        }
        group_apis::DESCRIBE_GROUPS_KEY => {
            if header.api_version < group_apis::DESCRIBE_GROUPS_MIN
                || header.api_version > group_apis::DESCRIBE_GROUPS_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_groups version" });
            }
            group_apis::handle_describe_groups(&mut w, broker, body).await?;
        }
        describe_configs::API_KEY => {
            if header.api_version < describe_configs::MIN_VERSION
                || header.api_version > describe_configs::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported describe_configs version" });
            }
            let req = describe_configs::DescribeConfigsRequest::decode(header.api_version, body)?;
            describe_configs::handle_and_encode(&mut w, broker, req);
        }
        alter_configs::API_KEY => {
            if header.api_version < alter_configs::MIN_VERSION
                || header.api_version > alter_configs::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported alter_configs version" });
            }
            let req = alter_configs::AlterConfigsRequest::decode(header.api_version, body)?;
            alter_configs::handle_and_encode(&mut w, broker, req);
        }
        create_partitions::API_KEY => {
            if header.api_version < create_partitions::MIN_VERSION
                || header.api_version > create_partitions::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported create_partitions version" });
            }
            let req = create_partitions::CreatePartitionsRequest::decode(header.api_version, body)?;
            create_partitions::handle_and_encode(&mut w, broker, req);
        }
        describe_quorum::API_KEY => {
            if header.api_version < describe_quorum::MIN_VERSION
                || header.api_version > describe_quorum::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported describe_quorum version" });
            }
            describe_quorum::handle_and_encode(&mut w, broker, config, body)?;
        }
        describe_cluster::API_KEY => {
            if header.api_version < describe_cluster::MIN_VERSION
                || header.api_version > describe_cluster::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported describe_cluster version" });
            }
            let _req = describe_cluster::DescribeClusterRequest::decode(header.api_version, body)?;
            describe_cluster::handle_and_encode(&mut w, config);
        }
        offset_for_leader_epoch::API_KEY => {
            if header.api_version < offset_for_leader_epoch::MIN_VERSION
                || header.api_version > offset_for_leader_epoch::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported offset_for_leader_epoch version" });
            }
            let req = offset_for_leader_epoch::OffsetForLeaderEpochRequest::decode(
                header.api_version, body,
            )?;
            offset_for_leader_epoch::handle_and_encode(&mut w, broker, req);
        }
        incremental_alter_configs::API_KEY => {
            if header.api_version < incremental_alter_configs::MIN_VERSION
                || header.api_version > incremental_alter_configs::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported incremental_alter_configs version" });
            }
            let req = incremental_alter_configs::IncrementalAlterConfigsRequest::decode(
                header.api_version, body,
            )?;
            incremental_alter_configs::handle_and_encode(&mut w, broker, req);
        }
        offset_delete::API_KEY => {
            if header.api_version < offset_delete::MIN_VERSION
                || header.api_version > offset_delete::MAX_VERSION
            {
                return Err(WireError::Eof { what: "unsupported offset_delete version" });
            }
            offset_delete::handle_and_encode(&mut w, broker, body).await?;
        }
        log_dirs::DESCRIBE_LOG_DIRS_KEY => {
            if header.api_version < log_dirs::DESCRIBE_LOG_DIRS_MIN
                || header.api_version > log_dirs::DESCRIBE_LOG_DIRS_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_log_dirs version" });
            }
            log_dirs::handle_describe_log_dirs(&mut w, broker, body)?;
        }
        log_dirs::ALTER_REPLICA_LOG_DIRS_KEY => {
            if header.api_version < log_dirs::ALTER_REPLICA_LOG_DIRS_MIN
                || header.api_version > log_dirs::ALTER_REPLICA_LOG_DIRS_MAX
            {
                return Err(WireError::Eof { what: "unsupported alter_replica_log_dirs version" });
            }
            log_dirs::handle_alter_replica_log_dirs(&mut w, body)?;
        }
        txn_introspect::DESCRIBE_PRODUCERS_KEY => {
            if header.api_version < txn_introspect::DESCRIBE_PRODUCERS_MIN
                || header.api_version > txn_introspect::DESCRIBE_PRODUCERS_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_producers version" });
            }
            txn_introspect::handle_describe_producers(&mut w, broker, body)?;
        }
        txn_introspect::LIST_TRANSACTIONS_KEY => {
            if header.api_version < txn_introspect::LIST_TRANSACTIONS_MIN
                || header.api_version > txn_introspect::LIST_TRANSACTIONS_MAX
            {
                return Err(WireError::Eof { what: "unsupported list_transactions version" });
            }
            let registry = broker.txn_registry();
            txn_introspect::handle_list_transactions(&mut w, &registry, body)?;
        }
        txn_introspect::DESCRIBE_TRANSACTIONS_KEY => {
            if header.api_version < txn_introspect::DESCRIBE_TRANSACTIONS_MIN
                || header.api_version > txn_introspect::DESCRIBE_TRANSACTIONS_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_transactions version" });
            }
            let registry = broker.txn_registry();
            txn_introspect::handle_describe_transactions(&mut w, &registry, body)?;
        }
        admin_stubs::DESCRIBE_ACLS_KEY => {
            if header.api_version < admin_stubs::DESCRIBE_ACLS_MIN
                || header.api_version > admin_stubs::DESCRIBE_ACLS_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_acls version" });
            }
            admin_stubs::handle_describe_acls(&mut w, broker, body)?;
        }
        admin_stubs::CREATE_ACLS_KEY => {
            if header.api_version < admin_stubs::CREATE_ACLS_MIN
                || header.api_version > admin_stubs::CREATE_ACLS_MAX
            {
                return Err(WireError::Eof { what: "unsupported create_acls version" });
            }
            admin_stubs::handle_create_acls(&mut w, broker, body)?;
        }
        admin_stubs::DELETE_ACLS_KEY => {
            if header.api_version < admin_stubs::DELETE_ACLS_MIN
                || header.api_version > admin_stubs::DELETE_ACLS_MAX
            {
                return Err(WireError::Eof { what: "unsupported delete_acls version" });
            }
            admin_stubs::handle_delete_acls(&mut w, broker, body)?;
        }
        admin_stubs::DESCRIBE_CLIENT_QUOTAS_KEY => {
            if header.api_version < admin_stubs::DESCRIBE_CLIENT_QUOTAS_MIN
                || header.api_version > admin_stubs::DESCRIBE_CLIENT_QUOTAS_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_client_quotas version" });
            }
            admin_stubs::handle_describe_client_quotas(&mut w, body)?;
        }
        admin_stubs::ALTER_CLIENT_QUOTAS_KEY => {
            if header.api_version < admin_stubs::ALTER_CLIENT_QUOTAS_MIN
                || header.api_version > admin_stubs::ALTER_CLIENT_QUOTAS_MAX
            {
                return Err(WireError::Eof { what: "unsupported alter_client_quotas version" });
            }
            admin_stubs::handle_alter_client_quotas(&mut w, body)?;
        }
        admin_stubs::DESCRIBE_USER_SCRAM_KEY => {
            if header.api_version < admin_stubs::DESCRIBE_USER_SCRAM_MIN
                || header.api_version > admin_stubs::DESCRIBE_USER_SCRAM_MAX
            {
                return Err(WireError::Eof { what: "unsupported describe_user_scram version" });
            }
            admin_stubs::handle_describe_user_scram(&mut w, body)?;
        }
        admin_stubs::ALTER_USER_SCRAM_KEY => {
            if header.api_version < admin_stubs::ALTER_USER_SCRAM_MIN
                || header.api_version > admin_stubs::ALTER_USER_SCRAM_MAX
            {
                return Err(WireError::Eof { what: "unsupported alter_user_scram version" });
            }
            admin_stubs::handle_alter_user_scram(&mut w, body)?;
        }
        admin_stubs::ALTER_PARTITION_REASSIGN_KEY => {
            if header.api_version < admin_stubs::ALTER_PARTITION_REASSIGN_MIN
                || header.api_version > admin_stubs::ALTER_PARTITION_REASSIGN_MAX
            {
                return Err(WireError::Eof { what: "unsupported alter_partition_reassignments version" });
            }
            admin_stubs::handle_alter_partition_reassign(&mut w, body)?;
        }
        admin_stubs::LIST_PARTITION_REASSIGN_KEY => {
            if header.api_version < admin_stubs::LIST_PARTITION_REASSIGN_MIN
                || header.api_version > admin_stubs::LIST_PARTITION_REASSIGN_MAX
            {
                return Err(WireError::Eof { what: "unsupported list_partition_reassignments version" });
            }
            admin_stubs::handle_list_partition_reassign(&mut w, body)?;
        }
        transactions::INIT_PRODUCER_ID_KEY => {
            if header.api_version < transactions::INIT_PRODUCER_ID_MIN
                || header.api_version > transactions::INIT_PRODUCER_ID_MAX
            {
                return Err(WireError::Eof { what: "unsupported init_producer_id version" });
            }
            let registry = broker.txn_registry();
            transactions::handle_init_producer_id(&mut w, &registry, body)?;
        }
        transactions::ADD_PARTITIONS_TO_TXN_KEY => {
            if header.api_version < transactions::ADD_PARTITIONS_TO_TXN_MIN
                || header.api_version > transactions::ADD_PARTITIONS_TO_TXN_MAX
            {
                return Err(WireError::Eof { what: "unsupported add_partitions_to_txn version" });
            }
            let registry = broker.txn_registry();
            transactions::handle_add_partitions_to_txn(&mut w, &registry, body)?;
        }
        transactions::ADD_OFFSETS_TO_TXN_KEY => {
            if header.api_version < transactions::ADD_OFFSETS_TO_TXN_MIN
                || header.api_version > transactions::ADD_OFFSETS_TO_TXN_MAX
            {
                return Err(WireError::Eof { what: "unsupported add_offsets_to_txn version" });
            }
            let registry = broker.txn_registry();
            transactions::handle_add_offsets_to_txn(&mut w, &registry, body)?;
        }
        transactions::END_TXN_KEY => {
            if header.api_version < transactions::END_TXN_MIN
                || header.api_version > transactions::END_TXN_MAX
            {
                return Err(WireError::Eof { what: "unsupported end_txn version" });
            }
            let registry = broker.txn_registry();
            transactions::handle_end_txn(&mut w, broker, &registry, body).await?;
        }
        transactions::WRITE_TXN_MARKERS_KEY => {
            if header.api_version < transactions::WRITE_TXN_MARKERS_MIN
                || header.api_version > transactions::WRITE_TXN_MARKERS_MAX
            {
                return Err(WireError::Eof { what: "unsupported write_txn_markers version" });
            }
            transactions::handle_write_txn_markers(&mut w, body)?;
        }
        transactions::TXN_OFFSET_COMMIT_KEY => {
            if header.api_version < transactions::TXN_OFFSET_COMMIT_MIN
                || header.api_version > transactions::TXN_OFFSET_COMMIT_MAX
            {
                return Err(WireError::Eof { what: "unsupported txn_offset_commit version" });
            }
            let registry = broker.txn_registry();
            transactions::handle_txn_offset_commit(&mut w, &registry, body)?;
        }
        _ => {
            return Err(WireError::Eof { what: "unsupported api key" });
        }
    }

    Ok(w.into_bytes())
}

/// Cap the negotiated ApiVersions response version at what we know how to
/// encode. If a client asks for v99, we still respond with our max.
fn sanitize_api_versions_version(requested: i16) -> i16 {
    let max = api_versions::supported_apis()
        .iter()
        .find(|a| a.api_key == api_versions::API_KEY)
        .map(|a| a.max_version)
        .unwrap_or(0);
    requested.min(max).max(0)
}

fn build_metadata_view(
    broker: &Broker,
    config: &KafkaConfig,
    req: &metadata::MetadataRequest,
) -> MetadataView {
    let all_topics: Vec<TopicInfo> = broker
        .topic_names_and_counts()
        .into_iter()
        .map(|(name, partition_count)| TopicInfo { name, partition_count })
        .collect();

    let mut brokers = vec![BrokerInfo {
        node_id: config.node_id,
        host: config.advertised_host.clone(),
        port: config.advertised_port,
        rack: None,
    }];
    for peer in &config.peer_brokers {
        brokers.push(BrokerInfo {
            node_id: peer.node_id,
            host: peer.host.clone(),
            port: peer.port,
            rack: None,
        });
    }

    let (topics, missing) = match &req.topics {
        None => (all_topics, vec![]),
        Some(asks) => {
            let mut found = Vec::new();
            let mut missing = Vec::new();
            for ask in asks {
                let Some(name) = ask.name.as_deref() else {
                    missing.push(String::from("(unknown topic id)"));
                    continue;
                };
                if let Some(t) = all_topics.iter().find(|t| t.name == name) {
                    found.push(t.clone());
                } else {
                    missing.push(name.to_string());
                }
            }
            (found, missing)
        }
    };

    MetadataView {
        cluster_id: config.cluster_id.clone(),
        controller_id: config.node_id,
        brokers,
        topics,
        missing,
    }
}
