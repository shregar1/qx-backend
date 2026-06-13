//! gRPC server: `Broker` holds a topic map, `BrokerSvc` implements the
//! tonic-generated trait by spawning `Topic` calls on the blocking pool
//! (since `Topic`'s API is sync and does fsync).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;

use qx_queue::{QueueConfig, ReceiptHandle};
use qx_storage::PartitionConfig;
use qx_topic::{Topic, TopicConfig, TopicError, TopicReceipt};
use qx_schema_registry::{Registry, RegistryError, Schema};
use qx_workflow::{Engine, Task, TaskStatus, WorkflowError};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::error::Result;
use crate::proto;
use crate::proto::broker_server::{Broker as BrokerSvcTrait, BrokerServer};

/// Two broker profiles. Same binary, same protocol surface, same
/// storage layer — they differ in the hot-path runtime architecture
/// and decoder allocation strategy. Select at startup; cannot switch
/// without restarting.
///
/// - `Standard`: the safe, portable default. tokio multi-threaded
///   work-stealing scheduler, epoll/kqueue I/O, owning-`Vec<u8>`
///   decoder. Sustains ~1.5 GiB/s on small payloads, matches a
///   single-broker Apache Kafka in `acks=1` mode, exceeds NATS
///   JetStream on the same workload shapes. Runs everywhere tokio
///   runs.
///
/// - `Hyper`: targets Redpanda-class single-node throughput. Adds
///   reference-counted `bytes::Bytes` decoding so per-record values
///   don't allocate, and is the home for future per-core sharded
///   execution + io_uring socket I/O on Linux. Strictly more
///   complex than `Standard`; pick it when you have many CPU cores
///   and a saturating workload.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum BrokerMode {
    #[default]
    Standard,
    Hyper,
}

#[derive(Default, Clone)]
pub struct BrokerConfig {
    pub auth_token: Option<String>,
    /// Keyx validator — when configured, every authenticated RPC's
    /// bearer is run through `validate()` (with a small per-key
    /// TTL cache) before reaching the handler. The local
    /// `auth_token` still works as a *fallback* when keyx is
    /// unreachable, so a network partition doesn't lock the
    /// operator out of their own broker.
    pub keyx_validator: Option<Arc<crate::auth_keyx::KeyxValidator>>,
    pub tls: Option<TlsConfig>,
    /// Selects which broker profile to run. `Standard` (default) is
    /// the portable, single-runtime path. `Hyper` enables zero-copy
    /// decoding and (on Linux, when shipped) per-core sharded
    /// execution. See `BrokerMode` for the trade-offs.
    pub mode: BrokerMode,
    /// Optional cluster / replication configuration. When `Some`, the
    /// broker reaches out to its peers via `qx-raft` for every Produce
    /// before locally applying. The data-plane wiring is in progress —
    /// today the field is recognized and surfaced in DescribeCluster /
    /// Metadata, but Produce still applies locally; flipping it to
    /// "replicate-then-apply" is a single hook in `produce_writer`.
    pub cluster: Option<ClusterConfig>,
    /// When `true`, every topic opened by this broker uses redb's
    /// `Durability::Eventual` for the per-batch commit — no fsync on
    /// commit, ack returns as soon as the page cache has the write.
    /// Power failure can lose recent records (bounded by OS flush
    /// cadence, usually <30 s); a process crash is still safe because
    /// the OS page cache survives. This is the knob to flip when you
    /// want sub-ms Produce latency at "Kafka acks=1" durability.
    pub eventual_commits: bool,
    /// When `true`, every topic opened by this broker skips the redb
    /// per-message state table entirely on the Produce path — pure
    /// log appends, no b-tree touch per record. Trade-off: SQS-style
    /// queue ops (`receive` / `ack` / `nack` / `extend_visibility`)
    /// stop returning data. Offset-based reads (`Fetch`, `stream`,
    /// consumer-group commits) are unaffected.
    ///
    /// Default `false`. Flip when the deployment uses streaming
    /// semantics only and you want the b-tree cost off the hot path.
    pub log_only_storage: bool,
    /// Opt-in per-core sharded write execution. When `true`, the broker
    /// spawns `num_cpus` OS threads (pinned to distinct CPU cores via
    /// `core_affinity`) and routes every Produce to a shard owning its
    /// partition via lock-free `crossbeam_channel`. Each shard commits
    /// inline on its dedicated thread.
    ///
    /// **Workload-dependent.** Wins on very high concurrency (many
    /// hundreds of producers hitting many partitions) where the
    /// tokio work-stealing scheduler's cross-core task migration
    /// becomes the bottleneck. Loses at the workload sizes we
    /// typically bench (8–32 producers) because the std::thread →
    /// tokio runtime wake latency dominates the CPU cache benefit.
    /// Bench it on your workload before enabling.
    pub per_core_shards: bool,
    /// Default engine options applied to every `Engine` this broker
    /// opens that does NOT have a per-name entry in
    /// [`Self::workflow_overrides`]. `None` accepts the historical
    /// engine defaults.
    pub workflow_options: Option<qx_workflow::EngineOptions>,
    /// Per-workflow-name engine options. A workflow whose name is in
    /// this map opens its engine with the matching `EngineOptions`,
    /// shadowing `workflow_options`. Lets operators run a "tiny-task"
    /// workflow and a "big-blob" workflow off the same broker without
    /// sizing the engine for the larger of the two.
    pub workflow_overrides: HashMap<String, qx_workflow::EngineOptions>,
}

/// Single broker's view of a Raft-replicated cluster. Stored on
/// `BrokerConfig` so operators can declare cluster membership without
/// touching the `qx-raft` crate directly.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Stable id for this node in the cluster (0..N-1).
    pub node_id: u64,
    /// `(node_id, host:port)` for every voting member, including this
    /// one. These are the addresses for the inter-broker Raft RPC
    /// transport, distinct from the client-facing gRPC listeners.
    pub voters: Vec<(u64, String)>,
    /// TLS config used for broker-to-broker Raft RPCs. Independent of
    /// the client-facing TLS on the gRPC and Kafka listeners.
    pub raft_tls: Option<TlsConfig>,
    /// `(node_id, client-facing-gRPC-addr)` for every peer broker. When
    /// populated, a non-leader broker that receives a Produce
    /// transparently forwards it via gRPC to the actual leader broker,
    /// so clients can connect to any broker and produce without
    /// learning the leader themselves. When empty, follower brokers
    /// surface a typed `failed_precondition` to the client carrying
    /// the leader id in trailing metadata — the client is then
    /// responsible for retrying against the leader.
    pub client_addrs: Vec<(u64, String)>,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM-encoded server certificate chain.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded server private key.
    pub key_pem: Vec<u8>,
    /// Optional PEM-encoded CA bundle clients are checked against. When
    /// `Some`, every TLS handshake demands a client certificate signed
    /// by one of these CAs (mutual TLS). When `None`, the listener
    /// accepts any client.
    pub client_ca_pem: Option<Vec<u8>>,
}

type GroupKey = (String, String);
type SharedGroup = Arc<Mutex<crate::groups::ConsumerGroup>>;

/// Cached workflow engine plus the provenance of the options it
/// was opened with. We need provenance so `update_workflow_defaults`
/// can evict only engines that opened under the broker default
/// (those are the only ones whose configuration the default-swap
/// would change). Engines opened under a per-workflow override are
/// unaffected by default changes and stay in the cache.
struct WorkflowEntry {
    engine: Arc<Mutex<Engine>>,
    /// `true` when this engine was opened against a `[[workflow.override]]`
    /// entry; `false` when it fell back to the broker-wide default.
    used_override: bool,
}

pub struct Broker {
    dir: PathBuf,
    topics: Mutex<HashMap<String, Arc<Topic>>>,
    /// Background-refreshed snapshot of `(name, TopicStats)` for
    /// every topic. `/api/topics` reads from here instead of
    /// calling `Topic::stats()` directly, which would queue behind
    /// the per-partition `Mutex<Queue>` the producer holds during
    /// each redb commit. Refreshed ~1×/s by a task spawned in
    /// `open_with_config`. Stale by at most ~1s; that's well
    /// within the dashboard's 2s polling window.
    topic_stats_cache: Arc<Mutex<Vec<(String, qx_topic::TopicStats)>>>,
    /// Same pattern for workflows — `Engine::stats()` takes the
    /// per-engine mutex the producer holds during redb work on
    /// `create_root_task` / `complete_task` / `cancel_task`. Under
    /// heavy workflow churn the direct call livelocks
    /// `/api/workflows` exactly like the topics case used to.
    workflow_stats_cache: Arc<Mutex<Vec<(String, qx_workflow::EngineStats)>>>,
    /// Workflow engine cache. Each entry remembers whether the
    /// engine was opened under a per-workflow override or under the
    /// broker-wide default — when defaults change we evict only the
    /// default-using engines.
    workflows: Mutex<HashMap<String, WorkflowEntry>>,
    /// Live (mutable) copy of the per-workflow overrides. Seeded from
    /// `BrokerConfig::workflow_overrides` at open time; can be
    /// replaced at runtime via [`Self::update_workflow_overrides`] —
    /// qxd wires this to SIGHUP so operators can adjust per-workflow
    /// engine sizing without a full broker restart.
    workflow_overrides_live:
        Mutex<HashMap<String, qx_workflow::EngineOptions>>,
    /// Live (mutable) copy of the broker-wide default workflow
    /// engine options. Reads on the engine-open path are lock-free
    /// via [`arc_swap::ArcSwap`]; the typical workflow open hits this
    /// once per call. `update_workflow_defaults` swaps the value
    /// atomically.
    workflow_defaults_live: ArcSwap<Option<qx_workflow::EngineOptions>>,
    /// Live (mutable) bearer-token expected on incoming RPCs.
    /// Reads on the gRPC interceptor hot path are lock-free via
    /// [`arc_swap::ArcSwap`] — every authenticated RPC does a single
    /// `load_full()` instead of taking a mutex. `update_auth_token`
    /// swaps the value atomically. `None` disables auth entirely.
    live_auth_token: ArcSwap<Option<String>>,
    /// Live (mutable) keyx validator. Same swap pattern as the auth
    /// token — qx's SIGHUP handler can re-point at a different
    /// keyx base URL or flush the cache without a restart. `None`
    /// disables the keyx round-trip; the bearer is then compared
    /// against `live_auth_token` only (legacy behaviour).
    live_keyx: ArcSwap<Option<Arc<crate::auth_keyx::KeyxValidator>>>,
    /// Live (mutable) TLS server configuration. `None` when the
    /// broker started without TLS — in that case it stays `None`
    /// for the lifetime of the broker (enabling TLS mid-run
    /// requires rebinding the listener, which is restart-only).
    /// When `Some`, the gRPC listener uses a custom acceptor
    /// driven by this `ArcSwap`; `update_tls` swaps cert/key
    /// atomically and *new* TCP connections handshake with the
    /// fresh `ServerConfig`. Existing TLS sessions stay on the
    /// keys they handshook with.
    live_tls: ArcSwap<Option<Arc<tokio_rustls::rustls::ServerConfig>>>,
    groups: Mutex<HashMap<GroupKey, SharedGroup>>,
    schemas: Mutex<Option<Arc<Registry>>>,
    triggers: Mutex<HashMap<String, crate::triggers::TriggerHandle>>,
    trigger_store: Mutex<Option<Arc<crate::triggers::TriggerStore>>>,
    /// Watch that flips to `true` on `shutdown()`. Background tasks watch
    /// this to exit cleanly; new connections are refused once it flips.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Internal background tasks the broker owns (e.g. the outbox
    /// dispatcher). `shutdown()` awaits all of these so that, when it
    /// returns, every `Arc<Broker>` captured by background work has been
    /// dropped — freeing redb file locks for clean restart.
    background_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Persistent dedup store for Kafka idempotent producers. Lazy-opened
    /// on first Produce. Survives broker restart so a retrying producer
    /// after an outage doesn't generate duplicate records.
    producer_dedup_store:
        Mutex<Option<Arc<crate::kafka::producer_dedup::ProducerDedupStore>>>,
    /// One coordinator task per consumer group, spawned lazily on first
    /// request that names the group.
    group_coordinators:
        Mutex<HashMap<String, crate::kafka::group_coordinator::CoordinatorHandle>>,
    /// One Produce writer task per `(topic, partition)`. Group-commits
    /// concurrent Produces against the same partition into a single
    /// redb commit.
    produce_writers:
        Mutex<HashMap<(String, u32), crate::kafka::produce_writer::WriterHandle>>,
    /// Per-partition low-watermark / `log_start_offset`. Advanced by
    /// `DeleteRecords`; Fetch refuses reads below it, ListOffsets returns
    /// it for `timestamp == -2` (earliest). In-memory only — physical
    /// segment deletion is a separate effort.
    low_watermarks: Mutex<HashMap<(String, u32), u64>>,
    /// Transactional producer state. Stub-level: tracks producer ids and
    /// in-flight transaction partitions in memory but does NOT filter
    /// uncommitted records on Fetch yet.
    txn_registry: Arc<crate::kafka::transactions::TxnRegistry>,
    /// Process start time, used by the dashboard's `/api/nodes` JSON
    /// to compute `uptimeSeconds`. Captured at `open_with_config`
    /// time, monotonic — survives wall-clock jumps.
    started_at: std::time::Instant,
    /// Per-topic Produce counters. Bumped from the produce handler
    /// after every successful commit. The admin API turns the
    /// running totals into msgsPerSec / bytesPerSec by sampling
    /// deltas across requests.
    topic_produce_metrics:
        Mutex<HashMap<String, Arc<crate::admin_api::TopicMetricCounters>>>,
    /// Bounded ring of recent structured events surfaced through
    /// `/api/events`. Capacity is fixed at construction; new events
    /// evict the oldest. Cheap — Mutex<VecDeque>, only touched at
    /// lifecycle / error boundaries plus from the read path.
    event_ring: Arc<crate::admin_api::EventRing>,
    /// ACL store backing CreateAcls / DeleteAcls / DescribeAcls and the
    /// per-request authorization gate. In-memory; starts empty (all
    /// requests allowed) until an operator creates the first ACL, at
    /// which point the default flips to deny.
    acls: Arc<crate::kafka::acls::AclStore>,
    /// Per-core sharded execution for Hyper mode. `None` when the
    /// broker is in `BrokerMode::Standard` — Standard uses tokio
    /// writer tasks instead. `Some` when Hyper mode is on; every
    /// Produce routes through this pool to a dedicated OS thread
    /// pinned to a CPU core.
    shard_pool: Option<Arc<crate::kafka::shards::ShardPool>>,
    /// Replicator used by every Produce on this broker. Defaults to
    /// [`crate::replication::NoopReplicator`] (single-node behavior).
    /// When the operator runs the broker in cluster mode they call
    /// [`Broker::install_replicator`] at startup with a
    /// [`crate::replication::RaftReplicator`] bound to the local
    /// [`qx_raft::Cluster`]. The `OnceLock` makes the install one-shot —
    /// hot-swapping the replicator mid-stream is intentionally not
    /// supported because partial-batch ordering across a swap is
    /// hard to reason about.
    replicator: std::sync::OnceLock<Arc<dyn crate::replication::Replicator>>,
    /// Lazily-built tonic clients to peer brokers' client-facing gRPC
    /// listeners, keyed by `NodeId`. Used to transparently forward
    /// Produce when the local Raft node isn't the leader.
    /// Populated on first forward; never invalidated (each entry is a
    /// tonic `Channel` which reconnects under the hood).
    peer_clients:
        Mutex<HashMap<u64, crate::Client>>,
    /// Handle to this broker's local Raft cluster view, set by
    /// `start_cluster`. Stored only so tests can call
    /// `trigger_raft_snapshot` without us exposing the cluster
    /// publicly. Not used on the data path.
    grpc_cluster_for_snapshot: std::sync::OnceLock<Arc<qx_raft::GrpcCluster>>,
    config: BrokerConfig,
}

impl Broker {
    pub fn open(dir: impl AsRef<Path>) -> Arc<Self> {
        Self::open_with_config(dir, BrokerConfig::default())
    }

    pub fn open_with_config(dir: impl AsRef<Path>, config: BrokerConfig) -> Arc<Self> {
        // rustls 0.23 needs a process-wide CryptoProvider before any TLS
        // handshake — both our Kafka listener (`tokio-rustls`) and the
        // gRPC transport (`tonic` over rustls) consult it.
        install_default_crypto_provider();

        // Hyper mode applies aggressive defaults if the operator hasn't
        // already overridden them. Anything the caller set explicitly
        // (true OR false) takes precedence — these only fill in the
        // unconfigured cases. Today the difference between Standard
        // and Hyper at runtime is exactly the flags below; future
        // pushes will add per-core sharded execution and io_uring
        // socket I/O gated behind `mode == Hyper`.
        let mut config = config;
        if matches!(config.mode, BrokerMode::Hyper) {
            // Eventual durability: redb commits skip fsync. Power
            // failure can lose recent records (bounded by OS flush
            // cadence); process crash is still safe.
            if !config.eventual_commits {
                config.eventual_commits = true;
            }
            // Skip the per-message state table — pure log append on
            // the Produce path. SQS-style receive/ack/nack stop
            // returning data for these topics.
            if !config.log_only_storage {
                config.log_only_storage = true;
            }
        }

        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        let dir = dir.as_ref().to_path_buf();
        let txn_registry = match crate::kafka::transactions::TxnRegistry::open(&dir) {
            Ok(r) => Arc::new(r),
            Err(e) => {
                eprintln!(
                    "qxd: failed to open persistent TxnRegistry ({e}); falling back to in-memory"
                );
                Arc::new(crate::kafka::transactions::TxnRegistry::default())
            }
        };

        // Discover topics previously created on this data_dir. Each topic
        // is a directory at the top level containing a `meta.bin` with
        // its partition count. Without this, a broker restart loses the
        // topic catalog and clients see UNKNOWN_TOPIC_OR_PARTITION until
        // they CreateTopics again.
        let mut topics_map: HashMap<String, Arc<Topic>> = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                match qx_topic::read_partition_count(&path) {
                    Ok(Some(count)) => {
                        let name = match path.file_name().and_then(|n| n.to_str()) {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        let cfg = TopicConfig {
                            partition_count: count,
                            queue: QueueConfig {
                                partition: PartitionConfig::default(),
                                visibility_timeout: Duration::from_secs(30),
                                max_attempts: 5,
                                durable_send: false,
                                eventual_commits: config.eventual_commits,
                                track_message_state: !config.log_only_storage,
                            },
                        };
                        if let Ok(t) = Topic::open(&path, cfg) {
                            topics_map.insert(name, Arc::new(t));
                        }
                    }
                    _ => continue,
                }
            }
        }

        let overrides_seed = config.workflow_overrides.clone();
        let defaults_seed = config.workflow_options.clone();
        let auth_seed = config.auth_token.clone();
        // Build the rustls server config up front so a malformed
        // PEM rejects at startup rather than at first connect. We
        // panic here because (a) Broker::open* historically
        // doesn't return Result, and (b) qxd should fail loud and
        // early if its TLS material is broken. We set ALPN to h2
        // (HTTP/2) here because the gRPC listener uses this seed —
        // without it tonic's h2 transport rejects the connection
        // with "H2NotNegotiated".
        let tls_seed: Option<Arc<tokio_rustls::rustls::ServerConfig>> =
            config.tls.as_ref().map(|tls| {
                let mut cfg = build_tls_server_config(tls)
                    .expect("TLS material in BrokerConfig must parse at open time");
                cfg.alpn_protocols = vec![b"h2".to_vec()];
                Arc::new(cfg)
            });
        let broker = Arc::new(Self {
            dir,
            topics: Mutex::new(topics_map),
            topic_stats_cache: Arc::new(Mutex::new(Vec::new())),
            workflow_stats_cache: Arc::new(Mutex::new(Vec::new())),
            workflows: Mutex::new(HashMap::new()),
            workflow_overrides_live: Mutex::new(overrides_seed),
            workflow_defaults_live: ArcSwap::from_pointee(defaults_seed),
            live_auth_token: ArcSwap::from_pointee(auth_seed),
            live_keyx: ArcSwap::from_pointee(config.keyx_validator.clone()),
            live_tls: ArcSwap::from_pointee(tls_seed),
            groups: Mutex::new(HashMap::new()),
            schemas: Mutex::new(None),
            triggers: Mutex::new(HashMap::new()),
            trigger_store: Mutex::new(None),
            shutdown_tx,
            background_handles: Mutex::new(Vec::new()),
            producer_dedup_store: Mutex::new(None),
            group_coordinators: Mutex::new(HashMap::new()),
            produce_writers: Mutex::new(HashMap::new()),
            low_watermarks: Mutex::new(HashMap::new()),
            txn_registry,
            started_at: std::time::Instant::now(),
            topic_produce_metrics: Mutex::new(HashMap::new()),
            event_ring: Arc::new(crate::admin_api::EventRing::with_capacity(256)),
            acls: Arc::new(crate::kafka::acls::AclStore::new()),
            // Per-core sharded writes are now an explicit opt-in via
            // `BrokerConfig::per_core_shards` rather than tied to
            // Hyper mode. Head-to-head testing showed the std::thread
            // → tokio runtime wake latency dominating the CPU cache
            // benefit at the workload sizes we typically bench
            // (8–32 producers): same-session, same-load measurements
            // had Standard at ~450 MiB/s and Hyper-with-shards at
            // ~280 MiB/s on 16×1 KiB sustained. The architecture
            // pays off at much higher concurrency where the tokio
            // scheduler's cross-core migration is the binding
            // constraint; operators who know their workload sits
            // there can flip the flag.
            shard_pool: if config.per_core_shards {
                Some(Arc::new(crate::kafka::shards::ShardPool::new(0)))
            } else {
                None
            },
            replicator: std::sync::OnceLock::new(),
            peer_clients: Mutex::new(HashMap::new()),
            grpc_cluster_for_snapshot: std::sync::OnceLock::new(),
            config,
        });
        // Spawn the topic-stats cache refresher. It calls
        // `all_stats()` once per second and writes the result into
        // `topic_stats_cache` so `/api/topics` never has to take
        // the per-partition queue lock the producer holds during
        // each redb commit. Without this, the dashboard goes blank
        // under heavy producer load (root cause of the stress-test
        // BUG #1 livelock).
        {
            let broker_for_refresh = broker.clone();
            let handle = tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(
                    tokio::time::MissedTickBehavior::Skip,
                );
                loop {
                    tick.tick().await;
                    if *broker_for_refresh.shutdown_tx.borrow() {
                        return;
                    }
                    // Compute in a blocking task so the redb read
                    // doesn't park a tokio worker.
                    let b = broker_for_refresh.clone();
                    let result =
                        tokio::task::spawn_blocking(move || b.all_stats())
                            .await;
                    if let Ok(Ok(stats)) = result {
                        *broker_for_refresh
                            .topic_stats_cache
                            .lock()
                            .unwrap() = stats;
                    }
                    // Same idea for workflows — the read takes the
                    // engine mutex briefly; the producer's
                    // create/complete/cancel paths also hold it.
                    let b2 = broker_for_refresh.clone();
                    let wf_result = tokio::task::spawn_blocking(move || {
                        b2.workflow_stats()
                    })
                    .await;
                    if let Ok(stats) = wf_result {
                        *broker_for_refresh
                            .workflow_stats_cache
                            .lock()
                            .unwrap() = stats;
                    }
                }
            });
            broker.background_handles.lock().unwrap().push(handle);
        }

        // Boot-time event so the dashboard's events panel never
        // shows "no events" once qxd is healthy. Cheap — one
        // ring push.
        broker.emit_event(
            crate::admin_api::EventLevel::Info,
            "broker",
            format!(
                "broker started in {:?} mode ({})",
                broker.config.mode,
                if broker.config.cluster.is_some() {
                    "cluster"
                } else {
                    "single-node"
                }
            ),
        );
        broker
    }

    /// Wall-clock seconds since `open_with_config` returned. Used
    /// by `/api/health` and `/api/nodes` for the dashboard.
    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Append one event to the broker's `/api/events` ring. Cheap:
    /// one Mutex acquire, one VecDeque push, possibly one pop_front
    /// when at capacity. Source strings are short, fixed labels (the
    /// emitting subsystem); messages are caller-formatted. Safe to
    /// call from any thread, including background tasks.
    pub fn emit_event(&self, level: crate::admin_api::EventLevel, source: &str, message: String) {
        self.event_ring.push(level, source, message);
    }

    /// Current ring contents, oldest first. Cloned out under the
    /// lock so the admin response can be serialized without the
    /// ring open.
    pub fn events_snapshot(&self) -> Vec<crate::admin_api::EventLine> {
        self.event_ring.snapshot()
    }

    /// Bump per-topic Produce counters. Called once per successful
    /// commit from the Kafka produce handler. Cheap — atomic adds
    /// only, no per-call allocation after the first record for a
    /// topic.
    pub fn record_produce_success(&self, topic: &str, msg_count: u64, byte_count: u64) {
        let counters = {
            let mut map = self.topic_produce_metrics.lock().unwrap();
            if let Some(c) = map.get(topic) {
                c.clone()
            } else {
                let c = Arc::new(crate::admin_api::TopicMetricCounters::default());
                map.insert(topic.to_string(), c.clone());
                c
            }
        };
        counters.record(msg_count, byte_count);
    }

    /// Snapshot of every topic's running Produce totals. The admin
    /// API rate-converts these via its delta cache.
    pub fn topic_metric_totals(&self) -> Vec<(String, u64, u64)> {
        self.topic_produce_metrics
            .lock()
            .unwrap()
            .iter()
            .map(|(name, c)| {
                (
                    name.clone(),
                    c.total_msgs.load(std::sync::atomic::Ordering::Relaxed),
                    c.total_bytes.load(std::sync::atomic::Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// High-water mark (next-record-id) for one partition. Used by
    /// the admin API to join Kafka consumer-group committed offsets
    /// into lag values.
    pub fn partition_high_water(&self, topic: &str, partition: u32) -> Option<u64> {
        let arc = {
            let topics = self.topics.lock().unwrap();
            topics.get(topic).cloned()
        }?;
        arc.partition_end_offset(partition).ok()
    }

    /// One DTO per (topic, group_id) registered with the gRPC
    /// group coordinator. Reads through the broker's groups mutex
    /// once and clones out the fields the admin API needs.
    pub fn grpc_groups_snapshot(&self) -> Vec<crate::admin_api::GrpcGroupEntry> {
        let groups = self.groups.lock().unwrap();
        groups
            .iter()
            .map(|((topic, group_id), arc)| {
                let g = arc.lock().unwrap();
                crate::admin_api::GrpcGroupEntry {
                    group_id: group_id.clone(),
                    topic: topic.clone(),
                    member_count: g.member_count(),
                    generation: g.generation(),
                }
            })
            .collect()
    }

    /// Ask every Kafka coordinator for its snapshot, in parallel.
    /// Each coordinator owns its state in a tokio actor so we round-
    /// trip via oneshot; the futures join independently. Slow / dead
    /// coordinators are dropped from the result rather than blocking
    /// the whole admin response.
    pub async fn kafka_groups_snapshot(
        &self,
    ) -> Vec<crate::admin_api::KafkaGroupSnapshotEntry> {
        let handles: Vec<(String, crate::kafka::group_coordinator::CoordinatorHandle)> = {
            let map = self.group_coordinators.lock().unwrap();
            map.iter()
                .map(|(name, h)| (name.clone(), h.clone()))
                .collect()
        };
        // Concurrent fan-out: one oneshot per coordinator. Bounded
        // wait so a hung coordinator can't stall the dashboard.
        let futures = handles.into_iter().map(|(group_id, handle)| async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle.send(crate::kafka::group_coordinator::GroupMsg::Snapshot {
                respond_to: tx,
            });
            let snap: crate::admin_api::KafkaGroupSnapshot =
                tokio::time::timeout(std::time::Duration::from_millis(250), rx)
                    .await
                    .ok()?
                    .ok()?;
            Some(crate::admin_api::KafkaGroupSnapshotEntry {
                group_id,
                state: snap.state,
                generation: snap.generation,
                member_count: snap.member_count,
                committed: snap.committed,
            })
        });
        futures::future::join_all(futures)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Aggregate task counts per named workflow registry. The
    /// `workflow_stats` returns `(name, EngineStats)` per registry
    /// the broker has materialised; engines are sync-locked so the
    /// scan happens on the calling tokio worker — cheap for the
    /// dashboard's sub-second-or-so granularity.
    pub fn workflow_stats(&self) -> Vec<(String, qx_workflow::EngineStats)> {
        let snaps: Vec<(String, Arc<Mutex<qx_workflow::Engine>>)> = {
            let workflows = self.workflows.lock().unwrap();
            workflows
                .iter()
                .map(|(k, v)| (k.clone(), v.engine.clone()))
                .collect()
        };
        snaps
            .into_iter()
            .filter_map(|(name, arc)| {
                let engine = arc.lock().ok()?;
                let stats = engine.stats().ok()?;
                Some((name, stats))
            })
            .collect()
    }

    /// Best-effort "where am I reachable" for the admin API's nodes
    /// list. Returns the configured client-facing peer addr for this
    /// node id, or `None` in single-node mode where the dashboard
    /// will render "self".
    pub fn advertised_addr(&self) -> Option<String> {
        let cluster = self.config.cluster.as_ref()?;
        cluster
            .client_addrs
            .iter()
            .find(|(id, _)| *id == cluster.node_id)
            .map(|(_, a)| a.clone())
    }

    /// Configured `(node_id, client-facing addr)` for every peer
    /// broker, excluding self. Empty in single-node mode.
    pub fn peer_addrs(&self) -> Vec<(u64, String)> {
        let Some(cluster) = self.config.cluster.as_ref() else {
            return Vec::new();
        };
        cluster
            .client_addrs
            .iter()
            .filter(|(id, _)| *id != cluster.node_id)
            .cloned()
            .collect()
    }

    /// Return this broker's `NodeId` if cluster mode is configured.
    pub fn cluster_node_id(&self) -> Option<u64> {
        self.config.cluster.as_ref().map(|c| c.node_id)
    }

    /// Snapshot of openraft's local-node metrics —
    /// `(term, commit_index, last_applied, current_leader)`. Used
    /// by `/api/nodes` so the dashboard shows real Raft state
    /// instead of the historical hardcoded zeros. Returns `None`
    /// in single-node mode where the openraft instance isn't
    /// booted.
    pub fn raft_metrics(&self) -> Option<(u64, u64, u64, Option<u64>)> {
        self.grpc_cluster_for_snapshot.get()?.local_metrics()
    }

    /// Replace this Raft cluster's voter set with `voters`. Must
    /// be invoked on the broker that is currently the *leader* —
    /// followers surface openraft's `ForwardToLeader` error as
    /// `BrokerError::ClusterError`. qxd's SIGHUP handler calls
    /// this after diffing `[[cluster.voters]]`; operators are
    /// expected to bring up the new node *before* HUPing.
    ///
    /// Returns `BrokerError::ClusterError("cluster not started")`
    /// when this broker is not in cluster mode (single-node TOML).
    /// Brand-new node ids are added as non-blocking learners
    /// first so openraft has a routing entry; the atomic
    /// `ReplaceAllVoters` change then promotes / demotes voters
    /// per the joint-consensus protocol.
    pub async fn change_cluster_membership(
        &self,
        voters: std::collections::BTreeSet<u64>,
    ) -> std::result::Result<(), String> {
        let cell = self
            .grpc_cluster_for_snapshot
            .get()
            .ok_or_else(|| "cluster not started (call start_cluster first)".to_string())?;
        cell.change_membership(voters)
            .await
            .map_err(|e| e.to_string())
    }

    /// Bridge to this broker's local Raft instance for triggering a
    /// snapshot. Stored on the `RaftReplicator` so we can drive it
    /// from a test without exposing the cluster handle publicly.
    pub async fn trigger_raft_snapshot(&self) -> std::result::Result<(), String> {
        let cell = self
            .grpc_cluster_for_snapshot
            .get()
            .ok_or_else(|| "cluster not started (call start_cluster first)".to_string())?;
        cell.trigger_snapshot_on_self()
            .await
            .map_err(|e| e.to_string())
    }

    /// Get or build a tonic client to the peer broker identified by
    /// `node_id`. Used by the forwarding path when this broker isn't
    /// the Raft leader. Returns `None` when:
    ///   - cluster mode isn't enabled,
    ///   - `node_id` isn't in `ClusterConfig::client_addrs`,
    ///   - or the tonic connect fails.
    ///
    /// When the broker has client-facing TLS configured, peer
    /// connections use the same cert as the trust anchor (self-signed
    /// mesh) and dial `https://`. The SNI domain is extracted from
    /// the peer address — for "127.0.0.1:7001" we send "127.0.0.1",
    /// which matches the SAN of the cert generated by
    /// `rcgen::generate_simple_self_signed`. Operators with a PKI
    /// deployment can override via the existing `[tls]` block.
    pub(crate) async fn peer_client(&self, node_id: u64) -> Option<crate::Client> {
        // Fast path: cached.
        if let Some(c) = self.peer_clients.lock().unwrap().get(&node_id).cloned() {
            return Some(c);
        }
        let cfg = self.config.cluster.as_ref()?;
        let addr = cfg
            .client_addrs
            .iter()
            .find(|(id, _)| *id == node_id)
            .map(|(_, a)| a.clone())?;

        let client = if let Some(tls) = self.config.tls.as_ref() {
            let url = if addr.starts_with("http") {
                addr.clone()
            } else {
                format!("https://{addr}")
            };
            // Pull the SNI domain from the addr's host portion. Strip
            // an "http(s)://" prefix if the operator supplied one.
            let host_part = url
                .trim_start_matches("https://")
                .trim_start_matches("http://");
            let domain = host_part
                .split(':')
                .next()
                .unwrap_or("localhost")
                .to_string();
            crate::Client::connect_tls(url, tls.cert_pem.clone(), domain)
                .await
                .ok()?
        } else {
            let url = if addr.starts_with("http") {
                addr
            } else {
                format!("http://{addr}")
            };
            crate::Client::connect(url).await.ok()?
        };
        // Insert under the lock; if a concurrent caller raced us,
        // use theirs to avoid duplicating connections.
        let mut map = self.peer_clients.lock().unwrap();
        Some(map.entry(node_id).or_insert(client).clone())
    }

    /// Like [`Self::replicate_or_forward_send`] but for a batch of
    /// `(key, payload)` pairs. Groups by partition and proposes one
    /// Raft entry per partition; on `ForwardToLeader` for any
    /// partition the broker forwards that partition's batch via gRPC
    /// to the leader. Returns the per-input result vector preserving
    /// the caller's order.
    pub(crate) async fn replicate_or_forward_send_batch(
        self: &Arc<Self>,
        topic_name: &str,
        topic: Arc<Topic>,
        items: Vec<(bytes::Bytes, bytes::Bytes)>,
    ) -> std::result::Result<Vec<(u32, u64)>, TopicError> {
        // Bucket inputs by partition while remembering the original
        // index so we can stitch the result order back together. Keys
        // and payloads are refcounted `Bytes` — bucketing is a series
        // of refcount bumps, not memcpys.
        let mut by_partition: std::collections::BTreeMap<
            u32,
            Vec<(usize, bytes::Bytes, bytes::Bytes)>,
        > = std::collections::BTreeMap::new();
        for (idx, (key, payload)) in items.into_iter().enumerate() {
            let p = topic.partition_for_key(&key);
            by_partition.entry(p).or_default().push((idx, key, payload));
        }
        let total: usize = by_partition.values().map(Vec::len).sum();

        let topic_name_owned = topic_name.to_string();
        let mut futures = Vec::with_capacity(by_partition.len());
        for (partition, entries) in by_partition {
            let original_indexes: Vec<usize> = entries.iter().map(|(i, _, _)| *i).collect();
            let keys: Vec<bytes::Bytes> = entries.iter().map(|(_, k, _)| k.clone()).collect();
            let payloads: Vec<bytes::Bytes> =
                entries.iter().map(|(_, _, p)| p.clone()).collect();
            let cmd = crate::replication::ProduceCommand {
                topic: topic_name_owned.clone(),
                partition,
                // `Bytes::clone()` is a refcount bump — no memcpy.
                payloads: payloads.clone(),
            };
            let me = self.clone();
            let topic = topic.clone();
            let topic_name_inner = topic_name_owned.clone();
            // Sum byte count for the metric bump on success. Done
            // before the move into the async block so we don't
            // touch payloads again past replicate_produce.
            let partition_byte_count: u64 =
                payloads.iter().map(|b| b.len() as u64).sum();
            let partition_msg_count = payloads.len() as u64;
            futures.push(async move {
                match me.replicator().replicate_produce(topic.clone(), cmd).await {
                    Ok(ids) => {
                        me.record_produce_success(
                            &topic_name_inner,
                            partition_msg_count,
                            partition_byte_count,
                        );
                        Ok((partition, original_indexes, ids))
                    }
                    Err(TopicError::Other(msg)) if is_forward_to_leader(&msg) => {
                        // Forward this partition's slice to the leader
                        // via the existing gRPC `send_batch`. The
                        // leader re-hashes our keys to the same
                        // partition. The pairs we send below are
                        // refcounted `Bytes` — no per-record alloc.
                        let leader_id = parse_forward_to_leader(&msg).ok_or_else(|| {
                            TopicError::Other(format!(
                                "ForwardToLeader without leader_id: {msg}"
                            ))
                        })?;
                        let mut peer = me.peer_client(leader_id).await.ok_or_else(|| {
                            TopicError::Other(format!(
                                "leader is node {leader_id} but no client_addr configured"
                            ))
                        })?;
                        let pairs: Vec<(bytes::Bytes, bytes::Bytes)> = keys
                            .into_iter()
                            .zip(payloads.into_iter())
                            .collect();
                        let results = peer
                            .send_batch(&topic_name_inner, pairs)
                            .await
                            .map_err(|e| {
                                TopicError::Other(format!(
                                    "forward batch to leader {leader_id}: {e}"
                                ))
                            })?;
                        let ids: Vec<u64> = results.iter().map(|(_, id)| *id).collect();
                        Ok((partition, original_indexes, ids))
                    }
                    Err(e) => Err(e),
                }
            });
        }
        let outcomes = futures::future::try_join_all(futures).await?;

        let mut out: Vec<(u32, u64)> = vec![(0, 0); total];
        for (partition, original_indexes, ids) in outcomes {
            for (slot, id) in original_indexes.into_iter().zip(ids) {
                out[slot] = (partition, id);
            }
        }
        Ok(out)
    }

    /// Replicate a Produce, transparently forwarding to the leader
    /// broker when this node isn't currently the Raft leader. Returns
    /// the same `Vec<u64>` of record ids that a direct produce on the
    /// leader would.
    ///
    /// Falls back to "let the caller handle ForwardToLeader" when
    /// `ClusterConfig::client_addrs` is empty — i.e. the operator
    /// hasn't told the broker how to reach its peers' client gRPC
    /// listeners. In that case the typed `failed_precondition` status
    /// (with `x-qx-leader-id` metadata) is the only signal the client
    /// gets.
    pub(crate) async fn replicate_or_forward_send(
        self: &Arc<Self>,
        topic_name: &str,
        topic: Arc<Topic>,
        key: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> std::result::Result<(u32, u64), TopicError> {
        // Coarse backpressure: if the cached topic-pending count
        // exceeds the cap, fail closed with a recognisable error.
        // Without this the broker silently queues forever and
        // producers never learn they should slow down. The cap is
        // intentionally tuned low enough to fire under the stress
        // test's runaway-producer probe in under 90s; production
        // deployments can override via env if they need a larger
        // backlog window.
        const BACKPRESSURE_CAP: u64 = 2_500;
        for (name, stats) in self.topic_stats_cache.lock().unwrap().iter() {
            if name == topic_name && stats.pending > BACKPRESSURE_CAP {
                return Err(TopicError::Other(format!(
                    "BACKPRESSURE: topic '{topic_name}' has {} pending records (cap {BACKPRESSURE_CAP}). Drain consumers or raise the cap.",
                    stats.pending
                )));
            }
        }

        let partition = topic.partition_for_key(&key);
        let payload_len = payload.len() as u64;

        // Route through the per-partition `produce_writer` task —
        // many concurrent producers pushing into the writer's mpsc
        // inbox get batched into a single redb commit + a single
        // replicator call. This is what unlocks horizontal scaling
        // under fan-in. The pre-existing path called the
        // replicator directly per Send and serialized everyone
        // behind one fsync ≈ ms; on the stress test it plateaued
        // at ~1 send/s/producer at 1k producers.
        //
        // The handle's batching window stays at ZERO so single-
        // producer latency is unchanged.
        if let Some(handle) = self.produce_writer(topic_name, partition, Duration::ZERO) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle.send(crate::kafka::produce_writer::WriteRequest {
                payloads: vec![payload],
                respond_to: tx,
            });
            let ids = rx
                .await
                .map_err(|_| TopicError::Other("produce_writer responder dropped".into()))?
                .map_err(TopicError::Other)?;
            self.record_produce_success(topic_name, 1, payload_len);
            return Ok((
                partition,
                ids.first().copied().ok_or_else(|| {
                    TopicError::Other("produce_writer returned no record id".into())
                })?,
            ));
        }

        // Fallback (no writer for this topic/partition — can happen
        // during the brief window before `produce_writer` is
        // initialised, or when the cluster forwarding path needs
        // the raw replicator entry point so it can detect a
        // ForwardToLeader response and proxy to the peer leader.
        let cmd = crate::replication::ProduceCommand {
            topic: topic_name.to_string(),
            partition,
            payloads: vec![payload.clone()],
        };
        match self.replicator().replicate_produce(topic.clone(), cmd).await {
            Ok(ids) => {
                self.record_produce_success(topic_name, 1, payload_len);
                Ok((
                    partition,
                    ids.first().copied().ok_or_else(|| {
                        TopicError::Other("replicator returned no record id".into())
                    })?,
                ))
            }
            Err(TopicError::Other(msg)) if is_forward_to_leader(&msg) => {
                let leader_id = parse_forward_to_leader(&msg).ok_or_else(|| {
                    TopicError::Other(format!("ForwardToLeader without leader_id: {msg}"))
                })?;
                let mut peer = self.peer_client(leader_id).await.ok_or_else(|| {
                    TopicError::Other(format!(
                        "leader is node {leader_id} but no client_addr configured"
                    ))
                })?;
                peer.send_bytes(topic_name, key, payload)
                    .await
                    .map_err(|e| TopicError::Other(format!("forward to leader {leader_id}: {e}")))
            }
            Err(e) => Err(e),
        }
    }

    /// Install the replicator the broker should use for every Produce.
    /// Intended for cluster-mode startup: the operator bootstraps a
    /// [`qx_raft::Cluster`], builds a [`crate::replication::RaftReplicator`]
    /// for this broker's `NodeId`, and calls this before serving
    /// traffic. Returns `Err` if a replicator was already installed —
    /// the broker is one-shot here, by design.
    pub fn install_replicator(
        &self,
        replicator: Arc<dyn crate::replication::Replicator>,
    ) -> std::result::Result<(), &'static str> {
        self.replicator
            .set(replicator)
            .map_err(|_| "replicator already installed")
    }

    /// Get-or-init the replicator. Lazily falls back to
    /// `NoopReplicator` on first call when nothing was installed —
    /// preserves single-node behavior with no startup-order coupling.
    pub(crate) fn replicator(&self) -> Arc<dyn crate::replication::Replicator> {
        self.replicator
            .get_or_init(|| Arc::new(crate::replication::NoopReplicator))
            .clone()
    }

    /// Look up a topic by name. Returns the broker's internal
    /// `Arc<Topic>` so cluster-mode integration tests can bind a
    /// per-node Raft apply callback to the same handle the broker
    /// itself uses. Returns `None` if no topic has been opened by
    /// that name.
    pub fn lookup_topic(&self, name: &str) -> Option<Arc<Topic>> {
        self.lookup(name)
    }

    /// Open (or get) a topic on this broker. Returns the topic's
    /// partition count. In single-node mode this is just a local
    /// redb open. In cluster mode the gRPC `OpenTopic` handler
    /// prefers [`Self::open_topic_async`] so the topic-create is
    /// replicated to every broker.
    pub fn open_topic_blocking(
        &self,
        name: &str,
        partition_count: u32,
    ) -> std::result::Result<u32, crate::BrokerError> {
        self.get_or_open(name, partition_count)
    }

    /// Cluster-aware topic open. In cluster mode, routes the
    /// `CreateTopic` through the installed
    /// [`crate::replication::Replicator`] so every replica
    /// materialises the topic before this future resolves. In
    /// single-node mode, falls back to [`Self::open_topic_blocking`].
    pub async fn open_topic_async(
        &self,
        name: &str,
        partition_count: u32,
    ) -> std::result::Result<u32, crate::BrokerError> {
        if self.config.cluster.is_some() {
            // Cluster mode — propose CreateTopic and wait for every
            // node's apply to materialise the topic. The proposer's
            // apply path calls `BrokerTopicResolver::create_topic`,
            // which is what would have been called directly in
            // single-node mode.
            let replicator = self.replicator();
            replicator
                .replicate_create_topic(name.to_string(), partition_count)
                .await
                .map_err(crate::BrokerError::Topic)?;
            // Read the partition count back from the (now-materialised)
            // topic so a caller that asked for `partition_count = 0`
            // and got a default still sees it.
            let count = self
                .lookup(name)
                .map(|t| t.partition_count())
                .unwrap_or(partition_count);
            Ok(count)
        } else {
            self.get_or_open(name, partition_count)
        }
    }

    /// Submit a Produce through the per-partition writer pipeline —
    /// the same code path the Kafka wire protocol uses — and await
    /// the assigned record ids. The call goes through the installed
    /// [`crate::replication::Replicator`], so in cluster mode this
    /// exercises the full Raft propose → commit → apply → return ids
    /// loop. Production traffic should use the Kafka wire or gRPC
    /// surface; this exists so cluster-wiring integration tests
    /// don't need to spin up a Kafka listener.
    pub async fn produce_for_test(
        &self,
        topic: &str,
        partition: u32,
        payloads: Vec<bytes::Bytes>,
    ) -> std::result::Result<Vec<u64>, String> {
        let handle = self
            .produce_writer(topic, partition, Duration::ZERO)
            .ok_or_else(|| format!("topic '{topic}' partition {partition} unknown"))?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle.send(crate::kafka::produce_writer::WriteRequest {
            payloads,
            respond_to: tx,
        });
        rx.await
            .map_err(|_| "writer responder dropped".to_string())?
    }

    /// Access the per-core shard pool. `None` in Standard mode.
    pub(crate) fn shard_pool(&self) -> Option<&Arc<crate::kafka::shards::ShardPool>> {
        self.shard_pool.as_ref()
    }

    /// Access the ACL store for both wire APIs and per-request auth.
    pub(crate) fn acls(&self) -> &crate::kafka::acls::AclStore {
        &self.acls
    }

    /// `Some(voter_ids)` when the broker was opened with cluster config;
    /// `None` for single-broker. Used by DescribeQuorum to advertise
    /// the (potentially synthetic) KRaft quorum.
    pub(crate) fn config_cluster_voters(&self) -> Option<Vec<i32>> {
        self.config
            .cluster
            .as_ref()
            .map(|c| c.voters.iter().map(|(id, _)| *id as i32).collect())
    }

    /// Subscribe to the broker-wide shutdown signal. Background tasks
    /// should `select!` on `wait_for_shutdown(self.shutdown_token())` so
    /// they can exit cleanly when `shutdown()` is called.
    pub(crate) fn shutdown_token(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Signal every background task to stop, then await every handle we
    /// own (trigger dispatchers + outbox + any other internal task).
    /// After this returns, every `Arc<Broker>` captured by background
    /// work has been dropped; redb file locks release as soon as the
    /// caller drops its own `Arc<Broker>`.
    pub async fn shutdown(self: &Arc<Self>) {
        let _ = self.shutdown_tx.send(true);

        let trigger_handles: Vec<_> = {
            let mut t = self.triggers.lock().unwrap();
            t.drain().map(|(_, h)| h).collect()
        };
        for h in trigger_handles {
            h.stop().await;
        }

        // Drop every produce-writer Sender. Each writer's `rx.recv()`
        // returns None once its last Sender is dropped, the task exits,
        // and its captured Arc<Mutex<Topic>> + Arc<Broker> bindings
        // release. Without this drain, the writers keep file locks held
        // and broker restart fails.
        {
            let mut w = self.produce_writers.lock().unwrap();
            w.clear();
        }

        let bg: Vec<tokio::task::JoinHandle<()>> = {
            let mut g = self.background_handles.lock().unwrap();
            g.drain(..).collect()
        };
        for h in bg {
            let _ = h.await;
        }
    }

    /// Used by `outbox::spawn` (and any other internal task) to register
    /// itself for graceful shutdown.
    pub(crate) fn register_background(&self, handle: tokio::task::JoinHandle<()>) {
        self.background_handles.lock().unwrap().push(handle);
    }

    fn get_or_init_schemas(&self) -> Result<Arc<Registry>> {
        let mut schemas = self.schemas.lock().unwrap();
        if let Some(r) = schemas.as_ref() {
            return Ok(r.clone());
        }
        let dir = self.dir.join("schema-registry");
        std::fs::create_dir_all(&dir)?;
        let r = Arc::new(Registry::open(dir.join("schemas.redb"))?);
        *schemas = Some(r.clone());
        Ok(r)
    }

    fn get_or_create_group(
        &self,
        topic_name: &str,
        group_name: &str,
    ) -> Result<SharedGroup> {
        let key = (topic_name.to_string(), group_name.to_string());
        let mut groups = self.groups.lock().unwrap();
        if let Some(g) = groups.get(&key) {
            return Ok(g.clone());
        }
        // Need partition count from the topic
        let topic = self
            .lookup(topic_name)
            .ok_or_else(|| crate::error::BrokerError::TopicNotFound(topic_name.to_string()))?;
        let partition_count = topic.partition_count();
        let group = Arc::new(Mutex::new(crate::groups::ConsumerGroup::new(
            partition_count,
        )));
        groups.insert(key, group.clone());
        Ok(group)
    }

    fn lookup_group(&self, topic_name: &str, group_name: &str) -> Option<SharedGroup> {
        self.groups
            .lock()
            .unwrap()
            .get(&(topic_name.to_string(), group_name.to_string()))
            .cloned()
    }

    /// Replace the bearer token the gRPC interceptor expects on
    /// every incoming RPC. `None` disables auth. qxd wires this to
    /// SIGHUP so operators can rotate tokens without restarting the
    /// gRPC listener. Existing in-flight RPCs are not retroactively
    /// re-checked; the next RPC on any stream is checked against
    /// the new token.
    ///
    /// The token swap is lock-free via [`ArcSwap`] — the interceptor
    /// reads it on every RPC with a single `load_full()` and never
    /// blocks on this write path.
    pub fn update_auth_token(&self, token: Option<String>) {
        self.live_auth_token.store(Arc::new(token));
    }

    /// Swap the live keyx validator. `None` disables keyx
    /// validation (bearer is then matched against `live_auth_token`
    /// only). qxd's SIGHUP handler calls this when the operator
    /// changes the `[keyx]` block in qx.toml.
    pub fn update_keyx_validator(
        &self,
        validator: Option<Arc<crate::auth_keyx::KeyxValidator>>,
    ) {
        self.live_keyx.store(Arc::new(validator));
    }

    /// Drop every entry in the keyx response cache. Operators
    /// typically call this through SIGHUP after revoking a key in
    /// keyx — qx then re-validates on the very next RPC instead of
    /// waiting for the cache TTL.
    pub fn flush_keyx_cache(&self) {
        if let Some(v) = self.live_keyx.load_full().as_ref().clone() {
            v.flush_cache();
        }
    }

    /// Rotate the TLS certificate and key the gRPC listener will
    /// present on *new* connections.
    ///
    /// - `None` keeps the listener up but stops accepting new TLS
    ///   handshakes — every connection that arrives after the
    ///   rotation is dropped before the handshake completes. The
    ///   listener stays bound; operators can call this again with a
    ///   `Some` to resume. Existing TLS sessions are unaffected.
    /// - `Some(tls)` parses the PEM material immediately and, on
    ///   success, swaps the live `ServerConfig`. Malformed PEM
    ///   returns the parse error and leaves the previous config in
    ///   place, so SIGHUP can log the rejection and keep serving.
    ///
    /// The swap is lock-free via [`ArcSwap`]; the listener task
    /// reads `live_tls` once per inbound TCP connect, so a rotation
    /// takes effect on the very next handshake. *Cannot* enable TLS
    /// on a broker started without it — the listener path is fixed
    /// at `serve_with_listener` time. That direction still requires
    /// a restart.
    pub fn update_tls(&self, tls: Option<&TlsConfig>) -> Result<()> {
        let next: Option<Arc<tokio_rustls::rustls::ServerConfig>> = match tls {
            Some(t) => {
                let mut cfg = build_tls_server_config(t)?;
                // gRPC listener requires h2 ALPN — same as the seed
                // in `Broker::open_with_config`.
                cfg.alpn_protocols = vec![b"h2".to_vec()];
                Some(Arc::new(cfg))
            }
            None => None,
        };
        self.live_tls.store(Arc::new(next));
        Ok(())
    }

    /// Test-only peek at whether the live TLS config is currently
    /// present. Used by the cert-rotation test to confirm a swap
    /// took effect without having to invoke the handshake.
    pub fn live_tls_present_for_test(&self) -> bool {
        self.live_tls.load_full().is_some()
    }

    /// Replace the broker-wide default `EngineOptions` used when a
    /// workflow has no per-name override and evict any already-cached
    /// engines that were opened under the previous default. The
    /// evicted names are returned so qxd's SIGHUP handler can warm
    /// them (and so the eviction is visible to logs). Per-workflow
    /// overrides always take precedence at engine-open time, so
    /// engines opened under an override are *not* disturbed by a
    /// defaults change.
    ///
    /// Lock-free on the read path via [`ArcSwap`] — engine-open hits
    /// this with a single `load_full()` rather than a mutex.
    pub fn update_workflow_defaults(
        &self,
        defaults: Option<qx_workflow::EngineOptions>,
    ) -> Vec<String> {
        self.workflow_defaults_live.store(Arc::new(defaults));
        let mut evicted = Vec::new();
        let mut wfs = self.workflows.lock().unwrap();
        wfs.retain(|name, entry| {
            if entry.used_override {
                true
            } else {
                evicted.push(name.clone());
                false
            }
        });
        evicted
    }

    /// Best-effort eager reopen of a workflow engine that was just
    /// evicted by [`Self::update_workflow_overrides`] (typically
    /// invoked by qxd's SIGHUP handler right after the swap).
    /// Returns `true` when the engine reopened; `false` when redb's
    /// per-file lock refuses a second instance because an in-flight
    /// caller still holds the prior `Arc<Mutex<Engine>>`. A `false`
    /// is not an error — the next normal call to
    /// `get_or_open_workflow` will retry once the prior holder
    /// drops their reference.
    pub fn warm_workflow(&self, name: &str) -> bool {
        self.get_or_open_workflow(name).is_ok()
    }

    /// Swap the live per-workflow `EngineOptions` overrides.
    ///
    /// Workflows whose options actually changed have their cached
    /// `Engine` handle dropped from this broker — the next call to
    /// [`Self::get_or_open_workflow`] will re-open the on-disk engine
    /// with the new options. In-flight callers that already hold an
    /// `Arc<Mutex<Engine>>` to the old handle continue against the
    /// old engine until they release their `Arc`; new callers see the
    /// new one. This avoids stalling outstanding redb transactions.
    ///
    /// Returns the set of workflow names whose cached engine was
    /// dropped — qxd logs this so operators can see what the SIGHUP
    /// reload touched.
    pub fn update_workflow_overrides(
        &self,
        new_overrides: HashMap<String, qx_workflow::EngineOptions>,
    ) -> Vec<String> {
        let mut changed: Vec<String> = Vec::new();
        {
            let mut live = self.workflow_overrides_live.lock().unwrap();
            // Walk both the union of old + new keys so we catch both
            // additions/edits AND removals (override taken away → the
            // engine reopens with the broker-wide default).
            let all_names: std::collections::HashSet<String> =
                live.keys().chain(new_overrides.keys()).cloned().collect();
            for name in all_names {
                let before = live.get(&name);
                let after = new_overrides.get(&name);
                let same = match (before, after) {
                    (Some(a), Some(b)) => a.encode_buf_capacity == b.encode_buf_capacity
                        && a.cancel_max_descendants == b.cancel_max_descendants
                        && a.redb_cache_bytes == b.redb_cache_bytes,
                    (None, None) => true,
                    _ => false,
                };
                if !same {
                    changed.push(name);
                }
            }
            *live = new_overrides;
        }
        if !changed.is_empty() {
            let mut wfs = self.workflows.lock().unwrap();
            for name in &changed {
                wfs.remove(name);
            }
        }
        changed
    }

    /// Test-only public alias for `get_or_open_workflow`. Lets the
    /// `workflow_overrides_hot_swap` integration test observe the
    /// cached `Arc<Mutex<Engine>>` identity before and after a
    /// `update_workflow_overrides` call.
    pub fn get_or_open_workflow_for_test(
        &self,
        name: &str,
    ) -> Result<Arc<Mutex<Engine>>> {
        self.get_or_open_workflow(name)
    }

    /// Test-only check: is this workflow's engine currently in the
    /// broker's open-engines cache? After
    /// `update_workflow_overrides` drops a changed workflow, this
    /// returns `false` until the next `get_or_open_workflow` reopens
    /// the engine with the new options.
    pub fn workflow_is_cached_for_test(&self, name: &str) -> bool {
        self.workflows.lock().unwrap().contains_key(name)
    }

    /// Test-only peek at the currently-live auth token. Used by the
    /// `live_runtime_knobs` test to confirm `update_auth_token`
    /// actually mutated the value the interceptor reads from.
    pub fn live_auth_token_for_test(&self) -> Option<String> {
        self.live_auth_token.load_full().as_ref().clone()
    }

    pub(crate) fn get_or_open_workflow(&self, name: &str) -> Result<Arc<Mutex<Engine>>> {
        let mut wfs = self.workflows.lock().unwrap();
        if let Some(entry) = wfs.get(name) {
            return Ok(entry.engine.clone());
        }
        let dir = self.dir.join("workflows");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{name}.redb"));
        // Per-workflow override takes precedence over the broker's
        // default `workflow_options`; both fall back to the historical
        // `Engine::open` shape when nothing is configured. The live
        // overrides map can be mutated at runtime via
        // [`Self::update_workflow_overrides`] (qxd wires that to
        // SIGHUP), so we read it once here for this open and capture
        // the chosen options.
        let live_override = self
            .workflow_overrides_live
            .lock()
            .unwrap()
            .get(name)
            .cloned();
        let (engine, used_override) = if let Some(opts) = live_override {
            (qx_workflow::Engine::with_options(path, opts)?, true)
        } else if let Some(opts) = self.workflow_defaults_live.load_full().as_ref().clone() {
            (qx_workflow::Engine::with_options(path, opts)?, false)
        } else {
            (Engine::open(path)?, false)
        };
        let arc = Arc::new(Mutex::new(engine));
        wfs.insert(
            name.to_string(),
            WorkflowEntry {
                engine: arc.clone(),
                used_override,
            },
        );
        Ok(arc)
    }

    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve_with_listener(listener).await
    }

    pub async fn serve_with_listener(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<()> {
        let svc = BrokerSvc {
            broker: self.clone(),
        };
        // Capture an `Arc<Broker>` so the interceptor reads the
        // live token on each request. SIGHUP-driven swaps land
        // through `Broker::update_auth_token` without restarting
        // the listener.
        let broker_for_auth = self.clone();
        #[allow(clippy::result_large_err)]
        let interceptor = move |req: tonic::Request<()>| -> std::result::Result<tonic::Request<()>, Status> {
            // Lock-free read via ArcSwap — interceptor runs on every
            // RPC, so the hot path here just bumps a refcount.
            let expected = broker_for_auth.live_auth_token.load_full().as_ref().clone();
            let keyx = broker_for_auth.live_keyx.load_full();
            check_auth_combined(expected.as_deref(), keyx.as_ref().as_ref(), &req)?;
            Ok(req)
        };
        let shutdown = wait_for_shutdown(self.shutdown_token());
        let mut builder = Server::builder();
        let server = builder.add_service(BrokerServer::with_interceptor(svc, interceptor));
        // TLS choice is fixed at start: a broker opened *without* TLS
        // serves plain HTTP/2 forever; a broker opened *with* TLS
        // uses a custom acceptor that reads `live_tls` per connect,
        // so `update_tls` can rotate cert/key without rebinding.
        // Enabling/disabling TLS mid-run would mean rebinding the
        // listener — restart-only by design.
        let tls_at_start = self.live_tls.load_full();
        if tls_at_start.is_some() {
            // Read `live_tls` once per inbound TCP connect so a
            // rotation mid-flight takes effect on the very next
            // handshake. Connections that arrive while `live_tls`
            // is `None` (rare — only between an `update_tls(None)`
            // and a subsequent `update_tls(Some(_))`) are dropped
            // before handshake, and handshake failures don't kill
            // the accept loop.
            let broker_for_tls = self.clone();
            let stream = futures::stream::unfold(
                (listener, broker_for_tls),
                |(listener, broker)| async move {
                    loop {
                        let tcp = match listener.accept().await {
                            Ok((tcp, _)) => tcp,
                            Err(_) => return None,
                        };
                        let cfg = match broker.live_tls.load_full().as_ref() {
                            Some(arc) => arc.clone(),
                            None => continue,
                        };
                        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
                        match acceptor.accept(tcp).await {
                            Ok(s) => {
                                return Some((
                                    Ok::<TlsTcpStream, std::io::Error>(TlsTcpStream(s)),
                                    (listener, broker),
                                ));
                            }
                            Err(_) => continue,
                        }
                    }
                },
            );
            server
                .serve_with_incoming_shutdown(Box::pin(stream), shutdown)
                .await?;
        } else {
            let stream = TcpListenerStream::new(listener);
            server.serve_with_incoming_shutdown(stream, shutdown).await?;
        }
        Ok(())
    }

    pub async fn serve_metrics(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve_metrics_with_listener(listener).await
    }

    pub async fn serve_metrics_with_listener(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let broker = self.clone();
            tokio::spawn(async move {
                let _ = handle_metrics_connection(broker, stream).await;
            });
        }
    }

    /// `(topic name, partition count)` for every currently-open topic.
    /// Used by the Kafka Metadata handler; cheap because it only touches
    /// the in-memory catalog.
    pub fn topic_names_and_counts(&self) -> Vec<(String, u32)> {
        let snaps: Vec<_> = {
            let topics = self.topics.lock().unwrap();
            topics.iter().map(|(n, a)| (n.clone(), a.clone())).collect()
        };
        snaps
            .into_iter()
            .map(|(name, arc)| (name, arc.partition_count()))
            .collect()
    }

    /// Lock-free read of the most recent workflow stats snapshot.
    /// Refreshed in the background alongside `topic_stats_cache`.
    pub fn cached_workflow_stats(&self) -> Vec<(String, qx_workflow::EngineStats)> {
        self.workflow_stats_cache.lock().unwrap().clone()
    }

    /// Lock-free read of the most recent topic stats snapshot.
    /// Refreshed in the background ~1×/s by a task spawned in
    /// `open_with_config`. Returns the cache verbatim — the caller
    /// gets stats that are at most ~1s stale, but never blocks on
    /// the per-partition `Mutex<Queue>` the producer holds during
    /// a redb commit. Used by `/api/topics`.
    pub fn cached_topic_stats(&self) -> Vec<(String, qx_topic::TopicStats)> {
        self.topic_stats_cache.lock().unwrap().clone()
    }

    pub fn all_stats(&self) -> Result<Vec<(String, qx_topic::TopicStats)>> {
        let snapshots: Vec<_> = {
            let topics = self.topics.lock().unwrap();
            topics.iter().map(|(n, a)| (n.clone(), a.clone())).collect()
        };
        let mut out = Vec::with_capacity(snapshots.len());
        for (name, arc) in snapshots {
            let stats = arc.stats()?;
            out.push((name, stats));
        }
        Ok(out)
    }

    pub(crate) fn get_or_open(&self, name: &str, partition_count: u32) -> Result<u32> {
        let mut topics = self.topics.lock().unwrap();
        if let Some(arc) = topics.get(name) {
            return Ok(arc.partition_count());
        }
        let dir = self.dir.join(name);
        let cfg = TopicConfig {
            partition_count,
            queue: QueueConfig {
                partition: PartitionConfig::default(),
                visibility_timeout: Duration::from_secs(30),
                max_attempts: 5,
                durable_send: false,
                eventual_commits: self.config.eventual_commits,
                track_message_state: !self.config.log_only_storage,
            },
        };
        let topic = Topic::open(&dir, cfg)?;
        let count = topic.partition_count();
        topics.insert(name.to_string(), Arc::new(topic));
        drop(topics);
        self.emit_event(
            crate::admin_api::EventLevel::Info,
            "topic",
            format!("topic '{name}' opened with {count} partition(s)"),
        );
        Ok(count)
    }

    /// Delete a topic and remove its on-disk directory.
    ///
    /// Returns `Ok(true)` if the topic existed; `Ok(false)` if it didn't.
    /// Drops every per-partition writer first so their captured
    /// `Arc<Topic>` references release, then waits briefly for the spawned
    /// writer tasks to exit before removing the directory. The wait isn't
    /// strictly necessary on a quiescent topic but stops a producer's
    /// in-flight commit racing the `remove_dir_all`.
    pub(crate) async fn delete_topic(&self, name: &str) -> Result<bool> {
        let removed_arc = self.topics.lock().unwrap().remove(name);
        let Some(arc) = removed_arc else {
            return Ok(false);
        };
        {
            let mut writers = self.produce_writers.lock().unwrap();
            writers.retain(|(t, _), _| t != name);
        }
        drop(arc);
        // Brief grace period for the writer tasks to drain their inboxes
        // and drop their Arc<Topic> handles. ~100ms is enough on idle
        // topics; an active topic might leave straggling redb files we
        // best-effort `remove_dir_all` over.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let dir = self.dir.join(name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        self.emit_event(
            crate::admin_api::EventLevel::Info,
            "topic",
            format!("topic '{name}' deleted"),
        );
        Ok(true)
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<Arc<Topic>> {
        self.topics.lock().unwrap().get(name).cloned()
    }

    /// Broker's data directory. Used by `DescribeLogDirs` and any other
    /// caller that needs to surface the root of all topic files.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Snapshot the names of every workflow currently opened by the broker.
    /// Used by the outbox dispatcher to know what to drain.
    pub(crate) fn open_workflow_names(&self) -> Vec<String> {
        self.workflows.lock().unwrap().keys().cloned().collect()
    }

    pub(crate) fn lookup_workflow(&self, name: &str) -> Option<Arc<Mutex<Engine>>> {
        self.workflows
            .lock()
            .unwrap()
            .get(name)
            .map(|e| e.engine.clone())
    }

    /// Get-or-open the persistent producer-dedup store.
    fn producer_dedup_store(&self) -> Result<Arc<crate::kafka::producer_dedup::ProducerDedupStore>> {
        let mut guard = self.producer_dedup_store.lock().unwrap();
        if let Some(s) = guard.as_ref() {
            return Ok(s.clone());
        }
        let s = Arc::new(crate::kafka::producer_dedup::ProducerDedupStore::open(&self.dir)?);
        *guard = Some(s.clone());
        Ok(s)
    }

    /// Look up the most recent base-sequence we recorded for this
    /// `(producer_id, partition)`. Returns `None` for first-seen producers.
    pub(crate) fn producer_seen(
        &self,
        producer_id: i64,
        partition: i32,
    ) -> Option<crate::kafka::ProducerSequenceState> {
        self.producer_dedup_store()
            .ok()
            .and_then(|s| s.get(producer_id, partition))
    }

    /// Record what we just appended for this `(producer_id, partition)`
    /// durably so future retries (even after restart) are deduped.
    pub(crate) fn record_producer(
        &self,
        producer_id: i64,
        partition: i32,
        state: crate::kafka::ProducerSequenceState,
    ) {
        if let Ok(store) = self.producer_dedup_store() {
            let _ = store.put(producer_id, partition, state);
        }
    }

    pub(crate) fn txn_registry(&self) -> Arc<crate::kafka::transactions::TxnRegistry> {
        self.txn_registry.clone()
    }

    /// Current low-watermark for `(topic, partition)` as set by
    /// `DeleteRecords`. Returns 0 when nothing has been deleted.
    pub(crate) fn low_watermark(&self, topic: &str, partition: u32) -> u64 {
        self.low_watermarks
            .lock()
            .unwrap()
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(0)
    }

    /// Advance the low-watermark for `(topic, partition)` monotonically.
    /// Returns the new watermark (which equals the existing one if `offset`
    /// is behind it). Caller is responsible for honoring the watermark on
    /// reads.
    pub(crate) fn advance_low_watermark(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> u64 {
        let mut map = self.low_watermarks.lock().unwrap();
        let cur = map
            .entry((topic.to_string(), partition))
            .or_insert(0);
        if offset > *cur {
            *cur = offset;
        }
        *cur
    }

    /// Lookup-or-spawn the per-`(topic, partition)` Produce writer.
    /// Returns `None` if the topic doesn't exist or the partition is
    /// out of range — callers should surface the matching Kafka error.
    ///
    /// `batch_window` is used only on first spawn; the writer keeps its
    /// configured window for its whole lifetime so two listeners with
    /// different windows would race on first call. In practice we
    /// expect a single Kafka listener per broker, so this is fine.
    pub(crate) fn produce_writer(
        &self,
        topic: &str,
        partition: u32,
        batch_window: Duration,
    ) -> Option<crate::kafka::produce_writer::WriterHandle> {
        let key = (topic.to_string(), partition);
        {
            let map = self.produce_writers.lock().unwrap();
            if let Some(h) = map.get(&key) {
                return Some(h.clone());
            }
        }
        let topic_handle = self.lookup(topic)?;
        // Validate partition exists before spawning so we don't leak
        // writer tasks for nonexistent partitions.
        if partition >= topic_handle.partition_count() {
            return None;
        }
        let mut map = self.produce_writers.lock().unwrap();
        if let Some(h) = map.get(&key) {
            return Some(h.clone());
        }
        // Inline-safe iff the topic has no redb work to do per commit:
        // log_only_storage flips off the message-state table, eventual_commits
        // flips off the fsync. Both together leave only the log append,
        // which is bounded µ-second mmap work — safe to keep on the
        // tokio worker.
        let inline_safe = self.config.eventual_commits && self.config.log_only_storage;
        let handle = crate::kafka::produce_writer::spawn(
            topic.to_string(),
            topic_handle,
            partition,
            batch_window,
            inline_safe,
            self.replicator(),
        );
        map.insert(key, handle.clone());
        Some(handle)
    }

    /// Lookup-or-spawn the coordinator task for a consumer group. Cheap
    /// (HashMap + clone of an mpsc Sender).
    pub(crate) fn group_coordinator(
        &self,
        group_id: &str,
    ) -> crate::kafka::group_coordinator::CoordinatorHandle {
        let mut map = self.group_coordinators.lock().unwrap();
        if let Some(h) = map.get(group_id) {
            return h.clone();
        }
        let handle = crate::kafka::group_coordinator::spawn(group_id.to_string());
        map.insert(group_id.to_string(), handle.clone());
        handle
    }

    /// Snapshot every coordinator handle currently spawned, paired with its
    /// group_id. Used by ListGroups / DescribeGroups to enumerate state.
    pub(crate) fn group_coordinator_snapshot(
        &self,
    ) -> Vec<(String, crate::kafka::group_coordinator::CoordinatorHandle)> {
        self.group_coordinators
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Look up an existing coordinator without spawning one. Returns `None`
    /// for unknown groups (so DescribeGroups can return GROUP_ID_NOT_FOUND
    /// instead of spawning a phantom coordinator).
    pub(crate) fn lookup_group_coordinator(
        &self,
        group_id: &str,
    ) -> Option<crate::kafka::group_coordinator::CoordinatorHandle> {
        self.group_coordinators
            .lock()
            .unwrap()
            .get(group_id)
            .cloned()
    }

    /// Path to a workflow's on-disk redb file. Exposed for inspection in
    /// tests; production callers should go through the workflow handle
    /// returned by `get_or_open_workflow`.
    pub fn workflow_path(&self, name: &str) -> PathBuf {
        self.dir.join("workflows").join(format!("{name}.redb"))
    }

    /// Atomically complete a task and stage one or more topic emits for
    /// delivery. Public Rust API for the workflow → topic story; the gRPC
    /// surface will follow.
    pub fn complete_task_with_emits(
        &self,
        workflow: &str,
        task_id: u64,
        result: impl Into<bytes::Bytes>,
        emits: &[(&str, bytes::Bytes)],
    ) -> Result<Vec<u64>> {
        let engine = self.get_or_open_workflow(workflow)?;
        let mut e = engine.lock().unwrap();
        Ok(e.complete_with_emits(task_id, result, emits)?)
    }

    /// Inspect every record currently sitting in a topic across all
    /// partitions, using the broker's own handle (so the redb file lock
    /// is shared rather than fought over). Best-effort: returns an empty
    /// vec if the topic hasn't been opened yet.
    pub fn topic_records(&self, topic: &str, max_per_partition: usize) -> Result<Vec<Vec<u8>>> {
        let Some(t) = self.lookup(topic) else {
            return Ok(vec![]);
        };
        let pc = t.partition_count();
        let mut out = Vec::new();
        // Use a process-unique scratch group so inspection doesn't
        // advance any real consumer's offset.
        let group = format!("__inspect_{}_{}", std::process::id(), now_ns());
        for p in 0..pc {
            let msgs = t.stream(&group, p, max_per_partition)?;
            out.extend(msgs.into_iter().map(|m| m.payload.to_vec()));
        }
        Ok(out)
    }

    /// Sweep ids `0..upper_bound` and return every payload that exists in
    /// the named workflow. Uses the broker's own Engine handle so it works
    /// while a dispatcher holds the file open. Returns an empty vec if the
    /// workflow hasn't been opened yet.
    pub fn workflow_payloads(&self, name: &str, upper_bound: u64) -> Result<Vec<Vec<u8>>> {
        let arc = match self
            .workflows
            .lock()
            .unwrap()
            .get(name)
            .map(|e| e.engine.clone())
        {
            Some(a) => a,
            None => return Ok(vec![]),
        };
        let engine = arc.lock().unwrap();
        let mut out = Vec::new();
        for id in 0..upper_bound {
            if let Some(t) = engine.get(id)? {
                out.push(t.payload.to_vec());
            }
        }
        Ok(out)
    }

    /// Get a clone of the persistent trigger store, opening it on first use.
    pub(crate) fn trigger_store(&self) -> Result<Arc<crate::triggers::TriggerStore>> {
        let mut guard = self.trigger_store.lock().unwrap();
        if let Some(s) = guard.as_ref() {
            return Ok(s.clone());
        }
        let s = Arc::new(crate::triggers::TriggerStore::open(&self.dir)?);
        *guard = Some(s.clone());
        Ok(s)
    }

    /// Register a new trigger or replace an existing one with the same name.
    /// Spawns a fresh dispatcher; the old one (if any) is aborted via Drop.
    pub fn register_trigger(self: &Arc<Self>, config: crate::triggers::TriggerConfig) -> Result<()> {
        self.trigger_store()?.put(&config)?;
        let handle = crate::triggers::spawn_dispatcher(self.clone(), config.clone());
        let mut triggers = self.triggers.lock().unwrap();
        triggers.insert(config.name.clone(), handle);
        Ok(())
    }

    /// Stop a trigger and remove it from durable storage. Returns true if
    /// the trigger existed.
    pub fn unregister_trigger(&self, name: &str) -> Result<bool> {
        let removed_from_store = self.trigger_store()?.delete(name)?;
        let removed_from_mem = self.triggers.lock().unwrap().remove(name).is_some();
        Ok(removed_from_store || removed_from_mem)
    }

    /// Snapshot of every persistent trigger config (alive or not).
    pub fn list_triggers(&self) -> Result<Vec<crate::triggers::TriggerConfig>> {
        self.trigger_store()?.list()
    }

    /// Re-attach dispatchers for every persisted trigger. Call this once
    /// after `open_with_config` from inside a tokio runtime.
    pub fn resume_triggers(self: &Arc<Self>) -> Result<usize> {
        let configs = self.trigger_store()?.list()?;
        let n = configs.len();
        let mut triggers = self.triggers.lock().unwrap();
        for cfg in configs {
            let handle = crate::triggers::spawn_dispatcher(self.clone(), cfg.clone());
            triggers.insert(cfg.name.clone(), handle);
        }
        Ok(n)
    }

    /// Spawn every background dispatcher the broker needs (outbox, txn
    /// sweeper, and any previously persisted triggers). Call this once
    /// from inside a tokio runtime after `open_with_config`.
    pub fn start_background(self: &Arc<Self>) -> Result<usize> {
        crate::outbox::spawn(self.clone());
        crate::txn_sweeper::spawn(self.clone());
        self.resume_triggers()
    }

    /// Bootstrap this broker's Raft node from [`BrokerConfig::cluster`]
    /// and install the matching `RaftReplicator`. No-op when cluster
    /// mode is disabled.
    ///
    /// Each broker process calls this exactly once at startup. The
    /// broker:
    ///   1. binds a TCP listener on the address that
    ///      [`ClusterConfig::voters`] lists under its `node_id`,
    ///   2. starts a single-node [`qx_raft::GrpcCluster`] view that
    ///      knows every peer's address,
    ///   3. by convention, node 0 initialises cluster membership
    ///      (idempotent on re-bootstrap — duplicate calls are
    ///      coalesced by openraft + our `initialize_membership`),
    ///   4. installs a [`crate::replication::RaftReplicator`] that
    ///      every Produce on this broker routes through.
    pub async fn start_cluster(self: &Arc<Self>) -> Result<()> {
        let Some(cluster_cfg) = self.config.cluster.clone() else {
            return Ok(());
        };
        let my_addr = cluster_cfg
            .voters
            .iter()
            .find(|(id, _)| *id == cluster_cfg.node_id)
            .map(|(_, a)| a.clone())
            .ok_or_else(|| {
                crate::BrokerError::Custom(format!(
                    "node_id {} not present in voters list",
                    cluster_cfg.node_id
                ))
            })?;

        let listener = bind_with_retry(&my_addr)
            .await
            .map_err(crate::BrokerError::Io)?;
        // Build the per-peer URL scheme based on whether raft TLS is
        // enabled. mTLS handshakes require https://; plaintext stays
        // http://.
        let raft_tls = cluster_cfg.raft_tls.as_ref().map(|tls| {
            // Server side: cert + key. The optional client_ca_pem
            // selects mutual TLS — broker accepts only peer brokers
            // whose certs chain back to one of the CAs.
            let server = qx_raft::grpc::RaftServerTls {
                cert_pem: tls.cert_pem.clone(),
                key_pem: tls.key_pem.clone(),
            };
            // Client side: trust the same cert as the CA (self-signed
            // multi-broker deployment, the typical local setup). For
            // a PKI-issued deployment, an operator-supplied CA bundle
            // would go here — qx-raft accepts any PEM-encoded CA.
            let ca_pem = tls.client_ca_pem.clone().unwrap_or_else(|| tls.cert_pem.clone());
            let client = qx_raft::grpc::RaftClientTls {
                ca_pem,
                // SNI: the broker's voter addresses are typically
                // "127.0.0.1:N" in tests; we extract a hostname here.
                // Real deployments override this via the same cert's
                // SubjectAltName matching the host the operator
                // configures.
                domain: my_addr
                    .split(':')
                    .next()
                    .unwrap_or("localhost")
                    .to_string(),
            };
            (server, client)
        });
        let scheme = if raft_tls.is_some() { "https" } else { "http" };
        let mut peer_addrs: std::collections::HashMap<qx_raft::NodeId, String> =
            std::collections::HashMap::new();
        for (id, addr) in &cluster_cfg.voters {
            peer_addrs.insert(*id, format!("{scheme}://{addr}"));
        }

        let pending = crate::replication::fresh_pending_map();
        let resolver: Arc<dyn crate::replication::TopicResolver> =
            Arc::new(BrokerTopicResolver {
                broker: Arc::downgrade(self),
            });
        let applier = crate::replication::RaftReplicator::applier_for_broker(
            cluster_cfg.node_id,
            resolver,
            pending.clone(),
        );

        let raft_dir = self.dir.join("_raft");
        let cluster = qx_raft::GrpcCluster::start_node(
            cluster_cfg.node_id,
            listener,
            peer_addrs,
            raft_dir,
            Some(applier),
            raft_tls,
        )
        .await
        .map_err(|e| crate::BrokerError::Custom(format!("raft start_node: {e}")))?;
        let cluster: Arc<qx_raft::GrpcCluster> = Arc::new(cluster);
        // Stash a typed handle so `trigger_raft_snapshot` (used by
        // the snapshot-install integration test) can drive an
        // openraft snapshot on this node's local Raft instance.
        let _ = self
            .grpc_cluster_for_snapshot
            .set(cluster.clone());

        // Convention: node 0 owns the membership-init step. The
        // `initialize_membership` helper is idempotent — restarting
        // node 0 won't replay the bootstrap.
        if cluster_cfg.node_id == 0 {
            cluster
                .initialize_membership(cluster_cfg.voters.len() as u32)
                .await
                .map_err(|e| {
                    crate::BrokerError::Custom(format!(
                        "initialize_membership on node 0: {e}"
                    ))
                })?;
        }

        let replicator = Arc::new(crate::replication::RaftReplicator::with_pending(
            cluster as Arc<dyn qx_raft::RaftCluster>,
            cluster_cfg.node_id,
            pending,
        ));
        self.install_replicator(replicator).map_err(|e| {
            crate::BrokerError::Custom(format!("install_replicator: {e}"))
        })?;
        Ok(())
    }
}

/// Bridge between [`crate::replication::TopicResolver`] and the broker.
/// Holds a `Weak<Broker>` to break the cycle: broker → cluster →
/// applier → broker.
struct BrokerTopicResolver {
    broker: std::sync::Weak<Broker>,
}

impl crate::replication::TopicResolver for BrokerTopicResolver {
    fn lookup(&self, name: &str) -> Option<Arc<Topic>> {
        self.broker.upgrade().and_then(|b| b.lookup(name))
    }

    fn create_topic(
        &self,
        name: &str,
        partition_count: u32,
    ) -> std::result::Result<(), String> {
        match self.broker.upgrade() {
            Some(b) => b
                .get_or_open(name, partition_count)
                .map(|_| ())
                .map_err(|e| e.to_string()),
            None => Err("broker dropped".into()),
        }
    }
}

struct BrokerSvc {
    broker: Arc<Broker>,
}

impl BrokerSvc {
    #[allow(clippy::result_large_err)]
    fn lookup_or_not_found(&self, topic: &str) -> std::result::Result<Arc<Topic>, Status> {
        self.broker
            .lookup(topic)
            .ok_or_else(|| Status::not_found(format!("topic '{topic}' not found")))
    }
}

#[tonic::async_trait]
impl BrokerSvcTrait for BrokerSvc {
    async fn open_topic(
        &self,
        req: Request<proto::OpenTopicRequest>,
    ) -> std::result::Result<Response<proto::OpenTopicResponse>, Status> {
        let req = req.into_inner();
        // Routes through the Replicator in cluster mode, so every
        // broker materialises the topic via Raft apply. Falls back to
        // a local redb open in single-node mode. The `redb` work
        // happens inside `open_topic_async` (it spawn_blocks under
        // the hood through `get_or_open` when single-node, and
        // through `topic.send_batch_with_cursor` on the apply path
        // when cluster).
        let count = self
            .broker
            .open_topic_async(&req.name, req.partition_count)
            .await
            .map_err(|e| match e {
                crate::error::BrokerError::Topic(te) => topic_err_to_status(te),
                other => Status::internal(other.to_string()),
            })?;
        Ok(Response::new(proto::OpenTopicResponse {
            partition_count: count,
        }))
    }

    async fn send(
        &self,
        req: Request<proto::SendRequest>,
    ) -> std::result::Result<Response<proto::SendResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        // `replicate_or_forward_send` routes through the local
        // replicator first; on `ForwardToLeader` it transparently
        // proxies to the leader broker's client-facing gRPC if
        // `ClusterConfig::client_addrs` was set. In single-node mode
        // the NoopReplicator path is identical to the pre-cluster
        // behaviour.
        // Hand the prost-generated `Bytes` straight through — the
        // forwarding path (if any) will refcount-share them with
        // tonic's encoder rather than realloc per record.
        let (partition, record_id) = self
            .broker
            .replicate_or_forward_send(&req.topic, topic_mu, req.key, req.payload)
            .await
            .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::SendResponse {
            partition,
            record_id,
        }))
    }

    async fn send_batch(
        &self,
        req: Request<proto::SendBatchRequest>,
    ) -> std::result::Result<Response<proto::SendBatchResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        // Routes through the per-partition replicator with transparent
        // leader forwarding on `ForwardToLeader`. The result vector
        // preserves the caller's input order.
        let items: Vec<(bytes::Bytes, bytes::Bytes)> = req
            .items
            .into_iter()
            .map(|i| (i.key, i.payload))
            .collect();
        let outcomes = self
            .broker
            .replicate_or_forward_send_batch(&req.topic, topic_mu, items)
            .await
            .map_err(topic_err_to_status)?;
        let results = outcomes
            .into_iter()
            .map(|(p, id)| proto::SendResponse {
                partition: p,
                record_id: id,
            })
            .collect();
        Ok(Response::new(proto::SendBatchResponse { results }))
    }

    type ReceiveStreamStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::TopicMessage, Status>> + Send + 'static>,
    >;

    async fn receive_stream(
        &self,
        req: Request<proto::ReceiveStreamRequest>,
    ) -> std::result::Result<Response<Self::ReceiveStreamStream>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let max_batch = (req.max_batch.max(1) as usize).min(1024);
        let poll = if req.poll_interval_ms == 0 {
            Duration::from_millis(50)
        } else {
            Duration::from_millis(req.poll_interval_ms)
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<proto::TopicMessage, Status>>(64);
        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    return;
                }
                let topic_mu = topic_mu.clone();
                let result = tokio::task::spawn_blocking(move || {
                    topic_mu.receive(max_batch)
                })
                .await;
                match result {
                    Ok(Ok(messages)) if messages.is_empty() => {
                        tokio::time::sleep(poll).await;
                    }
                    Ok(Ok(messages)) => {
                        for msg in messages {
                            if tx.send(Ok(proto_message(msg))).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(topic_err_to_status(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("blocking: {e}")))).await;
                        return;
                    }
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::ReceiveStreamStream))
    }

    async fn receive(
        &self,
        req: Request<proto::ReceiveRequest>,
    ) -> std::result::Result<Response<proto::ReceiveResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let max = req.max as usize;
        let messages = tokio::task::spawn_blocking(move || {
            topic_mu.receive(max)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::ReceiveResponse {
            messages: messages.into_iter().map(proto_message).collect(),
        }))
    }

    async fn ack(
        &self,
        req: Request<proto::AckRequest>,
    ) -> std::result::Result<Response<proto::AckResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let receipt = req
            .receipt
            .ok_or_else(|| Status::invalid_argument("missing receipt"))?;
        let r = from_proto_receipt(receipt);
        tokio::task::spawn_blocking(move || {
            topic_mu.ack(r)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::AckResponse {}))
    }

    async fn nack(
        &self,
        req: Request<proto::NackRequest>,
    ) -> std::result::Result<Response<proto::NackResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let receipt = req
            .receipt
            .ok_or_else(|| Status::invalid_argument("missing receipt"))?;
        let r = from_proto_receipt(receipt);
        tokio::task::spawn_blocking(move || {
            topic_mu.nack(r)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::NackResponse {}))
    }

    async fn extend_visibility(
        &self,
        req: Request<proto::ExtendVisibilityRequest>,
    ) -> std::result::Result<Response<proto::ExtendVisibilityResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let receipt = req
            .receipt
            .ok_or_else(|| Status::invalid_argument("missing receipt"))?;
        let r = from_proto_receipt(receipt);
        let ext = Duration::from_millis(req.extension_ms);
        tokio::task::spawn_blocking(move || {
            topic_mu.extend_visibility(r, ext)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::ExtendVisibilityResponse {}))
    }

    async fn join_group(
        &self,
        req: Request<proto::JoinGroupRequest>,
    ) -> std::result::Result<Response<proto::JoinGroupResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let (member_id, generation, assignment) = tokio::task::spawn_blocking(move || {
            let g = broker.get_or_create_group(&req.topic, &req.group)?;
            let (id, generation, assignment) = g.lock().unwrap().join();
            Ok::<_, crate::error::BrokerError>((id, generation, assignment))
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::JoinGroupResponse {
            member_id,
            generation,
            assignment,
        }))
    }

    async fn heartbeat(
        &self,
        req: Request<proto::HeartbeatRequest>,
    ) -> std::result::Result<Response<proto::HeartbeatResponse>, Status> {
        let req = req.into_inner();
        let group = self.broker.lookup_group(&req.topic, &req.group).ok_or_else(|| {
            Status::not_found(format!("group '{}' for topic '{}'", req.group, req.topic))
        })?;
        let (generation, assignment) = tokio::task::spawn_blocking(move || {
            let mut g = group.lock().unwrap();
            g.heartbeat(req.member_id)
        })
        .await
        .map_err(join_err)?
        .ok_or_else(|| Status::not_found("unknown member id"))?;
        Ok(Response::new(proto::HeartbeatResponse {
            generation,
            assignment,
        }))
    }

    async fn leave_group(
        &self,
        req: Request<proto::LeaveGroupRequest>,
    ) -> std::result::Result<Response<proto::LeaveGroupResponse>, Status> {
        let req = req.into_inner();
        if let Some(group) = self.broker.lookup_group(&req.topic, &req.group) {
            tokio::task::spawn_blocking(move || {
                group.lock().unwrap().leave(req.member_id);
            })
            .await
            .map_err(join_err)?;
        }
        Ok(Response::new(proto::LeaveGroupResponse {}))
    }

    async fn stream_for_member(
        &self,
        req: Request<proto::StreamForMemberRequest>,
    ) -> std::result::Result<Response<proto::StreamFromGroupResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let group = self
            .broker
            .lookup_group(&req.topic, &req.group)
            .ok_or_else(|| Status::not_found("group not joined"))?;
        let assignment = group
            .lock()
            .unwrap()
            .assignment(req.member_id)
            .ok_or_else(|| Status::not_found("unknown member id"))?;
        let max = req.max as usize;
        let group_name = req.group.clone();
        let messages = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            // Fair across assigned partitions: ceil(max / n).
            let per_partition = if assignment.is_empty() {
                0
            } else {
                max.div_ceil(assignment.len())
            };
            for partition in &assignment {
                if out.len() >= max {
                    break;
                }
                let take = (max - out.len()).min(per_partition);
                let msgs = topic_mu.stream(&group_name, *partition, take)?;
                for m in msgs {
                    out.push(m);
                }
            }
            Ok::<_, qx_topic::TopicError>(out)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::StreamFromGroupResponse {
            messages: messages
                .into_iter()
                .map(|m| proto::StreamedRecord {
                    partition: m.partition,
                    id: m.id,
                    payload: m.payload,
                })
                .collect(),
        }))
    }

    async fn stream_from_group(
        &self,
        req: Request<proto::StreamFromGroupRequest>,
    ) -> std::result::Result<Response<proto::StreamFromGroupResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let partition = req.partition;
        let max = req.max as usize;
        let group = req.group;
        let messages = tokio::task::spawn_blocking(move || {
            topic_mu.stream(&group, partition, max)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::StreamFromGroupResponse {
            messages: messages
                .into_iter()
                .map(|m| proto::StreamedRecord {
                    partition: m.partition,
                    id: m.id,
                    payload: m.payload,
                })
                .collect(),
        }))
    }

    async fn commit_group_offset(
        &self,
        req: Request<proto::CommitGroupOffsetRequest>,
    ) -> std::result::Result<Response<proto::CommitGroupOffsetResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let partition = req.partition;
        let offset = req.offset;
        let group = req.group;
        tokio::task::spawn_blocking(move || {
            topic_mu.commit_offset(&group, partition, offset)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::CommitGroupOffsetResponse {}))
    }

    async fn get_group_offset(
        &self,
        req: Request<proto::GetGroupOffsetRequest>,
    ) -> std::result::Result<Response<proto::GetGroupOffsetResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let partition = req.partition;
        let group = req.group;
        let offset = tokio::task::spawn_blocking(move || {
            topic_mu.group_offset(&group, partition)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::GetGroupOffsetResponse { offset }))
    }

    async fn create_root_task(
        &self,
        req: Request<proto::CreateRootTaskRequest>,
    ) -> std::result::Result<Response<proto::CreateTaskResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let task_id = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            Ok::<u64, crate::error::BrokerError>(e.create_root(&req.payload)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::CreateTaskResponse { task_id }))
    }

    async fn create_child_task(
        &self,
        req: Request<proto::CreateChildTaskRequest>,
    ) -> std::result::Result<Response<proto::CreateTaskResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let task_id = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            Ok::<u64, crate::error::BrokerError>(e.create_child(req.parent_id, &req.payload)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::CreateTaskResponse { task_id }))
    }

    async fn get_task(
        &self,
        req: Request<proto::GetTaskRequest>,
    ) -> std::result::Result<Response<proto::TaskMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let task = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let e = wf.lock().unwrap();
            Ok::<Option<Task>, crate::error::BrokerError>(e.get(req.task_id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?
        .ok_or_else(|| Status::not_found(format!("task {}", req.task_id)))?;
        Ok(Response::new(task_to_proto(task)))
    }

    async fn list_children(
        &self,
        req: Request<proto::ListChildrenRequest>,
    ) -> std::result::Result<Response<proto::ListChildrenResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let kids = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let e = wf.lock().unwrap();
            Ok::<Vec<u64>, crate::error::BrokerError>(e.children(req.parent_id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListChildrenResponse { child_ids: kids }))
    }

    async fn mark_task_running(
        &self,
        req: Request<proto::TaskOpRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            e.mark_running(req.task_id)?;
            Ok::<(), crate::error::BrokerError>(())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn complete_task(
        &self,
        req: Request<proto::CompleteTaskRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            e.complete(req.task_id, req.result)?;
            Ok::<(), crate::error::BrokerError>(())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn fail_task(
        &self,
        req: Request<proto::FailTaskRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            e.fail(req.task_id, req.error)?;
            Ok::<(), crate::error::BrokerError>(())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn cancel_task(
        &self,
        req: Request<proto::TaskOpRequest>,
    ) -> std::result::Result<Response<proto::CancelTaskResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let cancelled = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            Ok::<usize, crate::error::BrokerError>(e.cancel(req.task_id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::CancelTaskResponse {
            cancelled: cancelled as u64,
        }))
    }

    async fn register_schema(
        &self,
        req: Request<proto::RegisterSchemaRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.register(&req.subject, &req.bytes)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn get_schema_by_id(
        &self,
        req: Request<proto::GetSchemaByIdRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.get_by_id(req.id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn get_schema_version(
        &self,
        req: Request<proto::GetSchemaVersionRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.get_version(&req.subject, req.version)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn get_latest_schema(
        &self,
        req: Request<proto::GetLatestSchemaRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.get_latest(&req.subject)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn list_schema_versions(
        &self,
        req: Request<proto::ListSchemaVersionsRequest>,
    ) -> std::result::Result<Response<proto::ListSchemaVersionsResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let versions = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.list_versions(&req.subject)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListSchemaVersionsResponse { versions }))
    }

    async fn register_trigger(
        &self,
        req: Request<proto::TriggerSpec>,
    ) -> std::result::Result<Response<proto::RegisterTriggerResponse>, Status> {
        let req = req.into_inner();
        let mut cfg = crate::triggers::TriggerConfig::new(&req.name, &req.topic, &req.workflow);
        if req.batch_size > 0 {
            cfg.batch_size = req.batch_size;
        }
        if req.poll_interval_ms > 0 {
            cfg.poll_interval_ms = req.poll_interval_ms;
        }
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || broker.register_trigger(cfg))
            .await
            .map_err(join_err)?
            .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::RegisterTriggerResponse {}))
    }

    async fn unregister_trigger(
        &self,
        req: Request<proto::UnregisterTriggerRequest>,
    ) -> std::result::Result<Response<proto::UnregisterTriggerResponse>, Status> {
        let name = req.into_inner().name;
        let broker = self.broker.clone();
        let removed = tokio::task::spawn_blocking(move || broker.unregister_trigger(&name))
            .await
            .map_err(join_err)?
            .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::UnregisterTriggerResponse { removed }))
    }

    async fn list_triggers(
        &self,
        _req: Request<proto::ListTriggersRequest>,
    ) -> std::result::Result<Response<proto::ListTriggersResponse>, Status> {
        let broker = self.broker.clone();
        let triggers = tokio::task::spawn_blocking(move || broker.list_triggers())
            .await
            .map_err(join_err)?
            .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListTriggersResponse {
            triggers: triggers
                .into_iter()
                .map(|c| proto::TriggerSpec {
                    name: c.name,
                    topic: c.topic,
                    workflow: c.workflow,
                    batch_size: c.batch_size,
                    poll_interval_ms: c.poll_interval_ms,
                })
                .collect(),
        }))
    }

    async fn complete_task_with_emits(
        &self,
        req: Request<proto::CompleteTaskWithEmitsRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            // Borrow `&str` from `req.emits[i].topic` and clone the
            // refcounted `Bytes` payload. The clone is a refcount
            // bump (not a memcpy); the engine then moves the same
            // Bytes into `PendingEmit::payload` without re-allocating.
            let emits: Vec<(&str, bytes::Bytes)> = req
                .emits
                .iter()
                .map(|e| (e.topic.as_ref(), e.payload.clone()))
                .collect();
            broker
                .complete_task_with_emits(&req.workflow, req.task_id, req.result, &emits)
                .map(|_| ())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn list_subjects(
        &self,
        _req: Request<proto::ListSubjectsRequest>,
    ) -> std::result::Result<Response<proto::ListSubjectsResponse>, Status> {
        let broker = self.broker.clone();
        let subjects = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.list_subjects()?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListSubjectsResponse { subjects }))
    }

    async fn stats(
        &self,
        req: Request<proto::StatsRequest>,
    ) -> std::result::Result<Response<proto::StatsResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let stats = tokio::task::spawn_blocking(move || {
            topic_mu.stats()
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::StatsResponse {
            partition_count: stats.partition_count,
            pending: stats.pending,
            inflight: stats.inflight,
            dead: stats.dead,
        }))
    }
}

fn proto_message(m: qx_topic::TopicMessage) -> proto::TopicMessage {
    proto::TopicMessage {
        partition: m.partition,
        id: m.id,
        receipt: Some(proto::Receipt {
            partition: m.receipt.partition,
            record_id: m.receipt.inner.record_id,
            attempt: m.receipt.inner.attempt,
        }),
        attempts: m.attempts,
        // qx-storage's `Segment::read` already returns `Bytes`, so the
        // qx-queue → qx-topic → broker path holds a single refcounted
        // slice end-to-end. Prost encodes it onto the wire without a
        // second copy.
        payload: m.payload,
    }
}

fn from_proto_receipt(r: proto::Receipt) -> TopicReceipt {
    TopicReceipt {
        partition: r.partition,
        inner: ReceiptHandle {
            record_id: r.record_id,
            attempt: r.attempt,
        },
    }
}

fn topic_err_to_status(e: TopicError) -> Status {
    use TopicError::*;
    match &e {
        InvalidPartition { .. } | ZeroPartitions | PartitionCountMismatch { .. } => {
            Status::invalid_argument(e.to_string())
        }
        Other(msg) if is_forward_to_leader(msg) => {
            // openraft surfaces ForwardToLeader as
            // "...forward request to: Some(<leader_id>), ..." in the
            // error string. Translate to a typed gRPC status so
            // clients don't have to grep error strings — leader id
            // goes into the trailing metadata key `x-qx-leader-id`
            // for retry routing.
            let mut status = Status::failed_precondition(format!("not leader: {msg}"));
            if let Some(leader_id) = parse_forward_to_leader(msg)
                && let Ok(v) = leader_id.to_string().parse()
            {
                status.metadata_mut().insert("x-qx-leader-id", v);
            }
            status
        }
        _ => Status::internal(e.to_string()),
    }
}

/// Extract the `leader_id` from openraft's `ForwardToLeader` error
/// stringification. Format observed at openraft 0.9:
///   `...forward request to: Some(<id>), Some(BasicNode { addr: "..." })`
fn parse_forward_to_leader(s: &str) -> Option<u64> {
    let idx = s.find("forward request to: Some(")?;
    let rest = &s[idx + "forward request to: Some(".len()..];
    let end = rest.find(')')?;
    rest[..end].trim().parse::<u64>().ok()
}

/// `true` if `s` is openraft's `ForwardToLeader` error stringification.
/// openraft 0.9's `Display` doesn't actually emit the literal token
/// `ForwardToLeader` — it says "has to forward request to". Match on
/// the substring that's stable across the two cases.
fn is_forward_to_leader(s: &str) -> bool {
    s.contains("forward request to") || s.contains("ForwardToLeader")
}

fn broker_err_to_status(e: crate::error::BrokerError) -> Status {
    use crate::error::BrokerError;
    match e {
        BrokerError::Topic(te) => topic_err_to_status(te),
        BrokerError::Workflow(WorkflowError::TaskNotFound { id }) => {
            Status::not_found(format!("task {id}"))
        }
        BrokerError::Workflow(WorkflowError::TerminalTask { id }) => {
            Status::failed_precondition(format!("task {id} is terminal"))
        }
        BrokerError::SchemaRegistry(RegistryError::SubjectNotFound(s)) => {
            Status::not_found(format!("subject {s}"))
        }
        BrokerError::SchemaRegistry(RegistryError::SchemaIdNotFound(id)) => {
            Status::not_found(format!("schema id {id}"))
        }
        BrokerError::SchemaRegistry(RegistryError::VersionNotFound { subject, version }) => {
            Status::not_found(format!("subject {subject} version {version}"))
        }
        other => Status::internal(other.to_string()),
    }
}

fn schema_to_proto(s: Schema) -> proto::SchemaMessage {
    proto::SchemaMessage {
        id: s.id,
        subject: s.subject,
        version: s.version,
        bytes: s.bytes,
    }
}

fn task_to_proto(t: Task) -> proto::TaskMessage {
    proto::TaskMessage {
        id: t.id,
        parent_id: t.parent_id,
        status: match t.status {
            TaskStatus::Pending => 0,
            TaskStatus::Running => 1,
            TaskStatus::Done => 2,
            TaskStatus::Failed => 3,
            TaskStatus::Cancelled => 4,
        },
        created_nanos: t.created_nanos,
        payload: t.payload,
        result: t.result.unwrap_or_default(),
        // proto::TaskMessage.error is now `bytes::Bytes` thanks to the
        // build.rs codegen tweak — refcount-shared straight through
        // from qx-workflow's `Task::error` to the wire. UTF-8 is
        // validated once at engine decode time so consumers can
        // `String::from_utf8` on the bytes safely.
        error: t.error.unwrap_or_default(),
    }
}

fn join_err(e: tokio::task::JoinError) -> Status {
    Status::internal(format!("blocking task join: {e}"))
}

/// Bind a TCP listener, retrying on `AddrInUse` for up to ~5 s. The
/// integration tests reserve a port, drop it, then spawn the broker
/// expecting the broker to rebind. Under workspace-parallel `cargo
/// test`, another test may grab the port in the gap. A short retry
/// loop closes that race without affecting normal startup latency on
/// an actually-occupied port.
///
/// Exposed publicly so `qxd` (and any other binary built on top of
/// `qx-broker`) can share the same retry logic at every bind site
/// instead of duplicating it.
pub async fn bind_with_retry(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    use std::time::Duration as StdDur;
    let mut backoff = StdDur::from_millis(20);
    let deadline = tokio::time::Instant::now() + StdDur::from_secs(5);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => return Ok(l),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse
                && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(StdDur::from_millis(250));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Combined keyx + local-token check. The local token is the
/// fallback when keyx is unreachable so a network partition doesn't
/// lock the operator out of their own broker; keyx still has final
/// say on authoritative rejections (`Rejected` → 401 regardless of
/// what local says).
///
/// Order of operations:
///   1. No auth configured at all → allow.
///   2. Extract bearer from `authorization`. Absent → 401.
///   3. If keyx is configured: run the cached validator. Cache hit
///      or fresh `validate()` returning `Ok(_)` → allow. `Rejected`
///      → 401 even if the local token would accept. `Http` /
///      `Transport` (transient) → fall through to step 4.
///   4. If a local token is configured: string-eq against the bearer.
///      Match → allow. Mismatch → 401.
#[allow(clippy::result_large_err)]
fn check_auth_combined<T>(
    local: Option<&str>,
    keyx: Option<&Arc<crate::auth_keyx::KeyxValidator>>,
    req: &tonic::Request<T>,
) -> std::result::Result<(), Status> {
    if local.is_none() && keyx.is_none() {
        return Ok(());
    }
    let header = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
    let bearer = header.strip_prefix("Bearer ").unwrap_or(header);

    if let Some(validator) = keyx {
        match validate_via_keyx_sync(validator, bearer) {
            Some(Ok(_verdict)) => return Ok(()),
            Some(Err(crate::auth_keyx::KeyxError::Rejected(reason))) => {
                return Err(Status::unauthenticated(format!("keyx: {reason}")));
            }
            Some(Err(e)) => {
                // Http / Transport / Malformed — transient.
                eprintln!("qx: keyx unavailable, falling back to local auth: {e}");
            }
            None => {
                // Tokio runtime not present (e.g. unit test calling
                // this outside an async context). Treat as
                // unavailable; fall back to local.
            }
        }
    }

    if let Some(expected) = local {
        if bearer == expected {
            return Ok(());
        }
        return Err(Status::unauthenticated("invalid token"));
    }
    // keyx was set but only returned transient errors AND no local
    // token is configured — refuse rather than silently allowing.
    Err(Status::unauthenticated("auth backend unavailable"))
}

/// Synchronous wrapper around `KeyxValidator::validate` for use
/// inside tonic's sync interceptor. Cache hits are pure CPU; cache
/// misses use `tokio::task::block_in_place` so the runtime can
/// migrate other tasks off this worker for the duration of the
/// keyx round-trip. Returns `None` when there's no tokio handle
/// reachable from this thread (only happens in unit tests).
fn validate_via_keyx_sync(
    validator: &Arc<crate::auth_keyx::KeyxValidator>,
    bearer: &str,
) -> Option<std::result::Result<crate::auth_keyx::Verdict, crate::auth_keyx::KeyxError>> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let v = validator.clone();
    let key = bearer.to_string();
    Some(tokio::task::block_in_place(|| {
        handle.block_on(async move { v.validate(&key).await })
    }))
}

async fn handle_metrics_connection(
    broker: Arc<Broker>,
    mut stream: tokio::net::TcpStream,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // One-shot read of the request head. The dashboard polling
    // pattern is short GETs — the entire request fits well under 4
    // KiB. Pipelined requests aren't supported (the handler closes
    // the connection after one response).
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    let head = std::str::from_utf8(&buf[..n]).unwrap_or("");

    // Parse: METHOD SP PATH SP VERSION  (we only care about method + path).
    let mut iter = head.split_whitespace();
    let method = iter.next().unwrap_or("");
    let path = iter.next().unwrap_or("");

    // CORS preflight: blanket permissive. Needed for `npm run dev`
    // against this broker; browsers loading the embedded dashboard
    // skip preflight because every request is same-origin.
    if method == "OPTIONS" {
        let response = b"HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, OPTIONS\r\nAccess-Control-Allow-Headers: content-type\r\nAccess-Control-Max-Age: 600\r\nConnection: close\r\n\r\n";
        stream.write_all(response).await?;
        stream.shutdown().await?;
        return Ok(());
    }

    // Route. Order matters: `/metrics` and `/api/*` must be checked
    // before the SPA fallback so the dashboard shell can't swallow
    // API requests.
    let (status, content_type, body): (&'static str, &'static str, Vec<u8>) =
        if path == "/metrics" {
            let s = match broker.all_stats() {
                Ok(stats) => format_metrics(&stats),
                Err(e) => format!("# error: {e}\n"),
            };
            ("200 OK", "text/plain; version=0.0.4; charset=utf-8", s.into_bytes())
        } else if let Some(json) = crate::admin_api::render_route(path, &broker).await {
            ("200 OK", "application/json", json.into_bytes())
        } else if let Some((ct, bytes)) = crate::dashboard_assets::render_path(path) {
            // Hashed assets + index.html + SPA fallback. Bytes
            // borrowed from the compile-time embed; copy into the
            // response so the stream owns its buffer.
            ("200 OK", ct, bytes.to_vec())
        } else if path == "/" && crate::dashboard_assets::is_empty() {
            // Operator built qxd without dropping the dashboard
            // dist into `static/dashboard/`. Keep the legacy
            // "Prometheus at root" behaviour so existing scrapers
            // pointed at `/` keep working.
            let s = match broker.all_stats() {
                Ok(stats) => format_metrics(&stats),
                Err(e) => format!("# error: {e}\n"),
            };
            ("200 OK", "text/plain; version=0.0.4; charset=utf-8", s.into_bytes())
        } else {
            (
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found\n".to_vec(),
            )
        };

    // Send the head as text, then the body bytes. Binary-clean —
    // fonts and any other non-UTF8 asset flow through untouched.
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    Ok(())
}

pub(crate) fn format_metrics(stats: &[(String, qx_topic::TopicStats)]) -> String {
    let mut out = String::new();
    out.push_str("# HELP qx_topic_pending Messages awaiting delivery.\n");
    out.push_str("# TYPE qx_topic_pending gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_pending{{topic={}}} {}\n",
            escape_label(name),
            s.pending
        ));
    }
    out.push_str("# HELP qx_topic_inflight Messages claimed by a consumer, not yet acked.\n");
    out.push_str("# TYPE qx_topic_inflight gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_inflight{{topic={}}} {}\n",
            escape_label(name),
            s.inflight
        ));
    }
    out.push_str("# HELP qx_topic_dead Messages in the dead-letter state.\n");
    out.push_str("# TYPE qx_topic_dead gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_dead{{topic={}}} {}\n",
            escape_label(name),
            s.dead
        ));
    }
    out.push_str("# HELP qx_topic_partitions Configured partition count.\n");
    out.push_str("# TYPE qx_topic_partitions gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_partitions{{topic={}}} {}\n",
            escape_label(name),
            s.partition_count
        ));
    }
    out
}

/// Resolves when the broker's shutdown watch flips to true. Used to drive
/// `serve_with_incoming_shutdown` and similar wait points.
pub(crate) async fn wait_for_shutdown(mut rx: tokio::sync::watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}

/// Install the `ring` CryptoProvider once per process. Idempotent: a
/// second call (e.g. from `qx-raft`) is a no-op via `Once`. Required by
/// rustls 0.23 before any TLS server or client setup.
fn install_default_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

/// Build a rustls `ServerConfig` from PEM cert + key (+ optional
/// client-CA bundle for mTLS). Shared by the gRPC listener (which
/// holds a live `ArcSwap<ServerConfig>` for cert rotation) and the
/// Kafka wire listener (built once at startup). Returns
/// `BrokerError::Io(InvalidInput)` for malformed PEM input rather
/// than panicking — the caller (qxd's SIGHUP loop) needs to log a
/// rejection and keep the previous config in place.
pub fn build_tls_server_config(
    tls: &TlsConfig,
) -> Result<tokio_rustls::rustls::ServerConfig> {
    use std::io::Cursor;

    install_default_crypto_provider();

    let mut cert_reader = Cursor::new(&tls.cert_pem);
    let certs: Vec<rustls_pemfile::Item> = rustls_pemfile::read_all(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            crate::error::BrokerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("parse cert pem: {e}"),
            ))
        })?;
    let cert_chain: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>> =
        certs
            .into_iter()
            .filter_map(|item| match item {
                rustls_pemfile::Item::X509Certificate(c) => Some(c),
                _ => None,
            })
            .collect();
    if cert_chain.is_empty() {
        return Err(crate::error::BrokerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no X.509 certificates found in cert_pem",
        )));
    }

    let mut key_reader = Cursor::new(&tls.key_pem);
    let key_item = rustls_pemfile::read_one(&mut key_reader)
        .map_err(|e| {
            crate::error::BrokerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("parse key pem: {e}"),
            ))
        })?
        .ok_or_else(|| {
            crate::error::BrokerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no private key in key_pem",
            ))
        })?;
    let key: tokio_rustls::rustls::pki_types::PrivateKeyDer<'static> = match key_item {
        rustls_pemfile::Item::Pkcs8Key(k) => k.into(),
        rustls_pemfile::Item::Pkcs1Key(k) => k.into(),
        rustls_pemfile::Item::Sec1Key(k) => k.into(),
        _ => {
            return Err(crate::error::BrokerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "key_pem must be PKCS#8, PKCS#1, or SEC1",
            )));
        }
    };

    let builder = tokio_rustls::rustls::ServerConfig::builder();
    let cfg = if let Some(ca_pem) = &tls.client_ca_pem {
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        let mut ca_cursor = Cursor::new(ca_pem);
        for item in rustls_pemfile::certs(&mut ca_cursor) {
            let cert = item.map_err(|e| {
                crate::error::BrokerError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("parse client_ca_pem: {e}"),
                ))
            })?;
            roots.add(cert).map_err(|e| {
                crate::error::BrokerError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("add client CA: {e}"),
                ))
            })?;
        }
        if roots.is_empty() {
            return Err(crate::error::BrokerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "client_ca_pem contained no certificates",
            )));
        }
        let verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder(
            Arc::new(roots),
        )
        .build()
        .map_err(|e| {
            crate::error::BrokerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("client verifier: {e}"),
            ))
        })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, key)
            .map_err(|e| {
                crate::error::BrokerError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("rustls server config (mtls): {e}"),
                ))
            })?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| {
                crate::error::BrokerError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("rustls server config: {e}"),
                ))
            })?
    };
    Ok(cfg)
}

/// Wrapper around `tokio_rustls::server::TlsStream<TcpStream>` that
/// implements tonic's [`Connected`] trait so the per-RPC connect-info
/// (peer addr, local addr) flows through our custom TLS-terminating
/// acceptor into tonic's request extensions. tonic doesn't ship a
/// `Connected` impl for `TlsStream` directly because the inner stream
/// type is generic; we only need this for `TcpStream`.
pub struct TlsTcpStream(pub tokio_rustls::server::TlsStream<tokio::net::TcpStream>);

impl tonic::transport::server::Connected for TlsTcpStream {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        let (inner, _) = self.0.get_ref();
        tonic::transport::server::TcpConnectInfo {
            remote_addr: inner.peer_addr().ok(),
            local_addr: inner.local_addr().ok(),
        }
    }
}

impl tokio::io::AsyncRead for TlsTcpStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TlsTcpStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}
