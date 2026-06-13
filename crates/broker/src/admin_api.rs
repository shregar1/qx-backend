//! Read-only HTTP/JSON introspection surface for the qx-dashboard.
//!
//! Mounted on the same TCP listener that serves Prometheus
//! `/metrics`. Routes:
//!
//!   GET /api/health    → `{ "status": "ok", "uptimeSeconds": u64 }`
//!   GET /api/nodes     → `[NodeStat]`
//!   GET /api/topics    → `[TopicStat]` real pending/inflight + live msgs/byte rates
//!   GET /api/groups    → `[GroupStat]` snapshots from gRPC + Kafka coordinators
//!   GET /api/workflows → `[WorkflowStat]` aggregate task counts per engine
//!   GET /api/events    → `[EventLine]` (placeholder: always empty)
//!
//! Schemas mirror the zod schemas in `qx-dashboard/src/schemas/cluster.ts`
//! exactly — field names are camelCase to match what the UI validates.
//!
//! Every response carries `Access-Control-Allow-Origin: *` so the dev
//! dashboard can fetch from a different origin (Vite at :5173) without
//! a CORS proxy.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::server::Broker;

#[derive(Serialize, Clone, Debug)]
pub struct NodeStat {
    pub id: u64,
    pub addr: String,
    pub role: &'static str,
    pub status: &'static str,
    pub term: u64,
    #[serde(rename = "commitIndex")]
    pub commit_index: u64,
    #[serde(rename = "lastApplied")]
    pub last_applied: u64,
    #[serde(rename = "uptimeSeconds")]
    pub uptime_seconds: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct TopicStat {
    pub name: String,
    pub partitions: u32,
    pub pending: u64,
    pub inflight: u64,
    pub dead: u64,
    #[serde(rename = "retentionMode")]
    pub retention_mode: &'static str,
    #[serde(rename = "bytesPerSec")]
    pub bytes_per_sec: f64,
    #[serde(rename = "msgsPerSec")]
    pub msgs_per_sec: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct GroupStat {
    pub name: String,
    pub topic: String,
    pub members: u32,
    pub lag: u64,
    pub status: &'static str,
    pub generation: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct WorkflowStat {
    pub name: String,
    pub tasks: u64,
    pub running: u64,
    pub done: u64,
    pub failed: u64,
    pub cancelled: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct EventLine {
    pub id: String,
    pub ts: u64,
    pub level: &'static str,
    pub source: String,
    pub message: String,
}

/// Compile-time-known event severities. Stored as `&'static str` on
/// the wire so each event line costs no per-level allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLevel {
    Info,
    Warn,
    Error,
}

impl EventLevel {
    fn as_str(self) -> &'static str {
        match self {
            EventLevel::Info => "info",
            EventLevel::Warn => "warn",
            EventLevel::Error => "error",
        }
    }
}

/// Bounded ring buffer. Each event gets a unique monotonic id so the
/// dashboard can de-dupe across overlapping polls. Backed by
/// `VecDeque` — push_back + pop_front are both O(1).
pub struct EventRing {
    capacity: usize,
    next_id: AtomicU64,
    inner: Mutex<VecDeque<EventLine>>,
}

impl EventRing {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            next_id: AtomicU64::new(0),
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn push(&self, level: EventLevel, source: &str, message: String) {
        // Monotonic timestamp (ms) for the wire. UI sorts/groups by
        // this — SystemTime is fine because dashboard polling lives
        // on the order of seconds, not nanoseconds.
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let line = EventLine {
            id: format!("evt-{id}"),
            ts,
            level: level.as_str(),
            source: source.to_string(),
            message,
        };
        let mut buf = self.inner.lock().unwrap();
        if buf.len() == self.capacity {
            buf.pop_front();
        }
        buf.push_back(line);
    }

    pub fn snapshot(&self) -> Vec<EventLine> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }
}

/// Atomic counters bumped from the Produce path. The admin API
/// converts cumulative totals into per-second rates by sampling
/// deltas between successive queries — see `RateCache`.
#[derive(Default, Debug)]
pub struct TopicMetricCounters {
    pub total_msgs: AtomicU64,
    pub total_bytes: AtomicU64,
}

impl TopicMetricCounters {
    pub fn record(&self, msgs: u64, bytes: u64) {
        self.total_msgs.fetch_add(msgs, Ordering::Relaxed);
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Per-topic last-observed snapshot used to turn cumulative
/// counters into rates. Lives on the admin API and is process-wide:
/// every dashboard poll computes (current_total - previous_total)
/// / elapsed_seconds. First-ever request returns 0.0 for the rate
/// because there's no prior sample to subtract from.
#[derive(Debug, Default)]
struct RateCacheState {
    /// `(when_observed, total_msgs_snapshot, total_bytes_snapshot)`
    /// keyed by topic name.
    snapshots: HashMap<String, (Instant, u64, u64)>,
}

#[derive(Debug, Default)]
struct RateCache {
    inner: Mutex<RateCacheState>,
}

impl RateCache {
    fn observe(&self, topic: &str, msgs: u64, bytes: u64) -> (f64, f64) {
        let now = Instant::now();
        let mut state = self.inner.lock().unwrap();
        let rate = match state.snapshots.get(topic) {
            Some(&(prev_t, prev_m, prev_b)) => {
                let elapsed = now.duration_since(prev_t).as_secs_f64();
                if elapsed <= 1e-3 {
                    (0.0, 0.0)
                } else {
                    let dm = msgs.saturating_sub(prev_m) as f64;
                    let db = bytes.saturating_sub(prev_b) as f64;
                    (dm / elapsed, db / elapsed)
                }
            }
            None => (0.0, 0.0),
        };
        state.snapshots.insert(topic.to_string(), (now, msgs, bytes));
        rate
    }
}

/// Process-wide rate cache. Shared across every admin-API request so
/// successive polls see consistent deltas regardless of which TCP
/// connection happened to serve them. `OnceLock` so it costs
/// nothing in production builds where the dashboard isn't reached.
fn rate_cache() -> &'static RateCache {
    use std::sync::OnceLock;
    static CACHE: OnceLock<RateCache> = OnceLock::new();
    CACHE.get_or_init(RateCache::default)
}

pub fn nodes_snapshot(broker: &Broker) -> Vec<NodeStat> {
    let uptime = broker.uptime_seconds();
    let my_id = broker.cluster_node_id().unwrap_or(0);

    // Pull real openraft metrics for the local row — term,
    // commit_index, last_applied, and the leader id so we can mark
    // self as "leader" vs "follower" correctly. Single-node /
    // single-broker setups (no cluster configured) keep the prior
    // hardcoded shape since openraft isn't booted.
    let (term, commit_index, last_applied, leader_id) =
        broker.raft_metrics().unwrap_or((0, 0, 0, Some(my_id)));
    let my_role: &'static str = match leader_id {
        Some(id) if id == my_id => "leader",
        Some(_) => "follower",
        None => "candidate",
    };

    let mut out = vec![NodeStat {
        id: my_id,
        addr: broker
            .advertised_addr()
            .unwrap_or_else(|| "self".to_string()),
        role: my_role,
        status: "healthy",
        term,
        commit_index,
        last_applied,
        uptime_seconds: uptime,
    }];
    for peer in broker.peer_addrs() {
        let peer_role = match leader_id {
            Some(id) if id == peer.0 => "leader",
            _ => "follower",
        };
        out.push(NodeStat {
            id: peer.0,
            addr: peer.1,
            role: peer_role,
            // Peer reachability isn't probed here yet — assume
            // healthy. A future enhancement could TCP-probe the
            // peer addr and flip to "down" on connect failure.
            status: "healthy",
            term,
            commit_index,
            last_applied,
            uptime_seconds: 0,
        });
    }
    out
}

pub fn topics_snapshot(broker: &Broker) -> Vec<TopicStat> {
    // Read the background-refreshed cache instead of calling
    // `all_stats()` here. `all_stats()` locks every partition's
    // queue mutex — under heavy producer load (1k+ concurrent
    // sends) that lock is essentially never free, and /api/topics
    // would time out. The cache is refreshed once per second by a
    // task spawned in `Broker::open_with_config`; stats are at
    // most ~1s stale, comfortably inside the dashboard's 2s
    // polling window.
    let stats = broker.cached_topic_stats();
    // Index running totals once so we don't lock per-topic.
    let totals: HashMap<String, (u64, u64)> = broker
        .topic_metric_totals()
        .into_iter()
        .map(|(name, m, b)| (name, (m, b)))
        .collect();
    let cache = rate_cache();
    stats
        .into_iter()
        .map(|(name, s)| {
            let (msgs_total, bytes_total) =
                totals.get(&name).copied().unwrap_or((0, 0));
            let (msgs_per_sec, bytes_per_sec) =
                cache.observe(&name, msgs_total, bytes_total);
            TopicStat {
                name,
                partitions: s.partition_count,
                pending: s.pending,
                inflight: s.inflight,
                dead: s.dead,
                retention_mode: "queue",
                bytes_per_sec,
                msgs_per_sec,
                // Field order in the literal matters for clippy
                // only, not the JSON; serde renames handle wire shape.
            }
        })
        .collect()
}

pub async fn groups_snapshot(broker: &Broker) -> Vec<GroupStat> {
    let mut out: Vec<GroupStat> = Vec::new();

    // gRPC-side groups: one entry per (topic, group_id). Lag is
    // joined with the topic's high-water mark across all partitions
    // — the gRPC ConsumerGroup tracks partition assignment but not
    // a per-(group,partition) commit cursor on the same struct, so
    // we report 0 lag here. Real lag for gRPC clients lives in the
    // queue's RANGES_TABLE pending count, which `topics_snapshot`
    // already shows as `pending` per topic.
    for entry in broker.grpc_groups_snapshot() {
        out.push(GroupStat {
            name: entry.group_id,
            topic: entry.topic,
            members: entry.member_count,
            lag: 0,
            status: if entry.member_count == 0 {
                "empty"
            } else {
                "stable"
            },
            generation: entry.generation,
        });
    }

    // Kafka-side groups: ask every coordinator for its snapshot
    // concurrently. Each coordinator's committed offsets are joined
    // with the broker's per-(topic,partition) high-water mark to
    // produce a real lag number — one row per (group, topic).
    for kgroup in broker.kafka_groups_snapshot().await {
        // Aggregate per-topic across the committed (topic, partition,
        // offset) entries.
        let mut by_topic: HashMap<String, i64> = HashMap::new();
        for (topic, partition, offset) in &kgroup.committed {
            let high = broker.partition_high_water(topic, *partition as u32);
            let partition_lag = high
                .map(|h| (h as i64 - *offset).max(0) as u64)
                .unwrap_or(0);
            *by_topic.entry(topic.clone()).or_insert(0) += partition_lag as i64;
        }
        let status: &'static str = match kgroup.state {
            "Empty" => "empty",
            "Stable" => "stable",
            _ => "rebalancing",
        };
        // Emit one row per topic the group has touched. For empty
        // groups with no committed offsets, emit a single placeholder
        // row so the UI doesn't drop the group entirely.
        if by_topic.is_empty() {
            out.push(GroupStat {
                name: kgroup.group_id.clone(),
                topic: String::new(),
                members: kgroup.member_count,
                lag: 0,
                status,
                generation: kgroup.generation.max(0) as u64,
            });
        } else {
            for (topic, lag) in by_topic {
                out.push(GroupStat {
                    name: kgroup.group_id.clone(),
                    topic,
                    members: kgroup.member_count,
                    lag: lag.max(0) as u64,
                    status,
                    generation: kgroup.generation.max(0) as u64,
                });
            }
        }
    }

    out
}

pub fn workflows_snapshot(broker: &Broker) -> Vec<WorkflowStat> {
    // Background-cached read — see `topics_snapshot` for the
    // rationale. Direct `workflow_stats()` here would queue
    // behind the engine mutex held during create/complete/cancel.
    broker
        .cached_workflow_stats()
        .into_iter()
        .map(|(name, stats)| WorkflowStat {
            name,
            tasks: stats.total,
            running: stats.running + stats.pending,
            done: stats.done,
            failed: stats.failed,
            cancelled: stats.cancelled,
        })
        .collect()
}

pub fn events_snapshot(broker: &Broker) -> Vec<EventLine> {
    broker.events_snapshot()
}

/// Render the JSON body for a given path. Unknown paths return None.
pub async fn render_route(path: &str, broker: &Arc<Broker>) -> Option<String> {
    match path {
        "/api/health" => Some(
            serde_json::json!({
                "status": "ok",
                "uptimeSeconds": broker.uptime_seconds(),
            })
            .to_string(),
        ),
        "/api/nodes" => Some(serde_json::to_string(&nodes_snapshot(broker)).ok()?),
        "/api/topics" => Some(serde_json::to_string(&topics_snapshot(broker)).ok()?),
        "/api/groups" => {
            Some(serde_json::to_string(&groups_snapshot(broker).await).ok()?)
        }
        "/api/workflows" => {
            Some(serde_json::to_string(&workflows_snapshot(broker)).ok()?)
        }
        "/api/events" => Some(serde_json::to_string(&events_snapshot(broker)).ok()?),
        _ => None,
    }
}

/// Lightweight DTO the broker hands to `groups_snapshot` for gRPC
/// groups. Stays in `admin_api` so the broker's groups module
/// doesn't need to know about JSON shape.
pub struct GrpcGroupEntry {
    pub group_id: String,
    pub topic: String,
    pub member_count: u32,
    pub generation: u64,
}

/// The Kafka coordinator's snapshot type, augmented with the
/// group_id (which the coordinator doesn't carry — it's the map key
/// on the broker side).
pub struct KafkaGroupSnapshotEntry {
    pub group_id: String,
    pub state: &'static str,
    pub generation: i32,
    pub member_count: u32,
    pub committed: Vec<(String, i32, i64)>,
}

pub use crate::kafka::group_coordinator::GroupSnapshot as KafkaGroupSnapshot;
