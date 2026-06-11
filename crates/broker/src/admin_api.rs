//! Read-only HTTP/JSON introspection surface for the qx-dashboard.
//!
//! Mounted on the same TCP listener that serves Prometheus
//! `/metrics`. Routes:
//!
//!   GET /api/health    → `{ "status": "ok", "uptimeSeconds": u64 }`
//!   GET /api/nodes     → `[NodeStat]`
//!   GET /api/topics    → `[TopicStat]`
//!   GET /api/groups    → `[GroupStat]`
//!   GET /api/workflows → `[WorkflowStat]`
//!   GET /api/events    → `[EventLine]` (placeholder: always empty)
//!
//! Schemas mirror the zod schemas in `qx-dashboard/src/schemas/cluster.ts`
//! exactly — field names are camelCase to match what the UI validates.
//! Adding a new endpoint here means: (1) add the type + builder in
//! this file, (2) add a route arm in `handle_admin_route`, (3) the
//! dashboard's `useCluster` polling layer picks it up automatically.
//!
//! Every response carries `Access-Control-Allow-Origin: *` so the dev
//! dashboard can fetch from a different origin (Vite at :5173) without
//! a CORS proxy.

use std::sync::Arc;

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

/// Top-level builders. Each one is intentionally cheap — they touch
/// only in-memory state, never the filesystem or redb directly
/// (other than what's already cached behind broker mutexes). Called
/// per `/api/*` request, so cost matters at the per-request level.

pub fn nodes_snapshot(broker: &Broker) -> Vec<NodeStat> {
    let uptime = broker.uptime_seconds();
    // Single-node mode: report this broker as a healthy leader. When
    // cluster mode is on we additionally enumerate configured peers
    // — full term/commit-index introspection needs an openraft
    // metrics tap we haven't wired yet; report the structurally
    // correct snapshot for the leader and best-effort peers.
    let my_id = broker.cluster_node_id().unwrap_or(0);
    let mut out = vec![NodeStat {
        id: my_id,
        addr: broker
            .advertised_addr()
            .unwrap_or_else(|| "self".to_string()),
        role: "leader",
        status: "healthy",
        term: 0,
        commit_index: 0,
        last_applied: 0,
        uptime_seconds: uptime,
    }];
    for peer in broker.peer_addrs() {
        out.push(NodeStat {
            id: peer.0,
            addr: peer.1,
            role: "follower",
            status: "healthy",
            term: 0,
            commit_index: 0,
            last_applied: 0,
            uptime_seconds: 0,
        });
    }
    out
}

pub fn topics_snapshot(broker: &Broker) -> Vec<TopicStat> {
    // `all_stats` aggregates per-topic counters from each `Topic`'s
    // in-memory snapshot. The retention/throughput fields are
    // placeholder — `Topic` doesn't track them per-second yet; the
    // dashboard will show 0 until that's wired.
    let stats = match broker.all_stats() {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stats
        .into_iter()
        .map(|(name, s)| TopicStat {
            name,
            partitions: s.partition_count,
            pending: s.pending,
            inflight: s.inflight,
            dead: s.dead,
            retention_mode: "queue",
            bytes_per_sec: 0.0,
            msgs_per_sec: 0.0,
        })
        .collect()
}

pub fn groups_snapshot(_broker: &Broker) -> Vec<GroupStat> {
    // Consumer-group state lives behind the group coordinator
    // actor (one per group). Surfacing it cleanly needs a
    // `snapshot()` message round-trip we haven't built yet. Return
    // empty so the UI renders the "no groups" empty state honestly.
    Vec::new()
}

pub fn workflows_snapshot(_broker: &Broker) -> Vec<WorkflowStat> {
    // Same shape as `groups_snapshot` — needs an enumeration hook on
    // the workflow Engine before we can return real counts.
    Vec::new()
}

pub fn events_snapshot(_broker: &Broker) -> Vec<EventLine> {
    Vec::new()
}

/// Render the JSON body for a given path. Unknown paths return None,
/// the caller maps that to 404.
pub fn render_route(path: &str, broker: &Arc<Broker>) -> Option<String> {
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
        "/api/groups" => Some(serde_json::to_string(&groups_snapshot(broker)).ok()?),
        "/api/workflows" => {
            Some(serde_json::to_string(&workflows_snapshot(broker)).ok()?)
        }
        "/api/events" => Some(serde_json::to_string(&events_snapshot(broker)).ok()?),
        _ => None,
    }
}
