//! Kafka consumer-group coordinator.
//!
//! One tokio task per group. Owns all the per-group state — member set,
//! generation, current assignment — and serializes JoinGroup, SyncGroup,
//! Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch through an mpsc inbox.
//!
//! Rebalance protocol (eager):
//!
//! ```text
//! Empty                                 Stable
//!   │   JoinGroup arrives                 ▲ ───┐ LeaveGroup / Heartbeat
//!   │   member added, generation+1        │    │ timeout
//!   ▼                                     │    │
//! PreparingRebalance ─── join window ──── ▼    │
//!   ▲   JoinGroup arrives                      │
//!   │   member added/refreshed                 │
//!   │                                          │
//!   └──── close on deadline ──> CompletingRebalance
//!                                  │   SyncGroup from leader
//!                                  │   stores assignment, fans out
//!                                  ▼
//!                                Stable
//! ```
//!
//! `Heartbeat` returns `REBALANCE_IN_PROGRESS` while we're not Stable.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use super::error_codes;

/// How long we wait collecting JoinGroups after the first member kicks off
/// a rebalance. Real Kafka exposes this as `group.initial.rebalance.delay.ms`
/// (default 3000); 200ms is fine for tests and snappy enough for production.
const JOIN_WINDOW_MS: u64 = 200;

/// Generation id starts at 0 and increments on each rebalance.
type Generation = i32;

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

#[derive(Debug)]
struct Member {
    #[allow(dead_code)] // duplicated in the map key; useful for debugging
    member_id: String,
    /// Per-protocol metadata blob the client sent in JoinGroup — opaque to
    /// the broker, but the assignment leader needs it back so it can pick
    /// the right protocol and read embedded subscription info.
    protocols: Vec<(String, Vec<u8>)>,
    last_heartbeat: Instant,
    /// Window the broker waits between heartbeats before declaring a
    /// member dead. From the client's JoinGroup. Clamped to a sane range.
    session_timeout: Duration,
    /// Resolved on SyncGroup. None until the leader's SyncGroup fans out.
    pending_sync: Option<oneshot::Sender<SyncResponse>>,
}

#[derive(Debug)]
pub struct JoinRequest {
    pub member_id: String, // empty for first-time joiners
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    pub respond_to: oneshot::Sender<JoinResponse>,
}

#[derive(Debug, Clone)]
pub struct JoinMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
}

#[derive(Debug)]
pub struct JoinResponse {
    pub error_code: i16,
    pub generation_id: Generation,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    /// Populated only for the leader; empty list for followers.
    pub members: Vec<JoinMember>,
}

#[derive(Debug)]
pub struct SyncRequest {
    pub member_id: String,
    pub generation_id: Generation,
    /// Only the leader sends a non-empty assignment list.
    pub assignments: Vec<(String, Vec<u8>)>,
    pub respond_to: oneshot::Sender<SyncResponse>,
}

#[derive(Debug)]
pub struct SyncResponse {
    pub error_code: i16,
    pub assignment: Vec<u8>,
}

#[derive(Debug)]
pub struct HeartbeatRequest {
    pub member_id: String,
    pub generation_id: Generation,
    pub respond_to: oneshot::Sender<i16>, // error code
}

#[derive(Debug)]
pub struct LeaveRequest {
    pub member_ids: Vec<String>,
    pub respond_to: oneshot::Sender<Vec<(String, i16)>>, // (member_id, error_code)
}

#[derive(Debug)]
pub struct OffsetCommitRequest {
    pub member_id: String,
    pub generation_id: Generation,
    pub offsets: Vec<((String, i32), i64)>, // ((topic, partition), offset)
    pub respond_to: oneshot::Sender<Vec<((String, i32), i16)>>, // per-partition error
}

#[derive(Debug)]
pub struct OffsetFetchRequest {
    /// `None` = fetch every committed offset for the group.
    pub topics: Option<Vec<(String, Vec<i32>)>>,
    pub respond_to: oneshot::Sender<Vec<((String, i32), i64)>>, // ((topic, partition), offset)
}

#[derive(Debug)]
pub struct DescribeRequest {
    pub respond_to: oneshot::Sender<DescribeResponse>,
}

#[derive(Debug)]
pub struct OffsetDeleteRequest {
    pub keys: Vec<(String, i32)>,
    pub respond_to: oneshot::Sender<Vec<((String, i32), i16)>>,
}

#[derive(Debug, Clone)]
pub struct DescribeMember {
    pub member_id: String,
    /// The member's per-protocol metadata blob, matching the negotiated
    /// `protocol_name`. Empty for members that joined with a different
    /// protocol that lost the intersection.
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DescribeResponse {
    /// One of "Empty", "PreparingRebalance", "CompletingRebalance", "Stable".
    pub state: &'static str,
    /// `protocol_type` from the first JoinGroup. Empty if no member has
    /// ever joined.
    pub protocol_type: String,
    /// Negotiated protocol name (e.g. "range", "cooperative-sticky"). Empty
    /// while we haven't picked one.
    pub protocol_name: String,
    pub members: Vec<DescribeMember>,
}

#[derive(Debug)]
pub enum GroupMsg {
    Join(JoinRequest),
    Sync(SyncRequest),
    Heartbeat(HeartbeatRequest),
    Leave(LeaveRequest),
    OffsetCommit(OffsetCommitRequest),
    OffsetFetch(OffsetFetchRequest),
    OffsetDelete(OffsetDeleteRequest),
    Describe(DescribeRequest),
    /// Read-only introspection for the admin API. Returns one row
    /// per (topic, partition) committed by this group. Cheap — just
    /// clones in-memory state.
    Snapshot {
        respond_to: oneshot::Sender<GroupSnapshot>,
    },
    Shutdown,
}

/// In-memory snapshot of a coordinator's state. Returned by
/// `GroupMsg::Snapshot`; consumed by the admin API to populate
/// `/api/groups` without holding any actor-side state.
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub state: &'static str,
    pub generation: i32,
    pub member_count: u32,
    /// `(topic, partition, committed_offset)` for every committed
    /// position. The admin API aggregates these per-topic and joins
    /// with the broker's high-water mark to compute lag.
    pub committed: Vec<(String, i32, i64)>,
}

/// External handle: send messages to the coordinator over its mpsc inbox.
#[derive(Clone)]
pub struct CoordinatorHandle {
    inbox: mpsc::UnboundedSender<GroupMsg>,
}

impl CoordinatorHandle {
    pub fn send(&self, msg: GroupMsg) {
        let _ = self.inbox.send(msg);
    }
}

struct Coordinator {
    inbox: mpsc::UnboundedReceiver<GroupMsg>,
    group_id: String,
    state: State,
    generation: Generation,
    members: HashMap<String, Member>,
    leader: Option<String>,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    /// Parked JoinGroup responses, awaiting the join window to close.
    pending_joins: Vec<(String, oneshot::Sender<JoinResponse>)>,
    /// Committed offsets per (topic, partition). In-memory only for now.
    committed: HashMap<(String, i32), i64>,
    /// When the current join window closes (start time + JOIN_WINDOW_MS).
    join_deadline: Option<Instant>,
}

impl Coordinator {
    pub fn new(group_id: String, inbox: mpsc::UnboundedReceiver<GroupMsg>) -> Self {
        Self {
            inbox,
            group_id,
            state: State::Empty,
            generation: 0,
            members: HashMap::new(),
            leader: None,
            protocol_type: None,
            protocol_name: None,
            pending_joins: Vec::new(),
            committed: HashMap::new(),
            join_deadline: None,
        }
    }

    /// Run loop. Returns when the inbox closes or a Shutdown is received.
    pub async fn run(mut self) {
        let mut reap_interval = tokio::time::interval(Duration::from_secs(1));
        // Skip the initial immediate tick; first reap fires after 1s.
        reap_interval.tick().await;
        loop {
            let deadline = self.join_deadline;
            tokio::select! {
                msg = self.inbox.recv() => {
                    match msg {
                        Some(GroupMsg::Shutdown) | None => return,
                        Some(other) => self.handle(other),
                    }
                }
                _ = sleep_until_opt(deadline) => {
                    self.close_join_window();
                }
                _ = reap_interval.tick() => {
                    self.reap_stale_members();
                }
            }
        }
    }

    fn reap_stale_members(&mut self) {
        let now = Instant::now();
        let stale: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_heartbeat) > m.session_timeout)
            .map(|(id, _)| id.clone())
            .collect();
        if stale.is_empty() {
            return;
        }
        for id in &stale {
            self.members.remove(id);
        }
        if self.members.is_empty() {
            self.state = State::Empty;
            self.leader = None;
            self.protocol_name = None;
            self.join_deadline = None;
        } else if matches!(self.state, State::Stable | State::CompletingRebalance) {
            self.start_rebalance();
        }
    }

    fn handle(&mut self, msg: GroupMsg) {
        match msg {
            GroupMsg::Join(req) => self.on_join(req),
            GroupMsg::Sync(req) => self.on_sync(req),
            GroupMsg::Heartbeat(req) => self.on_heartbeat(req),
            GroupMsg::Leave(req) => self.on_leave(req),
            GroupMsg::OffsetCommit(req) => self.on_offset_commit(req),
            GroupMsg::OffsetFetch(req) => self.on_offset_fetch(req),
            GroupMsg::OffsetDelete(req) => self.on_offset_delete(req),
            GroupMsg::Describe(req) => self.on_describe(req),
            GroupMsg::Snapshot { respond_to } => self.on_snapshot(respond_to),
            GroupMsg::Shutdown => {}
        }
    }

    fn on_snapshot(&self, respond_to: oneshot::Sender<GroupSnapshot>) {
        let state = match self.state {
            State::Empty => "Empty",
            State::PreparingRebalance => "PreparingRebalance",
            State::CompletingRebalance => "CompletingRebalance",
            State::Stable => "Stable",
        };
        let committed: Vec<(String, i32, i64)> = self
            .committed
            .iter()
            .map(|((topic, partition), offset)| (topic.clone(), *partition, *offset))
            .collect();
        let _ = respond_to.send(GroupSnapshot {
            state,
            generation: self.generation,
            member_count: self.members.len() as u32,
            committed,
        });
    }

    fn on_describe(&self, req: DescribeRequest) {
        let state = match self.state {
            State::Empty => "Empty",
            State::PreparingRebalance => "PreparingRebalance",
            State::CompletingRebalance => "CompletingRebalance",
            State::Stable => "Stable",
        };
        let members: Vec<DescribeMember> = self
            .members
            .iter()
            .map(|(id, m)| {
                let metadata = m
                    .protocols
                    .iter()
                    .find(|(n, _)| Some(n) == self.protocol_name.as_ref())
                    .map(|(_, b)| b.clone())
                    .unwrap_or_default();
                DescribeMember {
                    member_id: id.clone(),
                    metadata,
                }
            })
            .collect();
        let resp = DescribeResponse {
            state,
            protocol_type: self.protocol_type.clone().unwrap_or_default(),
            protocol_name: self.protocol_name.clone().unwrap_or_default(),
            members,
        };
        let _ = req.respond_to.send(resp);
    }

    fn on_join(&mut self, req: JoinRequest) {
        let member_id = if req.member_id.is_empty() {
            new_member_id()
        } else {
            req.member_id.clone()
        };

        // Clamp to [1s, 5min]: clients sometimes send 0 (mistake) or
        // ludicrously large values.
        let session_timeout = Duration::from_millis(
            (req.session_timeout_ms.max(1_000).min(300_000)) as u64,
        );

        let member = Member {
            member_id: member_id.clone(),
            protocols: req.protocols.clone(),
            last_heartbeat: Instant::now(),
            session_timeout,
            pending_sync: None,
        };

        // Trigger a rebalance if we're currently Stable (a new or rejoining
        // member breaks the steady state).
        if matches!(self.state, State::Stable | State::CompletingRebalance) {
            self.start_rebalance();
        }
        if matches!(self.state, State::Empty) {
            self.start_rebalance();
        }

        self.members.insert(member_id.clone(), member);
        if self.protocol_type.is_none() {
            self.protocol_type = Some(req.protocol_type);
        }
        // Park the JoinGroup response until the join window closes.
        self.pending_joins.push((member_id, req.respond_to));
    }

    fn start_rebalance(&mut self) {
        self.state = State::PreparingRebalance;
        self.generation += 1;
        self.leader = None;
        self.protocol_name = None;
        // Clear any old pending syncs — they're for a stale generation.
        for member in self.members.values_mut() {
            if let Some(tx) = member.pending_sync.take() {
                let _ = tx.send(SyncResponse {
                    error_code: error_codes::REBALANCE_IN_PROGRESS,
                    assignment: Vec::new(),
                });
            }
        }
        self.join_deadline = Some(Instant::now() + Duration::from_millis(JOIN_WINDOW_MS));
    }

    fn close_join_window(&mut self) {
        if !matches!(self.state, State::PreparingRebalance) {
            self.join_deadline = None;
            return;
        }
        self.join_deadline = None;

        // Choose a leader: smallest member_id (deterministic).
        let mut ids: Vec<&String> = self.members.keys().collect();
        ids.sort();
        let leader = ids.first().map(|s| (*s).clone());
        self.leader = leader.clone();

        // Pick the protocol that all members support, taking the leader's
        // first preference that survives the intersection. This is what
        // lets cooperative-sticky negotiate cleanly when some members
        // offer it and the leader prefers it; if any member only knows
        // "range", we fall back to range.
        let protocol = compute_negotiated_protocol(&self.members, leader.as_deref());
        self.protocol_name = protocol.clone();

        // Snapshot the member metadata for the leader's response.
        let members_for_leader: Vec<JoinMember> = self
            .members
            .iter()
            .map(|(id, m)| JoinMember {
                member_id: id.clone(),
                metadata: m
                    .protocols
                    .iter()
                    .find(|(n, _)| Some(n) == protocol.as_ref())
                    .map(|(_, b)| b.clone())
                    .unwrap_or_default(),
            })
            .collect();

        self.state = State::CompletingRebalance;

        // Respond to every parked JoinGroup.
        let pending = std::mem::take(&mut self.pending_joins);
        for (member_id, tx) in pending {
            let is_leader = Some(&member_id) == leader.as_ref();
            let resp = JoinResponse {
                error_code: error_codes::NONE,
                generation_id: self.generation,
                protocol_name: protocol.clone(),
                leader: leader.clone().unwrap_or_default(),
                member_id,
                members: if is_leader { members_for_leader.clone() } else { Vec::new() },
            };
            let _ = tx.send(resp);
        }
    }

    fn on_sync(&mut self, req: SyncRequest) {
        if req.generation_id != self.generation {
            let _ = req.respond_to.send(SyncResponse {
                error_code: error_codes::REBALANCE_IN_PROGRESS,
                assignment: Vec::new(),
            });
            return;
        }
        if !self.members.contains_key(&req.member_id) {
            let _ = req.respond_to.send(SyncResponse {
                error_code: error_codes::UNKNOWN_MEMBER_ID,
                assignment: Vec::new(),
            });
            return;
        }

        let is_leader = Some(&req.member_id) == self.leader.as_ref();

        if is_leader {
            // Leader sent the full assignment map. Fan out to every member.
            let leader_id = req.member_id.clone();
            let mut leader_resp_opt = Some(req.respond_to);
            let mut by_member: HashMap<String, Vec<u8>> = req.assignments.into_iter().collect();
            let member_ids: Vec<String> = self.members.keys().cloned().collect();
            for member_id in member_ids {
                let assignment = by_member.remove(&member_id).unwrap_or_default();
                if member_id == leader_id {
                    if let Some(tx) = leader_resp_opt.take() {
                        let _ = tx.send(SyncResponse {
                            error_code: error_codes::NONE,
                            assignment,
                        });
                    }
                    if let Some(m) = self.members.get_mut(&member_id) {
                        m.pending_sync = None;
                    }
                    continue;
                }
                if let Some(m) = self.members.get_mut(&member_id) {
                    if let Some(tx) = m.pending_sync.take() {
                        let _ = tx.send(SyncResponse {
                            error_code: error_codes::NONE,
                            assignment,
                        });
                    } else {
                        // Follower hasn't called SyncGroup yet — stash the
                        // assignment for when they do, in protocols[0] under
                        // the magic key "__assignment__".
                        m.protocols = vec![(
                            "__assignment__".to_string(),
                            assignment,
                        )];
                    }
                }
            }
            self.state = State::Stable;
        } else {
            // Follower: if leader has already synced, our assignment is
            // stashed in protocols under "__assignment__".
            if let Some(m) = self.members.get_mut(&req.member_id) {
                let stashed = m
                    .protocols
                    .iter()
                    .find(|(n, _)| n == "__assignment__")
                    .map(|(_, b)| b.clone());
                if let Some(asg) = stashed {
                    let _ = req.respond_to.send(SyncResponse {
                        error_code: error_codes::NONE,
                        assignment: asg,
                    });
                } else {
                    // Park until leader's SyncGroup arrives.
                    m.pending_sync = Some(req.respond_to);
                }
            }
        }
    }

    fn on_heartbeat(&mut self, req: HeartbeatRequest) {
        let code = if req.generation_id != self.generation {
            error_codes::REBALANCE_IN_PROGRESS
        } else if !matches!(self.state, State::Stable) {
            error_codes::REBALANCE_IN_PROGRESS
        } else if !self.members.contains_key(&req.member_id) {
            error_codes::UNKNOWN_MEMBER_ID
        } else {
            if let Some(m) = self.members.get_mut(&req.member_id) {
                m.last_heartbeat = Instant::now();
            }
            error_codes::NONE
        };
        let _ = req.respond_to.send(code);
    }

    fn on_leave(&mut self, req: LeaveRequest) {
        let mut results = Vec::with_capacity(req.member_ids.len());
        let mut any_removed = false;
        for mid in &req.member_ids {
            let removed = self.members.remove(mid).is_some();
            if removed {
                any_removed = true;
            }
            results.push((mid.clone(), error_codes::NONE));
        }
        let _ = req.respond_to.send(results);

        if any_removed {
            if self.members.is_empty() {
                self.state = State::Empty;
                self.leader = None;
                self.protocol_name = None;
                self.join_deadline = None;
            } else {
                self.start_rebalance();
            }
        }
    }

    fn on_offset_commit(&mut self, req: OffsetCommitRequest) {
        let mut results = Vec::with_capacity(req.offsets.len());
        // Generation check: if the caller is fenced, reject every offset.
        let fenced = req.generation_id != self.generation && req.generation_id >= 0;
        for (key, offset) in req.offsets {
            if fenced {
                results.push((key, error_codes::REBALANCE_IN_PROGRESS));
            } else {
                self.committed.insert(key.clone(), offset);
                results.push((key, error_codes::NONE));
            }
        }
        let _ = req.respond_to.send(results);
    }

    fn on_offset_delete(&mut self, req: OffsetDeleteRequest) {
        let mut results = Vec::with_capacity(req.keys.len());
        for key in req.keys {
            let removed = self.committed.remove(&key).is_some();
            let code = if removed {
                error_codes::NONE
            } else {
                error_codes::UNKNOWN_TOPIC_OR_PARTITION
            };
            results.push((key, code));
        }
        let _ = req.respond_to.send(results);
    }

    fn on_offset_fetch(&mut self, req: OffsetFetchRequest) {
        let mut out = Vec::new();
        match req.topics {
            None => {
                for (k, v) in &self.committed {
                    out.push((k.clone(), *v));
                }
            }
            Some(topics) => {
                for (name, partitions) in topics {
                    for p in partitions {
                        let key = (name.clone(), p);
                        let val = self.committed.get(&key).copied().unwrap_or(-1);
                        out.push((key, val));
                    }
                }
            }
        }
        let _ = req.respond_to.send(out);
    }
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(t) => {
            let now = Instant::now();
            if t > now {
                tokio::time::sleep(t - now).await;
            }
            // already past — return immediately
        }
        None => std::future::pending::<()>().await,
    }
}

/// Intersect every member's offered protocol set and return the highest-
/// priority survivor from the leader's order. Returns `None` if the
/// intersection is empty (which we treat as "no agreement"; rebalance
/// proceeds anyway with protocol = None, which Kafka clients tolerate).
fn compute_negotiated_protocol(
    members: &HashMap<String, Member>,
    leader_id: Option<&str>,
) -> Option<String> {
    if members.is_empty() {
        return None;
    }
    let mut iter = members.values();
    let first = iter.next().unwrap();
    let mut intersect: std::collections::HashSet<String> =
        first.protocols.iter().map(|(n, _)| n.clone()).collect();
    for m in iter {
        let s: std::collections::HashSet<String> =
            m.protocols.iter().map(|(n, _)| n.clone()).collect();
        intersect.retain(|x| s.contains(x));
        if intersect.is_empty() {
            return None;
        }
    }
    let leader = leader_id.and_then(|id| members.get(id))?;
    leader
        .protocols
        .iter()
        .find(|(n, _)| intersect.contains(n))
        .map(|(n, _)| n.clone())
}

fn new_member_id() -> String {
    // 12 random base64-ish chars, enough for tests + uniqueness.
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let h = xxhash_rust::xxh3::xxh3_64_with_seed(&nanos.to_le_bytes(), 0xCAFEBEEFu64);
    format!("qx-{:016x}", h)
}

/// Spawn a coordinator task. Returns a handle the broker can clone freely.
pub fn spawn(group_id: String) -> CoordinatorHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let coord = Coordinator::new(group_id, rx);
    tokio::spawn(coord.run());
    CoordinatorHandle { inbox: tx }
}
