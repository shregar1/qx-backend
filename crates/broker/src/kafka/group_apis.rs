//! Consumer-group API handlers: FindCoordinator (10), JoinGroup (11),
//! SyncGroup (14), Heartbeat (12), LeaveGroup (13), OffsetCommit (8),
//! OffsetFetch (9). They all funnel into the per-group coordinator task.
//!
//! These handlers are async because they await responses from the
//! coordinator's `oneshot::Receiver`. The Kafka dispatcher already runs
//! per-connection on its own tokio task, so awaiting here doesn't block
//! the broker.

use tokio::sync::oneshot;

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use super::group_coordinator::{
    DescribeRequest, GroupMsg, HeartbeatRequest, JoinMember, JoinRequest, JoinResponse,
    LeaveRequest, OffsetCommitRequest, OffsetFetchRequest, SyncRequest,
};
use crate::server::Broker;

// ─────────────────────────────  FindCoordinator (10)  ─────────────────────

pub const FIND_COORDINATOR_KEY: i16 = 10;
pub const FIND_COORDINATOR_MIN: i16 = 3;
pub const FIND_COORDINATOR_MAX: i16 = 3;

pub fn handle_find_coordinator(
    w: &mut Writer,
    config: &super::server::KafkaConfig,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let _key = r.read_compact_string()?;
    let _key_type = r.read_i8()?;
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None); // error_message
    w.write_i32(config.node_id);
    w.write_compact_string(&config.advertised_host);
    w.write_i32(config.advertised_port);
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  JoinGroup (11)  ────────────────────────────

pub const JOIN_GROUP_KEY: i16 = 11;
pub const JOIN_GROUP_MIN: i16 = 9;
pub const JOIN_GROUP_MAX: i16 = 9;

pub async fn handle_join_group(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let group_id = r.read_compact_string()?;
    let session_timeout_ms = r.read_i32()?;
    let rebalance_timeout_ms = r.read_i32()?;
    let member_id = r.read_compact_string()?;
    let _group_instance_id = r.read_compact_nullable_string()?;
    let protocol_type = r.read_compact_string()?;
    let n = r.read_uvarint()?;
    let pcount = if n == 0 { 0 } else { n - 1 };
    let mut protocols = Vec::with_capacity(pcount as usize);
    for _ in 0..pcount {
        let name = r.read_compact_string()?;
        let metadata = r.read_compact_bytes()?.unwrap_or_default();
        r.read_tagged_fields()?;
        protocols.push((name, metadata));
    }
    r.read_tagged_fields()?;

    let coord = broker.group_coordinator(&group_id);
    let (tx, rx) = oneshot::channel();
    coord.send(GroupMsg::Join(JoinRequest {
        member_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        protocol_type,
        protocols,
        respond_to: tx,
    }));

    let resp = match rx.await {
        Ok(r) => r,
        Err(_) => JoinResponse {
            error_code: error_codes::COORDINATOR_NOT_AVAILABLE,
            generation_id: -1,
            protocol_name: None,
            leader: String::new(),
            member_id: String::new(),
            members: Vec::new(),
        },
    };

    w.write_i32(0); // throttle_time_ms
    w.write_i16(resp.error_code);
    w.write_i32(resp.generation_id);
    w.write_compact_nullable_string(Some("consumer")); // protocol_type
    w.write_compact_nullable_string(resp.protocol_name.as_deref());
    w.write_compact_string(&resp.leader);
    w.write_bool(false); // skip_assignment (v9+; we always assign)
    w.write_compact_string(&resp.member_id);
    w.write_compact_array_len(resp.members.len() as u32);
    for m in resp.members {
        w.write_compact_string(&m.member_id);
        w.write_compact_nullable_string(None); // group_instance_id
        w.write_uvarint(m.metadata.len() as u32 + 1); // compact_bytes
        w.write_bytes_raw(&m.metadata);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  SyncGroup (14)  ────────────────────────────

pub const SYNC_GROUP_KEY: i16 = 14;
pub const SYNC_GROUP_MIN: i16 = 5;
pub const SYNC_GROUP_MAX: i16 = 5;

pub async fn handle_sync_group(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let group_id = r.read_compact_string()?;
    let generation_id = r.read_i32()?;
    let member_id = r.read_compact_string()?;
    let _group_instance_id = r.read_compact_nullable_string()?;
    let _protocol_type = r.read_compact_nullable_string()?;
    let _protocol_name = r.read_compact_nullable_string()?;
    let n = r.read_uvarint()?;
    let acount = if n == 0 { 0 } else { n - 1 };
    let mut assignments = Vec::with_capacity(acount as usize);
    for _ in 0..acount {
        let mid = r.read_compact_string()?;
        let bytes = r.read_compact_bytes()?.unwrap_or_default();
        r.read_tagged_fields()?;
        assignments.push((mid, bytes));
    }
    r.read_tagged_fields()?;

    let coord = broker.group_coordinator(&group_id);
    let (tx, rx) = oneshot::channel();
    coord.send(GroupMsg::Sync(SyncRequest {
        member_id,
        generation_id,
        assignments,
        respond_to: tx,
    }));

    let resp = match rx.await {
        Ok(r) => r,
        Err(_) => super::group_coordinator::SyncResponse {
            error_code: error_codes::COORDINATOR_NOT_AVAILABLE,
            assignment: Vec::new(),
        },
    };

    w.write_i32(0); // throttle_time_ms
    w.write_i16(resp.error_code);
    w.write_compact_nullable_string(Some("consumer")); // protocol_type
    w.write_compact_nullable_string(None); // protocol_name (we don't track it on this path)
    w.write_uvarint(resp.assignment.len() as u32 + 1);
    w.write_bytes_raw(&resp.assignment);
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  Heartbeat (12)  ────────────────────────────

pub const HEARTBEAT_KEY: i16 = 12;
pub const HEARTBEAT_MIN: i16 = 4;
pub const HEARTBEAT_MAX: i16 = 4;

pub async fn handle_heartbeat(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let group_id = r.read_compact_string()?;
    let generation_id = r.read_i32()?;
    let member_id = r.read_compact_string()?;
    let _group_instance_id = r.read_compact_nullable_string()?;
    r.read_tagged_fields()?;

    let coord = broker.group_coordinator(&group_id);
    let (tx, rx) = oneshot::channel();
    coord.send(GroupMsg::Heartbeat(HeartbeatRequest {
        member_id,
        generation_id,
        respond_to: tx,
    }));
    let code = rx.await.unwrap_or(error_codes::COORDINATOR_NOT_AVAILABLE);

    w.write_i32(0);
    w.write_i16(code);
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  LeaveGroup (13)  ───────────────────────────

pub const LEAVE_GROUP_KEY: i16 = 13;
pub const LEAVE_GROUP_MIN: i16 = 4;
pub const LEAVE_GROUP_MAX: i16 = 4;

pub async fn handle_leave_group(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let group_id = r.read_compact_string()?;
    let n = r.read_uvarint()?;
    let mcount = if n == 0 { 0 } else { n - 1 };
    let mut member_ids = Vec::with_capacity(mcount as usize);
    for _ in 0..mcount {
        let mid = r.read_compact_string()?;
        let _gid = r.read_compact_nullable_string()?;
        // v5+ has a "reason" field. We parse it permissively: if there's
        // still bytes before tagged_fields, peek a string.
        // For simplicity, just call read_tagged_fields and rely on the
        // client never sending extra (works for v4).
        r.read_tagged_fields()?;
        member_ids.push(mid);
    }
    r.read_tagged_fields()?;

    let coord = broker.group_coordinator(&group_id);
    let (tx, rx) = oneshot::channel();
    coord.send(GroupMsg::Leave(LeaveRequest {
        member_ids,
        respond_to: tx,
    }));
    let results = rx
        .await
        .unwrap_or_default();

    w.write_i32(0);
    w.write_i16(error_codes::NONE);
    w.write_compact_array_len(results.len() as u32);
    for (mid, code) in results {
        w.write_compact_string(&mid);
        w.write_compact_nullable_string(None); // group_instance_id
        w.write_i16(code);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  OffsetCommit (8)  ──────────────────────────

pub const OFFSET_COMMIT_KEY: i16 = 8;
pub const OFFSET_COMMIT_MIN: i16 = 8;
pub const OFFSET_COMMIT_MAX: i16 = 8;

pub async fn handle_offset_commit(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let group_id = r.read_compact_string()?;
    let generation_id = r.read_i32()?;
    let _member_id = r.read_compact_string()?;
    let _group_instance_id = r.read_compact_nullable_string()?;
    let n = r.read_uvarint()?;
    let topic_count = if n == 0 { 0 } else { n - 1 };
    let mut commits = Vec::new();
    let mut topic_layout: Vec<(String, Vec<i32>)> = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut partitions_for_topic = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            let pidx = r.read_i32()?;
            let offset = r.read_i64()?;
            let _committed_leader_epoch = r.read_i32()?;
            let _metadata = r.read_compact_nullable_string()?;
            r.read_tagged_fields()?;
            commits.push(((name.clone(), pidx), offset));
            partitions_for_topic.push(pidx);
        }
        r.read_tagged_fields()?;
        topic_layout.push((name, partitions_for_topic));
    }
    r.read_tagged_fields()?;

    // Send to coordinator AND persist into qx-topic group_offset so that
    // ordinary `topic.stream(group, partition, ...)` advances correctly.
    let coord = broker.group_coordinator(&group_id);
    let (tx, rx) = oneshot::channel();
    coord.send(GroupMsg::OffsetCommit(OffsetCommitRequest {
        member_id: String::new(),
        generation_id,
        offsets: commits.clone(),
        respond_to: tx,
    }));
    let mut per_partition_err: std::collections::HashMap<(String, i32), i16> = rx
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();

    // Also push the committed offset into qx-topic so native consumers
    // (and the trigger dispatcher) see it.
    for ((topic_name, partition), offset) in commits {
        if per_partition_err
            .get(&(topic_name.clone(), partition))
            .copied()
            .unwrap_or(error_codes::NONE)
            != error_codes::NONE
        {
            continue;
        }
        if let Some(topic) = broker.lookup(&topic_name) {
            let _ = topic.commit_offset(&group_id, partition as u32, offset as u64);
        }
    }

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(topic_layout.len() as u32);
    for (topic_name, partitions) in topic_layout {
        w.write_compact_string(&topic_name);
        w.write_compact_array_len(partitions.len() as u32);
        for pidx in partitions {
            let code = per_partition_err
                .remove(&(topic_name.clone(), pidx))
                .unwrap_or(error_codes::NONE);
            w.write_i32(pidx);
            w.write_i16(code);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  OffsetFetch (9)  ──────────────────────────

pub const OFFSET_FETCH_KEY: i16 = 9;
pub const OFFSET_FETCH_MIN: i16 = 8;
pub const OFFSET_FETCH_MAX: i16 = 8;

pub async fn handle_offset_fetch(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let gn = r.read_uvarint()?;
    let group_count = if gn == 0 { 0 } else { gn - 1 };
    let mut groups_in = Vec::with_capacity(group_count as usize);
    for _ in 0..group_count {
        let group_id = r.read_compact_string()?;
        let tn = r.read_uvarint()?;
        let topics: Option<Vec<(String, Vec<i32>)>> = if tn == 0 {
            None
        } else {
            let tcount = tn - 1;
            let mut topics_v = Vec::with_capacity(tcount as usize);
            for _ in 0..tcount {
                let name = r.read_compact_string()?;
                let pn = r.read_uvarint()?;
                let pcount = if pn == 0 { 0 } else { pn - 1 };
                let mut partitions = Vec::with_capacity(pcount as usize);
                for _ in 0..pcount {
                    partitions.push(r.read_i32()?);
                }
                r.read_tagged_fields()?;
                topics_v.push((name, partitions));
            }
            Some(topics_v)
        };
        r.read_tagged_fields()?;
        groups_in.push((group_id, topics));
    }
    let _require_stable = r.read_bool()?;
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(groups_in.len() as u32);
    for (group_id, topics) in groups_in {
        let coord = broker.group_coordinator(&group_id);
        let (tx, rx) = oneshot::channel();
        coord.send(GroupMsg::OffsetFetch(OffsetFetchRequest {
            topics: topics.clone(),
            respond_to: tx,
        }));
        let entries = rx.await.unwrap_or_default();
        let by_key: std::collections::HashMap<(String, i32), i64> = entries.into_iter().collect();

        w.write_compact_string(&group_id);
        // Group the response back by topic. If the request specified topics
        // we use that layout; if not, derive it from the entries.
        let layout: Vec<(String, Vec<i32>)> = match topics {
            Some(t) => t,
            None => {
                let mut by_topic: std::collections::BTreeMap<String, Vec<i32>> =
                    std::collections::BTreeMap::new();
                for (k, _) in &by_key {
                    by_topic.entry(k.0.clone()).or_default().push(k.1);
                }
                by_topic.into_iter().collect()
            }
        };
        w.write_compact_array_len(layout.len() as u32);
        for (name, partitions) in layout {
            w.write_compact_string(&name);
            w.write_compact_array_len(partitions.len() as u32);
            for pidx in partitions {
                // Prefer the coordinator's in-memory view (which carries
                // any txn-in-flight state). Fall back to qx-topic's
                // durable group_offset so committed offsets survive a
                // broker restart, even when the coordinator is freshly
                // spawned post-restart and its HashMap is empty. The
                // coordinator returns `-1` for entries it doesn't have,
                // so treat that the same as "missing" and fall through.
                let from_coord = by_key.get(&(name.clone(), pidx)).copied();
                let offset = match from_coord {
                    Some(o) if o >= 0 => o,
                    _ => {
                        if let Some(topic) = broker.lookup(&name) {
                            match topic.group_offset(&group_id, pidx as u32) {
                                Ok(Some(o)) => o as i64,
                                _ => -1,
                            }
                        } else {
                            -1
                        }
                    }
                };
                w.write_i32(pidx);
                w.write_i64(offset);
                w.write_i32(-1); // committed_leader_epoch
                w.write_compact_nullable_string(None); // metadata
                w.write_i16(error_codes::NONE);
                w.write_empty_tagged_fields();
            }
            w.write_empty_tagged_fields();
        }
        w.write_i16(error_codes::NONE);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────────────  ListGroups (16)  ──────────────────────────

pub const LIST_GROUPS_KEY: i16 = 16;
pub const LIST_GROUPS_MIN: i16 = 4;
pub const LIST_GROUPS_MAX: i16 = 4;

/// ListGroups v4: returns every group the coordinator knows about.
///
/// v4 body is flexible:
///
/// ```text
/// states_filter compact_array<compact_string>
/// tagged_fields
/// ```
///
/// Response:
/// ```text
/// throttle_time_ms i32
/// error_code       i16
/// groups compact_array<{
///   group_id     compact_string
///   protocol_type compact_string
///   group_state  compact_string
///   tagged_fields
/// }>
/// tagged_fields
/// ```
pub async fn handle_list_groups(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let filter_count_raw = r.read_uvarint()?;
    let filter_count = if filter_count_raw == 0 {
        0
    } else {
        filter_count_raw - 1
    };
    let mut state_filter: Vec<String> = Vec::with_capacity(filter_count as usize);
    for _ in 0..filter_count {
        state_filter.push(r.read_compact_string()?);
    }
    r.read_tagged_fields()?;

    let coordinators = broker.group_coordinator_snapshot();
    let mut groups: Vec<(String, String, String)> = Vec::with_capacity(coordinators.len());
    for (gid, handle) in coordinators {
        let (tx, rx) = oneshot::channel();
        handle.send(GroupMsg::Describe(DescribeRequest { respond_to: tx }));
        if let Ok(desc) = rx.await {
            if !state_filter.is_empty() && !state_filter.iter().any(|s| s == desc.state) {
                continue;
            }
            groups.push((gid, desc.protocol_type, desc.state.to_string()));
        }
    }

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_array_len(groups.len() as u32);
    for (group_id, protocol_type, state) in groups {
        w.write_compact_string(&group_id);
        w.write_compact_string(&protocol_type);
        w.write_compact_string(&state);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ────────────────────────────  DescribeGroups (15)  ───────────────────────

pub const DESCRIBE_GROUPS_KEY: i16 = 15;
pub const DESCRIBE_GROUPS_MIN: i16 = 5;
pub const DESCRIBE_GROUPS_MAX: i16 = 5;

/// DescribeGroups v5 (flexible). Body:
///
/// ```text
/// groups compact_array<compact_string>
/// include_authorized_operations bool
/// tagged_fields
/// ```
///
/// Response (one entry per requested group):
/// ```text
/// throttle_time_ms i32
/// groups compact_array<{
///   error_code             i16
///   group_id               compact_string
///   group_state            compact_string
///   protocol_type          compact_string
///   protocol_data          compact_string  (i.e. negotiated protocol_name)
///   members compact_array<{
///     member_id          compact_string
///     group_instance_id  compact_nullable_string
///     client_id          compact_string
///     client_host        compact_string
///     member_metadata    compact_bytes
///     member_assignment  compact_bytes
///     tagged_fields
///   }>
///   authorized_operations i32
///   tagged_fields
/// }>
/// tagged_fields
/// ```
pub async fn handle_describe_groups(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n_raw = r.read_uvarint()?;
    let n = if n_raw == 0 { 0 } else { n_raw - 1 };
    let mut group_ids: Vec<String> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        group_ids.push(r.read_compact_string()?);
    }
    let _include_authorized = r.read_bool()?;
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(group_ids.len() as u32);
    for gid in group_ids {
        match broker.lookup_group_coordinator(&gid) {
            None => {
                w.write_i16(error_codes::NONE);
                w.write_compact_string(&gid);
                w.write_compact_string("Dead");
                w.write_compact_string("");
                w.write_compact_string("");
                w.write_compact_array_len(0);
                w.write_i32(0); // authorized_operations
                w.write_empty_tagged_fields();
            }
            Some(handle) => {
                let (tx, rx) = oneshot::channel();
                handle.send(GroupMsg::Describe(DescribeRequest { respond_to: tx }));
                let desc = rx.await.ok();
                let Some(desc) = desc else {
                    w.write_i16(error_codes::UNKNOWN_SERVER_ERROR);
                    w.write_compact_string(&gid);
                    w.write_compact_string("");
                    w.write_compact_string("");
                    w.write_compact_string("");
                    w.write_compact_array_len(0);
                    w.write_i32(0);
                    w.write_empty_tagged_fields();
                    continue;
                };
                w.write_i16(error_codes::NONE);
                w.write_compact_string(&gid);
                w.write_compact_string(desc.state);
                w.write_compact_string(&desc.protocol_type);
                w.write_compact_string(&desc.protocol_name);
                w.write_compact_array_len(desc.members.len() as u32);
                for m in desc.members {
                    w.write_compact_string(&m.member_id);
                    w.write_compact_nullable_string(None); // group_instance_id
                    w.write_compact_string(""); // client_id — not tracked
                    w.write_compact_string(""); // client_host — not tracked
                    w.write_compact_bytes(Some(&m.metadata));
                    w.write_compact_bytes(None); // assignment — would need separate cache
                    w.write_empty_tagged_fields();
                }
                w.write_i32(0); // authorized_operations
                w.write_empty_tagged_fields();
            }
        }
    }
    w.write_empty_tagged_fields();
    Ok(())
}
