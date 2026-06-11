//! In-memory `RaftStorage` impl. Uses the legacy single-trait storage; the
//! `Adaptor` from openraft converts it into the v2 `RaftLogStorage` +
//! `RaftStateMachine` pair required by `Raft::new`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use tokio::sync::Mutex;

use crate::types::{AppResponse, NodeId, TypeConfig};

#[derive(Default, Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

#[derive(Default, Debug)]
struct StateMachine {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    log: Vec<Vec<u8>>,
}

#[derive(Default, Debug)]
struct Inner {
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged_log_id: Option<LogId<NodeId>>,
    state_machine: StateMachine,
    current_snapshot: Option<StoredSnapshot>,
    snapshot_idx: u64,
}

/// Side-channel callback invoked on each committed `EntryPayload::Normal`.
/// Used to drive external state (e.g. a replicated `qx_topic::Topic`).
///
/// The `u64` is the Raft log index of the entry. Brokers use it as a
/// monotonic apply cursor stored alongside the external state so a
/// crash between the callback running and the Raft commit landing on
/// disk doesn't cause a re-apply on restart (the cursor in the external
/// state survived, the callback sees `index <= cursor`, skips). For
/// in-memory state machines (e.g. `ReplicatedTopic` which loses its
/// log on restart and rebuilds from `replay_to_callback`) the index is
/// informational only.
///
/// During `replay_to_callback` the index is the position in the
/// persisted state-machine log (1-based) — a synthetic but
/// monotonically increasing value.
pub type ApplyCallback = Arc<dyn Fn(u64, &[u8]) + Send + Sync>;

#[derive(Clone)]
pub struct MemoryStorage {
    inner: Arc<Mutex<Inner>>,
    apply_callback: Option<ApplyCallback>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            apply_callback: None,
        }
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_apply_callback(callback: ApplyCallback) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            apply_callback: Some(callback),
        }
    }

    pub async fn state_machine_log(&self) -> Vec<Vec<u8>> {
        self.inner.lock().await.state_machine.log.clone()
    }

    pub async fn state_machine_len(&self) -> u64 {
        self.inner.lock().await.state_machine.log.len() as u64
    }
}

impl RaftLogReader<TypeConfig> for MemoryStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + Send,
    {
        let inner = self.inner.lock().await;
        Ok(inner
            .log
            .range(range)
            .map(|(_, v)| v.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemoryStorage {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let last_applied = inner.state_machine.last_applied;
        let last_membership = inner.state_machine.last_membership.clone();

        let data = serde_json::to_vec(&inner.state_machine.log).map_err(|e| {
            StorageError::IO {
                source: StorageIOError::read_state_machine(AnyError::new(&e)),
            }
        })?;

        inner.snapshot_idx += 1;
        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, inner.snapshot_idx)
        } else {
            format!("--{}", inner.snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for MemoryStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last = inner.log.values().last().map(|e| e.log_id);
        let last_log_id = last.or(inner.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> std::result::Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.last_purged_log_id = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> std::result::Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let inner = self.inner.lock().await;
        Ok((
            inner.state_machine.last_applied,
            inner.state_machine.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> std::result::Result<Vec<AppResponse>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            inner.state_machine.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(AppResponse { index: 0 });
                }
                EntryPayload::Normal(req) => {
                    inner.state_machine.log.push(req.payload.clone());
                    let index = inner.state_machine.log.len() as u64 - 1;
                    responses.push(AppResponse { index });
                    if let Some(cb) = &self.apply_callback {
                        cb(entry.log_id.index, &req.payload);
                    }
                }
                EntryPayload::Membership(m) => {
                    inner.state_machine.last_membership =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                    responses.push(AppResponse { index: 0 });
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let log: Vec<Vec<u8>> = serde_json::from_slice(&data).map_err(|e| StorageError::IO {
            source: StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;
        let mut inner = self.inner.lock().await;
        inner.state_machine.log = log;
        inner.state_machine.last_applied = meta.last_log_id;
        inner.state_machine.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        match &inner.current_snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}
