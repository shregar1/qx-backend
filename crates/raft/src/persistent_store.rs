//! Persistent `RaftStorage` over a `qx_storage::PartitionLog` + a side `redb`
//! database for metadata and indexing.
//!
//! Layout per node:
//!   `{dir}/node-{id}/raft-log/`       — `PartitionLog`, one record per Entry
//!   `{dir}/node-{id}/state.redb`      — vote, log-index map, snapshots, sm-log
//!
//! Raft index ↔ PartitionLog record id are decoupled via the `log_index`
//! table because openraft is free to start at any index and PartitionLog
//! assigns its own ids sequentially.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::path::Path;
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
use qx_storage::{PartitionConfig, PartitionLog};
use redb::{Database, ReadableTable, TableDefinition};
use tokio::sync::Mutex;

use crate::types::{AppResponse, NodeId, TypeConfig};

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const LOG_INDEX_TABLE: TableDefinition<u64, u64> = TableDefinition::new("log_index");
const SM_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("sm_log");
const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");

const META_VOTE: &str = "vote";
const META_LAST_PURGED: &str = "last_purged";
const META_LAST_APPLIED: &str = "last_applied";
const META_LAST_MEMBERSHIP: &str = "last_membership";
const SNAPSHOT_CURRENT: &str = "current";

#[derive(Default)]
struct StateMachine {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    log: Vec<Vec<u8>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

struct Inner {
    log: PartitionLog,
    db: Database,
    state_machine: StateMachine,
    current_snapshot: Option<StoredSnapshot>,
    snapshot_idx: u64,
    last_purged_log_id: Option<LogId<NodeId>>,
}

#[derive(Clone)]
pub struct PersistentStorage {
    inner: Arc<Mutex<Inner>>,
    apply_callback: Option<crate::store::ApplyCallback>,
}

impl PersistentStorage {
    pub async fn open(dir: impl AsRef<Path>) -> crate::error::Result<Self> {
        Self::open_with_callback(dir, None).await
    }

    pub async fn open_with_callback(
        dir: impl AsRef<Path>,
        apply_callback: Option<crate::store::ApplyCallback>,
    ) -> crate::error::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let log = PartitionLog::open(dir.join("raft-log"), PartitionConfig::default())?;
        let db = Database::create(dir.join("state.redb"))?;
        ensure_schema(&db)?;

        let mut state_machine = StateMachine::default();
        let mut last_purged_log_id = None;
        let mut current_snapshot = None;

        // Restore metadata
        let read_txn = db.begin_read()?;
        let meta = read_txn.open_table(META_TABLE)?;
        if let Some(v) = meta.get(META_LAST_PURGED)? {
            last_purged_log_id =
                Some(serde_json::from_slice(v.value()).map_err(decode_err)?);
        }
        if let Some(v) = meta.get(META_LAST_APPLIED)? {
            state_machine.last_applied =
                Some(serde_json::from_slice(v.value()).map_err(decode_err)?);
        }
        if let Some(v) = meta.get(META_LAST_MEMBERSHIP)? {
            state_machine.last_membership =
                serde_json::from_slice(v.value()).map_err(decode_err)?;
        }

        // Restore state machine log
        let sm = read_txn.open_table(SM_LOG_TABLE)?;
        for entry in sm.iter()? {
            let (_, v) = entry?;
            state_machine.log.push(v.value().to_vec());
        }

        // Restore current snapshot if any
        let snaps = read_txn.open_table(SNAPSHOT_TABLE)?;
        if let Some(v) = snaps.get(SNAPSHOT_CURRENT)? {
            current_snapshot =
                Some(serde_json::from_slice(v.value()).map_err(decode_err)?);
        }
        drop(meta);
        drop(sm);
        drop(snaps);
        drop(read_txn);

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                log,
                db,
                state_machine,
                current_snapshot,
                snapshot_idx: 0,
                last_purged_log_id,
            })),
            apply_callback,
        })
    }

    /// Walk the persisted **state machine log** (the sequence of applied
    /// payloads loaded from redb on open) front-to-back and invoke `callback`
    /// for every entry. Used to rebuild external-to-storage state on restart.
    ///
    /// This deliberately walks the state machine log rather than the raw
    /// Raft log: openraft may have taken a snapshot and purged early Raft
    /// log entries, but the state machine log on disk reflects every applied
    /// payload (load_on_open + apply both push to it).
    /// Walk the persisted state-machine log front-to-back and invoke
    /// `callback` for every entry with a synthetic raft-index (the
    /// 1-based position in the log). Used by in-memory state machines
    /// (e.g. `ReplicatedTopic`) to rebuild on restart.
    ///
    /// Persistent broker state should NOT use replay — its applier
    /// uses the real Raft log index to drive an idempotency cursor.
    pub async fn replay_to_callback<F>(&self, mut callback: F) -> crate::error::Result<()>
    where
        F: FnMut(u64, &[u8]),
    {
        let inner = self.inner.lock().await;
        for (i, payload) in inner.state_machine.log.iter().enumerate() {
            // synthetic 1-based index — replay isn't tied to the real
            // raft log index because the in-memory log only stores
            // payloads, not log ids.
            callback((i as u64) + 1, payload);
        }
        Ok(())
    }

    pub async fn state_machine_log(&self) -> Vec<Vec<u8>> {
        self.inner.lock().await.state_machine.log.clone()
    }

    pub async fn state_machine_len(&self) -> u64 {
        self.inner.lock().await.state_machine.log.len() as u64
    }
}

impl RaftLogReader<TypeConfig> for PersistentStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + Send,
    {
        let inner = self.inner.lock().await;
        let read_txn = inner.db.begin_read().map_err(io_err)?;
        let index_table = read_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;

        let mut out = Vec::new();
        let start = match range.start_bound() {
            Bound::Included(&v) => Bound::Included(v),
            Bound::Excluded(&v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(&v) => Bound::Included(v),
            Bound::Excluded(&v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        };
        for entry in index_table.range((start, end)).map_err(io_err)? {
            let (_, pl_id_g) = entry.map_err(io_err)?;
            let pl_id = pl_id_g.value();
            let record = inner.log.read(pl_id).map_err(io_err)?;
            let parsed: Entry<TypeConfig> =
                serde_json::from_slice(record.payload()).map_err(decode_err_logs)?;
            out.push(parsed);
        }
        Ok(out)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for PersistentStorage {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let last_applied = inner.state_machine.last_applied;
        let last_membership = inner.state_machine.last_membership.clone();
        let data = serde_json::to_vec(&inner.state_machine.log).map_err(snapshot_err)?;

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
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let bytes = serde_json::to_vec(&stored).map_err(snapshot_err)?;

        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut snaps = write_txn.open_table(SNAPSHOT_TABLE).map_err(io_err)?;
            snaps
                .insert(SNAPSHOT_CURRENT, bytes.as_slice())
                .map_err(io_err)?;
        }
        write_txn.commit().map_err(io_err)?;
        inner.current_snapshot = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for PersistentStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let bytes = serde_json::to_vec(vote).map_err(vote_err)?;
        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut meta = write_txn.open_table(META_TABLE).map_err(io_err)?;
            meta.insert(META_VOTE, bytes.as_slice()).map_err(io_err)?;
        }
        write_txn.commit().map_err(io_err)?;
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let read_txn = inner.db.begin_read().map_err(io_err)?;
        let meta = read_txn.open_table(META_TABLE).map_err(io_err)?;
        match meta.get(META_VOTE).map_err(io_err)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value()).map_err(vote_err)?)),
            None => Ok(None),
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let read_txn = inner.db.begin_read().map_err(io_err)?;
        let index_table = read_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;
        let last_log_id = match index_table.last().map_err(io_err)? {
            Some((idx_g, pl_g)) => {
                let pl_id = pl_g.value();
                let record = inner.log.read(pl_id).map_err(io_err)?;
                let parsed: Entry<TypeConfig> =
                    serde_json::from_slice(record.payload()).map_err(decode_err_logs)?;
                let _ = idx_g;
                Some(parsed.log_id)
            }
            None => inner.last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.inner.lock().await;
        // Append each entry to PartitionLog, collect (raft_index, pl_id) pairs.
        let mut index_updates: Vec<(u64, u64)> = Vec::new();
        for entry in entries {
            let bytes = serde_json::to_vec(&entry).map_err(decode_err_logs)?;
            let pl_id = inner.log.append(&bytes).map_err(io_err)?;
            index_updates.push((entry.log_id.index, pl_id));
        }
        inner.log.sync().map_err(io_err)?;

        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut index_table = write_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;
            for (raft_idx, pl_id) in index_updates {
                index_table.insert(raft_idx, pl_id).map_err(io_err)?;
            }
        }
        write_txn.commit().map_err(io_err)?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        // Find the earliest PartitionLog id corresponding to raft index >= log_id.index
        let (pl_id_at_or_after, conflicting_raft_indices) = {
            let read_txn = inner.db.begin_read().map_err(io_err)?;
            let index_table = read_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;
            let mut earliest_pl_id: Option<u64> = None;
            let mut keys: Vec<u64> = Vec::new();
            for entry in index_table.range(log_id.index..).map_err(io_err)? {
                let (k, pl_g) = entry.map_err(io_err)?;
                keys.push(k.value());
                let pl_id = pl_g.value();
                earliest_pl_id = Some(match earliest_pl_id {
                    None => pl_id,
                    Some(x) => x.min(pl_id),
                });
            }
            (earliest_pl_id, keys)
        };

        if let Some(pl_id) = pl_id_at_or_after {
            inner.log.truncate_after(pl_id).map_err(io_err)?;
        }

        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut index_table = write_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;
            for k in conflicting_raft_indices {
                index_table.remove(k).map_err(io_err)?;
            }
        }
        write_txn.commit().map_err(io_err)?;
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        // Collect raft indices to purge and find the maximum PL id among them
        let (max_pl_id, purged_raft_indices) = {
            let read_txn = inner.db.begin_read().map_err(io_err)?;
            let index_table = read_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;
            let mut max_pl_id: Option<u64> = None;
            let mut keys: Vec<u64> = Vec::new();
            for entry in index_table.range(..=log_id.index).map_err(io_err)? {
                let (k, pl_g) = entry.map_err(io_err)?;
                keys.push(k.value());
                let pl_id = pl_g.value();
                max_pl_id = Some(match max_pl_id {
                    None => pl_id,
                    Some(x) => x.max(pl_id),
                });
            }
            (max_pl_id, keys)
        };

        if let Some(pl_id) = max_pl_id {
            inner.log.truncate_before(pl_id + 1).map_err(io_err)?;
        }

        let purged_bytes = serde_json::to_vec(&log_id).map_err(decode_err_logs)?;
        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut index_table = write_txn.open_table(LOG_INDEX_TABLE).map_err(io_err)?;
            for k in purged_raft_indices {
                index_table.remove(k).map_err(io_err)?;
            }
            let mut meta = write_txn.open_table(META_TABLE).map_err(io_err)?;
            meta.insert(META_LAST_PURGED, purged_bytes.as_slice())
                .map_err(io_err)?;
        }
        write_txn.commit().map_err(io_err)?;
        inner.last_purged_log_id = Some(log_id);
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

        // Stage all writes for this batch in one redb txn.
        let mut new_payloads: Vec<Vec<u8>> = Vec::new();
        let mut new_last_applied: Option<LogId<NodeId>> = inner.state_machine.last_applied;
        let mut new_last_membership = inner.state_machine.last_membership.clone();

        for entry in entries {
            new_last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(AppResponse { index: 0 });
                }
                EntryPayload::Normal(req) => {
                    new_payloads.push(req.payload.clone());
                    let new_len = inner.state_machine.log.len() + new_payloads.len();
                    responses.push(AppResponse {
                        index: new_len as u64 - 1,
                    });
                    if let Some(cb) = &self.apply_callback {
                        cb(entry.log_id.index, &req.payload);
                    }
                }
                EntryPayload::Membership(m) => {
                    new_last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    responses.push(AppResponse { index: 0 });
                }
            }
        }

        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut sm_log = write_txn.open_table(SM_LOG_TABLE).map_err(io_err)?;
            for (i, p) in new_payloads.iter().enumerate() {
                let sm_idx = (inner.state_machine.log.len() + i) as u64;
                sm_log.insert(sm_idx, p.as_slice()).map_err(io_err)?;
            }
            let mut meta = write_txn.open_table(META_TABLE).map_err(io_err)?;
            if let Some(last) = new_last_applied {
                let bytes = serde_json::to_vec(&last).map_err(decode_err_logs)?;
                meta.insert(META_LAST_APPLIED, bytes.as_slice())
                    .map_err(io_err)?;
            }
            let mem_bytes = serde_json::to_vec(&new_last_membership).map_err(decode_err_logs)?;
            meta.insert(META_LAST_MEMBERSHIP, mem_bytes.as_slice())
                .map_err(io_err)?;
        }
        write_txn.commit().map_err(io_err)?;

        inner.state_machine.log.extend(new_payloads);
        inner.state_machine.last_applied = new_last_applied;
        inner.state_machine.last_membership = new_last_membership;

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

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let bytes = serde_json::to_vec(&stored).map_err(snapshot_err)?;

        let write_txn = inner.db.begin_write().map_err(io_err)?;
        {
            let mut sm_log = write_txn.open_table(SM_LOG_TABLE).map_err(io_err)?;
            // Wipe state machine table and rewrite from snapshot
            let keys: Vec<u64> = sm_log
                .iter()
                .map_err(io_err)?
                .filter_map(|e| e.ok().map(|(k, _)| k.value()))
                .collect();
            for k in keys {
                sm_log.remove(k).map_err(io_err)?;
            }
            for (i, p) in log.iter().enumerate() {
                sm_log.insert(i as u64, p.as_slice()).map_err(io_err)?;
            }
            let mut snaps = write_txn.open_table(SNAPSHOT_TABLE).map_err(io_err)?;
            snaps
                .insert(SNAPSHOT_CURRENT, bytes.as_slice())
                .map_err(io_err)?;
            let mut meta_t = write_txn.open_table(META_TABLE).map_err(io_err)?;
            if let Some(last) = meta.last_log_id {
                let lb = serde_json::to_vec(&last).map_err(decode_err_logs)?;
                meta_t
                    .insert(META_LAST_APPLIED, lb.as_slice())
                    .map_err(io_err)?;
            }
            let mb = serde_json::to_vec(&meta.last_membership).map_err(decode_err_logs)?;
            meta_t
                .insert(META_LAST_MEMBERSHIP, mb.as_slice())
                .map_err(io_err)?;
        }
        write_txn.commit().map_err(io_err)?;

        inner.state_machine.log = log;
        inner.state_machine.last_applied = meta.last_log_id;
        inner.state_machine.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.current_snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

fn ensure_schema(db: &Database) -> crate::error::Result<()> {
    let txn = db.begin_write()?;
    txn.open_table(META_TABLE)?;
    txn.open_table(LOG_INDEX_TABLE)?;
    txn.open_table(SM_LOG_TABLE)?;
    txn.open_table(SNAPSHOT_TABLE)?;
    txn.commit()?;
    Ok(())
}

fn io_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write(AnyError::new(&e)),
    }
}

fn decode_err(e: serde_json::Error) -> crate::error::RaftError {
    crate::error::RaftError::StateDb(Box::new(e))
}

fn decode_err_logs(e: serde_json::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(AnyError::new(&e)),
    }
}

fn vote_err(e: serde_json::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_vote(AnyError::new(&e)),
    }
}

fn snapshot_err(e: serde_json::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_snapshot(None, AnyError::new(&e)),
    }
}
