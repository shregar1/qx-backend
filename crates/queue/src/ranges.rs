//! Range-encoded message state.
//!
//! The old layout stored one redb row per message in `MESSAGES_TABLE`.
//! With contiguous Pending offsets that meant N B-tree inserts per
//! produce batch and a B-tree delete per ack — the dominant cost in
//! every produce / receive / ack hot path.
//!
//! This module stores *intervals* of homogeneous state. A fresh queue
//! with 1M Pending messages is one row, not 1M rows. A receive of K
//! messages off the head of that range splits it into an `InFlight`
//! prefix of length K and a `Pending` suffix — two rows. An in-order
//! ack shrinks the head range by 1 (remove + insert on the head key)
//! and an `ack_batch` collapses N acks into a single B-tree update.
//!
//! Messages with a per-row key (dedup or compaction) cannot share a
//! range, so they live as singletons (`end = start + 1`). They mix
//! freely with multi-offset ranges in the same table because both use
//! the `start_offset` as the key.
//!
//! Encoding (little-endian, packed):
//! ```text
//! u64 end_offset_exclusive
//! u8  status              (0=Pending, 1=InFlight, 2=Dead)
//! u32 attempts
//! u64 inflight_until_nanos
//! u32 dedup_key_len       (0 ⇒ no dedup key)
//! [u8; dedup_key_len] dedup_key
//! u32 compaction_key_len  (0 ⇒ no compaction key)
//! [u8; compaction_key_len] compaction_key
//! ```
//!
//! A range can only be merged with its predecessor on `send` when:
//!   - the predecessor ends exactly at the new start,
//!   - both are `Pending` with the same `attempts` value,
//!   - neither carries a dedup or compaction key.
//! Otherwise a fresh row is inserted.

use qx_storage::RecordId;
use redb::{ReadableTable, Table, TableDefinition};

use crate::error::{QueueError, Result};
use crate::state::Status;

pub(crate) const RANGES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("ranges");

const HEADER_BYTES: usize = 8 + 1 + 4 + 8 + 4 + 4;

/// One range row: covers `[start, end_offset)` (start is implied by the
/// redb key). All offsets in this row share the same `status`,
/// `attempts`, and `inflight_until_nanos`.
///
/// For singleton rows that carry a per-row key, `end_offset = start + 1`.
#[derive(Debug, Clone)]
pub(crate) struct RangeEntry {
    pub end_offset: RecordId,
    pub status: Status,
    pub attempts: u32,
    pub inflight_until_nanos: u64,
    pub dedup_key: Option<Vec<u8>>,
    pub compaction_key: Option<Vec<u8>>,
}

impl RangeEntry {
    pub(crate) fn pending_run(end_offset: RecordId) -> Self {
        Self {
            end_offset,
            status: Status::Pending,
            attempts: 0,
            inflight_until_nanos: 0,
            dedup_key: None,
            compaction_key: None,
        }
    }

    pub(crate) fn singleton_pending_dedup(start: RecordId, key: Vec<u8>) -> Self {
        Self {
            end_offset: start + 1,
            status: Status::Pending,
            attempts: 0,
            inflight_until_nanos: 0,
            dedup_key: Some(key),
            compaction_key: None,
        }
    }

    pub(crate) fn singleton_pending_compaction(start: RecordId, key: Vec<u8>) -> Self {
        Self {
            end_offset: start + 1,
            status: Status::Pending,
            attempts: 0,
            inflight_until_nanos: 0,
            dedup_key: None,
            compaction_key: Some(key),
        }
    }

    pub(crate) fn length(&self, start: RecordId) -> u64 {
        self.end_offset.saturating_sub(start)
    }

    pub(crate) fn has_any_key(&self) -> bool {
        self.dedup_key.is_some() || self.compaction_key.is_some()
    }

    /// Can a Pending row with the same `attempts` and no per-row key
    /// be appended to this entry (extending `end_offset` by 1+)?
    pub(crate) fn is_extendable_with_pending(&self) -> bool {
        matches!(self.status, Status::Pending)
            && self.attempts == 0
            && self.inflight_until_nanos == 0
            && !self.has_any_key()
    }

    pub(crate) fn is_claimable(&self, now_nanos: u64) -> bool {
        match self.status {
            Status::Pending => true,
            Status::InFlight => self.inflight_until_nanos <= now_nanos,
            Status::Dead => false,
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let dedup_len = self.dedup_key.as_ref().map_or(0, Vec::len);
        let compact_len = self.compaction_key.as_ref().map_or(0, Vec::len);
        let mut buf = Vec::with_capacity(HEADER_BYTES + dedup_len + compact_len);
        buf.extend_from_slice(&self.end_offset.to_le_bytes());
        buf.push(self.status as u8);
        buf.extend_from_slice(&self.attempts.to_le_bytes());
        buf.extend_from_slice(&self.inflight_until_nanos.to_le_bytes());
        buf.extend_from_slice(&(dedup_len as u32).to_le_bytes());
        if let Some(k) = &self.dedup_key {
            buf.extend_from_slice(k);
        }
        buf.extend_from_slice(&(compact_len as u32).to_le_bytes());
        if let Some(k) = &self.compaction_key {
            buf.extend_from_slice(k);
        }
        buf
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES {
            return Err(QueueError::CorruptState);
        }
        let end_offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let status = match bytes[8] {
            0 => Status::Pending,
            1 => Status::InFlight,
            2 => Status::Dead,
            _ => return Err(QueueError::CorruptState),
        };
        let attempts = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let inflight_until_nanos = u64::from_le_bytes(bytes[13..21].try_into().unwrap());
        let dedup_len = u32::from_le_bytes(bytes[21..25].try_into().unwrap()) as usize;
        let dedup_end = 25 + dedup_len;
        if bytes.len() < dedup_end + 4 {
            return Err(QueueError::CorruptState);
        }
        let dedup_key = if dedup_len > 0 {
            Some(bytes[25..dedup_end].to_vec())
        } else {
            None
        };
        let compact_len = u32::from_le_bytes(
            bytes[dedup_end..dedup_end + 4].try_into().unwrap(),
        ) as usize;
        let compact_start = dedup_end + 4;
        if bytes.len() < compact_start + compact_len {
            return Err(QueueError::CorruptState);
        }
        let compaction_key = if compact_len > 0 {
            Some(bytes[compact_start..compact_start + compact_len].to_vec())
        } else {
            None
        };
        Ok(Self {
            end_offset,
            status,
            attempts,
            inflight_until_nanos,
            dedup_key,
            compaction_key,
        })
    }
}

/// Append a contiguous run of `count` Pending offsets starting at
/// `start`. The new run is merged into the preceding range when both
/// are Pending / attempts == 0 / key-free / adjacent. Otherwise a fresh
/// row is inserted.
pub(crate) fn append_pending_run(
    table: &mut Table<'_, u64, &'static [u8]>,
    start: RecordId,
    count: u64,
) -> Result<()> {
    debug_assert!(count > 0, "append_pending_run with count=0 is a bug");
    let new_end = start + count;

    let prev = {
        let mut iter = table.range(..start)?;
        iter.next_back()
            .transpose()?
            .map(|(k, v)| (k.value(), RangeEntry::decode(v.value())))
    };
    if let Some((prev_start, decoded)) = prev {
        let prev_entry = decoded?;
        if prev_entry.end_offset == start && prev_entry.is_extendable_with_pending() {
            let merged = RangeEntry {
                end_offset: new_end,
                ..prev_entry
            };
            table.insert(prev_start, merged.encode().as_slice())?;
            return Ok(());
        }
    }
    let fresh = RangeEntry::pending_run(new_end);
    table.insert(start, fresh.encode().as_slice())?;
    Ok(())
}

/// Insert a single-offset row (used for dedup / compaction keys).
/// Singletons never merge — they always sit as their own row.
pub(crate) fn insert_singleton(
    table: &mut Table<'_, u64, &'static [u8]>,
    offset: RecordId,
    entry: RangeEntry,
) -> Result<()> {
    debug_assert_eq!(entry.end_offset, offset + 1, "singleton end must be start+1");
    table.insert(offset, entry.encode().as_slice())?;
    Ok(())
}

/// Look up the range whose `[start, end_offset)` covers `offset`. Used
/// by `ack`, `nack`, `extend_visibility`.
pub(crate) fn find_covering<T>(
    table: &T,
    offset: RecordId,
) -> Result<Option<(RecordId, RangeEntry)>>
where
    T: ReadableTable<u64, &'static [u8]>,
{
    let mut iter = table.range(..=offset)?;
    let Some(entry) = iter.next_back().transpose()? else {
        return Ok(None);
    };
    let start = entry.0.value();
    let decoded = RangeEntry::decode(entry.1.value())?;
    if decoded.end_offset > offset {
        Ok(Some((start, decoded)))
    } else {
        Ok(None)
    }
}

/// Remove `offset` from the range identified by `(start, entry)`. May
/// shrink the head, the tail, split into two ranges, or remove the row
/// entirely. The caller has already verified the range covers `offset`.
pub(crate) fn delete_offset(
    table: &mut Table<'_, u64, &'static [u8]>,
    start: RecordId,
    entry: &RangeEntry,
    offset: RecordId,
) -> Result<()> {
    debug_assert!(start <= offset && offset < entry.end_offset);
    let is_singleton = entry.end_offset == start + 1;
    if is_singleton {
        table.remove(start)?;
        return Ok(());
    }
    if offset == start {
        // shrink head: remove old key, re-insert under start+1
        table.remove(start)?;
        let shifted = RangeEntry {
            // attempts / inflight / status / key fields preserved
            ..entry.clone()
        };
        table.insert(start + 1, shifted.encode().as_slice())?;
        return Ok(());
    }
    if offset + 1 == entry.end_offset {
        // shrink tail: update end
        let shrunk = RangeEntry {
            end_offset: offset,
            ..entry.clone()
        };
        table.insert(start, shrunk.encode().as_slice())?;
        return Ok(());
    }
    // middle ack: split into two ranges, dropping `offset`.
    let left = RangeEntry {
        end_offset: offset,
        ..entry.clone()
    };
    let right = RangeEntry {
        end_offset: entry.end_offset,
        // keys (if any) belonged to the singleton — multi-offset
        // ranges by invariant have neither dedup nor compaction key.
        dedup_key: None,
        compaction_key: None,
        ..entry.clone()
    };
    table.insert(start, left.encode().as_slice())?;
    table.insert(offset + 1, right.encode().as_slice())?;
    Ok(())
}

/// Mutate `offset` in place while keeping the rest of `(start, entry)`
/// untouched. Used by `nack` (transition InFlight → Pending for one
/// offset) and `extend_visibility` (move the InFlight deadline for one
/// offset). On multi-offset ranges this implies a split.
pub(crate) fn replace_offset_with(
    table: &mut Table<'_, u64, &'static [u8]>,
    start: RecordId,
    entry: &RangeEntry,
    offset: RecordId,
    new_status: Status,
    new_attempts: u32,
    new_inflight_until: u64,
) -> Result<()> {
    debug_assert!(start <= offset && offset < entry.end_offset);
    let is_singleton = entry.end_offset == start + 1;
    if is_singleton {
        let mutated = RangeEntry {
            status: new_status,
            attempts: new_attempts,
            inflight_until_nanos: new_inflight_until,
            ..entry.clone()
        };
        table.insert(start, mutated.encode().as_slice())?;
        return Ok(());
    }
    // multi-offset: split around `offset`.
    if offset == start {
        // head: shrink original to start+1.. and insert new singleton
        // at start.
        table.remove(start)?;
        let tail = RangeEntry {
            end_offset: entry.end_offset,
            dedup_key: None,
            compaction_key: None,
            ..entry.clone()
        };
        table.insert(start + 1, tail.encode().as_slice())?;
        let head = RangeEntry {
            end_offset: start + 1,
            status: new_status,
            attempts: new_attempts,
            inflight_until_nanos: new_inflight_until,
            dedup_key: None,
            compaction_key: None,
        };
        table.insert(start, head.encode().as_slice())?;
        return Ok(());
    }
    if offset + 1 == entry.end_offset {
        // tail: shrink original to ..offset, insert singleton at offset.
        let head = RangeEntry {
            end_offset: offset,
            ..entry.clone()
        };
        table.insert(start, head.encode().as_slice())?;
        let tail = RangeEntry {
            end_offset: offset + 1,
            status: new_status,
            attempts: new_attempts,
            inflight_until_nanos: new_inflight_until,
            dedup_key: None,
            compaction_key: None,
        };
        table.insert(offset, tail.encode().as_slice())?;
        return Ok(());
    }
    // middle: three rows.
    let left = RangeEntry {
        end_offset: offset,
        ..entry.clone()
    };
    let middle = RangeEntry {
        end_offset: offset + 1,
        status: new_status,
        attempts: new_attempts,
        inflight_until_nanos: new_inflight_until,
        dedup_key: None,
        compaction_key: None,
    };
    let right = RangeEntry {
        end_offset: entry.end_offset,
        dedup_key: None,
        compaction_key: None,
        ..entry.clone()
    };
    table.insert(start, left.encode().as_slice())?;
    table.insert(offset, middle.encode().as_slice())?;
    table.insert(offset + 1, right.encode().as_slice())?;
    Ok(())
}

/// Highest known offset across all ranges, used by recovery to find the
/// boundary between "state-tracked" and "log-tail orphan" records.
pub(crate) fn max_known_offset<T>(table: &T) -> Result<Option<RecordId>>
where
    T: ReadableTable<u64, &'static [u8]>,
{
    let Some(entry) = table.last()? else {
        return Ok(None);
    };
    let decoded = RangeEntry::decode(entry.1.value())?;
    Ok(Some(decoded.end_offset - 1))
}

/// Lowest start offset (used by log compaction).
pub(crate) fn min_start_offset<T>(table: &T) -> Result<Option<RecordId>>
where
    T: ReadableTable<u64, &'static [u8]>,
{
    Ok(table.first()?.map(|(k, _)| k.value()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::Database;
    use tempfile::NamedTempFile;

    fn fresh_db() -> (NamedTempFile, Database) {
        let f = NamedTempFile::new().unwrap();
        let db = Database::create(f.path()).unwrap();
        {
            let txn = db.begin_write().unwrap();
            txn.open_table(RANGES_TABLE).unwrap();
            txn.commit().unwrap();
        }
        (f, db)
    }

    #[test]
    fn encode_decode_pending_run_roundtrips() {
        let e = RangeEntry::pending_run(1_000_000);
        let bytes = e.encode();
        let back = RangeEntry::decode(&bytes).unwrap();
        assert_eq!(back.end_offset, 1_000_000);
        assert_eq!(back.status, Status::Pending);
        assert_eq!(back.attempts, 0);
        assert!(back.dedup_key.is_none() && back.compaction_key.is_none());
    }

    #[test]
    fn encode_decode_singleton_with_dedup_key_roundtrips() {
        let e = RangeEntry::singleton_pending_dedup(42, b"order-7".to_vec());
        let back = RangeEntry::decode(&e.encode()).unwrap();
        assert_eq!(back.end_offset, 43);
        assert_eq!(back.dedup_key.as_deref(), Some(&b"order-7"[..]));
        assert!(back.compaction_key.is_none());
    }

    #[test]
    fn decode_rejects_short_header() {
        assert!(RangeEntry::decode(&[0u8; 5]).is_err());
    }

    #[test]
    fn decode_rejects_invalid_status() {
        let mut bytes = RangeEntry::pending_run(1).encode();
        bytes[8] = 99;
        assert!(RangeEntry::decode(&bytes).is_err());
    }

    #[test]
    fn append_pending_run_merges_adjacent_pending() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 0, 100).unwrap();
            append_pending_run(&mut t, 100, 50).unwrap();
            append_pending_run(&mut t, 150, 1).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let rows: Vec<_> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value(), RangeEntry::decode(v.value()).unwrap().end_offset)
            })
            .collect();
        assert_eq!(rows, vec![(0u64, 151u64)], "three appends collapse to one row");
    }

    #[test]
    fn append_pending_run_does_not_merge_across_gap() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 0, 5).unwrap();
            // gap at 5..10
            append_pending_run(&mut t, 10, 5).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let rows: Vec<_> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value(), RangeEntry::decode(v.value()).unwrap().end_offset)
            })
            .collect();
        assert_eq!(rows, vec![(0u64, 5u64), (10u64, 15u64)]);
    }

    #[test]
    fn find_covering_returns_range_containing_offset() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 10, 20).unwrap(); // [10, 30)
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let (start, e) = find_covering(&t, 15).unwrap().unwrap();
        assert_eq!(start, 10);
        assert_eq!(e.end_offset, 30);
        assert!(find_covering(&t, 9).unwrap().is_none());
        assert!(find_covering(&t, 30).unwrap().is_none());
    }

    #[test]
    fn delete_offset_singleton_removes_row() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            insert_singleton(
                &mut t,
                7,
                RangeEntry::singleton_pending_dedup(7, b"k".to_vec()),
            )
            .unwrap();
            let (s, e) = find_covering(&t, 7).unwrap().unwrap();
            delete_offset(&mut t, s, &e, 7).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        assert!(t.first().unwrap().is_none());
    }

    #[test]
    fn delete_offset_head_shifts_start() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 0, 5).unwrap(); // [0, 5)
            let (s, e) = find_covering(&t, 0).unwrap().unwrap();
            delete_offset(&mut t, s, &e, 0).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let rows: Vec<_> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value(), RangeEntry::decode(v.value()).unwrap().end_offset)
            })
            .collect();
        assert_eq!(rows, vec![(1u64, 5u64)]);
    }

    #[test]
    fn delete_offset_tail_shrinks_end() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 0, 5).unwrap();
            let (s, e) = find_covering(&t, 4).unwrap().unwrap();
            delete_offset(&mut t, s, &e, 4).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let rows: Vec<_> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value(), RangeEntry::decode(v.value()).unwrap().end_offset)
            })
            .collect();
        assert_eq!(rows, vec![(0u64, 4u64)]);
    }

    #[test]
    fn delete_offset_middle_splits_range() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 0, 5).unwrap();
            let (s, e) = find_covering(&t, 2).unwrap().unwrap();
            delete_offset(&mut t, s, &e, 2).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let rows: Vec<_> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value(), RangeEntry::decode(v.value()).unwrap().end_offset)
            })
            .collect();
        assert_eq!(rows, vec![(0u64, 2u64), (3u64, 5u64)]);
    }

    #[test]
    fn replace_offset_with_middle_makes_three_rows() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            // [0, 10) InFlight attempts=1
            let r = RangeEntry {
                end_offset: 10,
                status: Status::InFlight,
                attempts: 1,
                inflight_until_nanos: 999,
                dedup_key: None,
                compaction_key: None,
            };
            t.insert(0u64, r.encode().as_slice()).unwrap();
            let (s, e) = find_covering(&t, 5).unwrap().unwrap();
            // simulate nack of offset 5
            replace_offset_with(&mut t, s, &e, 5, Status::Pending, 1, 0).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        let rows: Vec<_> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                let d = RangeEntry::decode(v.value()).unwrap();
                (k.value(), d.end_offset, d.status)
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (0u64, 5u64, Status::InFlight),
                (5u64, 6u64, Status::Pending),
                (6u64, 10u64, Status::InFlight),
            ]
        );
    }

    #[test]
    fn max_known_offset_returns_end_minus_one() {
        let (_f, db) = fresh_db();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(RANGES_TABLE).unwrap();
            append_pending_run(&mut t, 0, 100).unwrap();
        }
        txn.commit().unwrap();
        let rtxn = db.begin_read().unwrap();
        let t = rtxn.open_table(RANGES_TABLE).unwrap();
        assert_eq!(max_known_offset(&t).unwrap(), Some(99));
    }
}
