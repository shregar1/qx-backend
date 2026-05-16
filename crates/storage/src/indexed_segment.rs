//! Record-id addressing layered over a byte-offset `Segment` via composition.
//!
//! `IndexedSegment` owns a `Segment` and a dense in-memory map
//! `(record_id - base_record_id) -> byte offset`, rebuilt at open time by
//! scanning the segment. The on-disk format is unchanged — the index is
//! purely a runtime accelerator.

use std::path::Path;

use crate::error::{Result, StorageError};
use crate::segment::{Offset, Record, Segment};

pub type RecordId = u64;

pub struct IndexedSegment {
    segment: Segment,
    base_record_id: RecordId,
    record_offsets: Vec<Offset>,
}

impl IndexedSegment {
    pub fn create(path: impl AsRef<Path>, base_record_id: RecordId) -> Result<Self> {
        Ok(Self {
            segment: Segment::create(path)?,
            base_record_id,
            record_offsets: Vec::new(),
        })
    }

    pub fn open(path: impl AsRef<Path>, base_record_id: RecordId) -> Result<Self> {
        let segment = Segment::open(path)?;
        let record_offsets: Vec<Offset> = segment
            .iter_offsets()
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            segment,
            base_record_id,
            record_offsets,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<RecordId> {
        let byte_offset = self.segment.append(payload)?;
        let record_id = self.base_record_id + self.record_offsets.len() as RecordId;
        self.record_offsets.push(byte_offset);
        Ok(record_id)
    }

    pub fn read(&self, id: RecordId) -> Result<Record> {
        if id < self.base_record_id {
            return Err(StorageError::RecordNotFound { id });
        }
        let idx = (id - self.base_record_id) as usize;
        let byte_offset = *self
            .record_offsets
            .get(idx)
            .ok_or(StorageError::RecordNotFound { id })?;
        self.segment.read(byte_offset)
    }

    pub fn sync(&self) -> Result<()> {
        self.segment.sync()
    }

    pub fn base_record_id(&self) -> RecordId {
        self.base_record_id
    }

    pub fn next_record_id(&self) -> RecordId {
        self.base_record_id + self.record_offsets.len() as RecordId
    }

    pub fn len(&self) -> u64 {
        self.record_offsets.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.record_offsets.is_empty()
    }

    pub fn size_bytes(&self) -> u64 {
        self.segment.size_bytes()
    }

    pub fn path(&self) -> &Path {
        self.segment.path()
    }

    /// Truncate so the segment contains records `[base_record_id, record_id)`.
    /// If `record_id` is at or before the base, the segment becomes empty.
    /// If `record_id` exceeds the current next id, this is a no-op.
    pub fn truncate_after(&mut self, record_id: RecordId) -> Result<()> {
        if record_id >= self.next_record_id() {
            return Ok(());
        }
        let keep = record_id.saturating_sub(self.base_record_id) as usize;
        let new_write_pos = if keep == 0 {
            0
        } else {
            // Byte position right after the last kept record. The next record's
            // byte offset (record_offsets[keep]) gives us that boundary.
            self.record_offsets[keep]
        };
        self.segment.truncate_to(new_write_pos)?;
        self.record_offsets.truncate(keep);
        Ok(())
    }

    pub fn append_batch(&mut self, payloads: &[&[u8]]) -> Result<Vec<RecordId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let byte_offsets = self.segment.append_batch(payloads)?;
        let mut ids = Vec::with_capacity(payloads.len());
        for off in byte_offsets {
            let id = self.base_record_id + self.record_offsets.len() as RecordId;
            self.record_offsets.push(off);
            ids.push(id);
        }
        Ok(ids)
    }
}
