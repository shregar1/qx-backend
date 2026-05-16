//! Streaming consumer-side reader for a `PartitionLog`.
//!
//! `LogReader` is a cursor: it knows the next record id to fetch and reads
//! sequentially. It borrows the log immutably, so concurrent appends require
//! interior mutability at a higher layer.

use crate::error::Result;
use crate::indexed_segment::RecordId;
use crate::partition_log::PartitionLog;
use crate::segment::Record;

pub struct LogReader<'a> {
    log: &'a PartitionLog,
    next_id: RecordId,
}

impl<'a> LogReader<'a> {
    pub fn new(log: &'a PartitionLog) -> Self {
        Self { log, next_id: 0 }
    }

    pub fn starting_at(log: &'a PartitionLog, id: RecordId) -> Self {
        Self { log, next_id: id }
    }

    pub fn position(&self) -> RecordId {
        self.next_id
    }

    pub fn seek(&mut self, id: RecordId) {
        self.next_id = id;
    }

    pub fn read_next(&mut self) -> Result<Option<(RecordId, Record)>> {
        if self.next_id >= self.log.next_record_id() {
            return Ok(None);
        }
        let id = self.next_id;
        let record = self.log.read(id)?;
        self.next_id = id + 1;
        Ok(Some((id, record)))
    }

    pub fn read_batch(&mut self, max: usize) -> Result<Vec<(RecordId, Record)>> {
        let mut out = Vec::with_capacity(max.min(64));
        for _ in 0..max {
            match self.read_next()? {
                Some(item) => out.push(item),
                None => break,
            }
        }
        Ok(out)
    }
}
