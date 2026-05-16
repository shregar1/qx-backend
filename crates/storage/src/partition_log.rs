//! Multi-segment Kafka-style log for a single partition.
//!
//! Layout on disk:
//!   <dir>/00000000000000000000.log   (base record id encoded in filename)
//!   <dir>/00000000000000000042.log
//!
//! Segments are sealed at `max_segment_bytes` and a fresh segment is created
//! whose base record id equals the next record id. The directory itself is
//! fsynced after each roll so newly-created segment files survive a crash
//! immediately following the roll (Kafka has historically been loose here).

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::indexed_segment::{IndexedSegment, RecordId};
use crate::segment::Record;

pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 1 << 30;
const SEGMENT_FILENAME_WIDTH: usize = 20;
const SEGMENT_EXTENSION: &str = "log";

#[derive(Clone, Debug)]
pub struct PartitionConfig {
    pub max_segment_bytes: u64,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        }
    }
}

pub struct PartitionLog {
    dir: PathBuf,
    segments: Vec<IndexedSegment>,
    config: PartitionConfig,
}

impl PartitionLog {
    pub fn open(dir: impl AsRef<Path>, config: PartitionConfig) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut bases = discover_segment_bases(&dir)?;
        bases.sort_unstable();

        let mut segments = Vec::with_capacity(bases.len());
        let mut expected_next: RecordId = 0;
        for base in bases {
            if base != expected_next {
                return Err(StorageError::SegmentGap {
                    expected: expected_next,
                    found: base,
                });
            }
            let path = segment_path(&dir, base);
            let seg = IndexedSegment::open(&path, base)?;
            expected_next = seg.next_record_id();
            segments.push(seg);
        }

        Ok(Self {
            dir,
            segments,
            config,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<RecordId> {
        if self.needs_new_segment() {
            self.roll_segment()?;
        }
        self.segments
            .last_mut()
            .expect("roll_segment guarantees an active segment")
            .append(payload)
    }

    pub fn read(&self, id: RecordId) -> Result<Record> {
        let idx = self
            .find_segment_index(id)
            .ok_or(StorageError::RecordNotFound { id })?;
        self.segments[idx].read(id)
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(active) = self.segments.last() {
            active.sync()?;
        }
        Ok(())
    }

    pub fn next_record_id(&self) -> RecordId {
        self.segments
            .last()
            .map(IndexedSegment::next_record_id)
            .unwrap_or(0)
    }

    pub fn len(&self) -> u64 {
        self.segments.iter().map(IndexedSegment::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Delete every record with id >= `record_id` (used by Raft to resolve
    /// conflicting log entries from a former leader). Segments wholly past
    /// `record_id` are unlinked; the partial-overlap segment is truncated
    /// in-place. The partition is never left empty — the last segment is
    /// preserved (possibly truncated to size 0) so subsequent appends
    /// continue from `record_id`.
    pub fn truncate_after(&mut self, record_id: RecordId) -> Result<usize> {
        let mut removed = 0;
        while self.segments.len() > 1
            && self.segments.last().unwrap().base_record_id() >= record_id
        {
            let path = self.segments.last().unwrap().path().to_path_buf();
            self.segments.pop();
            std::fs::remove_file(&path)?;
            removed += 1;
        }
        if let Some(last) = self.segments.last_mut() {
            last.truncate_after(record_id)?;
        }
        if removed > 0 {
            fsync_dir(&self.dir)?;
        }
        Ok(removed)
    }

    pub fn truncate_before(&mut self, record_id: RecordId) -> Result<usize> {
        let mut removed = 0;
        while self.segments.len() > 1
            && self.segments[0].next_record_id() <= record_id
        {
            let path = self.segments[0].path().to_path_buf();
            self.segments.remove(0);
            std::fs::remove_file(&path)?;
            removed += 1;
        }
        if removed > 0 {
            fsync_dir(&self.dir)?;
        }
        Ok(removed)
    }

    pub fn append_batch(&mut self, payloads: &[&[u8]]) -> Result<Vec<RecordId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        if self.needs_new_segment() {
            self.roll_segment()?;
        }
        let active = self
            .segments
            .last_mut()
            .expect("roll_segment guarantees an active segment");
        active.append_batch(payloads)
    }

    fn needs_new_segment(&self) -> bool {
        match self.segments.last() {
            None => true,
            Some(active) => active.size_bytes() >= self.config.max_segment_bytes,
        }
    }

    fn roll_segment(&mut self) -> Result<()> {
        let base = self.next_record_id();
        let path = segment_path(&self.dir, base);
        let segment = IndexedSegment::create(&path, base)?;
        self.segments.push(segment);
        fsync_dir(&self.dir)?;
        Ok(())
    }

    fn find_segment_index(&self, id: RecordId) -> Option<usize> {
        match self
            .segments
            .binary_search_by_key(&id, |s| s.base_record_id())
        {
            Ok(i) => Some(i),
            Err(0) => None,
            Err(i) => Some(i - 1),
        }
    }
}

fn segment_path(dir: &Path, base: RecordId) -> PathBuf {
    dir.join(format!(
        "{base:0width$}.{ext}",
        width = SEGMENT_FILENAME_WIDTH,
        ext = SEGMENT_EXTENSION
    ))
}

fn discover_segment_bases(dir: &Path) -> Result<Vec<RecordId>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some(SEGMENT_EXTENSION) {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| StorageError::InvalidSegmentName {
                name: path.display().to_string(),
            })?;
        let base: RecordId = stem
            .parse()
            .map_err(|_| StorageError::InvalidSegmentName {
                name: stem.to_string(),
            })?;
        bases.push(base);
    }
    Ok(bases)
}

fn fsync_dir(dir: &Path) -> Result<()> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}
