//! Tiered storage: cold-tier records that have aged out of the hot
//! redb-backed log live in a `ColdStore` (typically S3 or a similar
//! object store; the in-tree impl uses the local filesystem).
//!
//! Semantics:
//!
//! - The hot tier (redb) keeps every record from `log_start_offset` to
//!   the high watermark.
//! - The cold tier holds a copy of records `[0, log_start_offset)`, one
//!   per `(partition, record_id)` key.
//! - On `read_record`, the topic first looks at the hot tier; if the
//!   id is below the local low watermark, it falls back to the cold
//!   store.
//! - Offloading is an explicit operation: `Topic::offload_to_cold`
//!   reads records `[start, end)` from the hot tier, pushes them to
//!   cold, then advances the local low-water mark. A future background
//!   worker can call this on a schedule.
//!
//! Why an explicit, sync-style trait rather than an async one: every
//! operation here is per-record (cheap blob fetch). For an actual
//! `aws-sdk-s3` impl the underlying call is async, but those
//! implementations can spawn-blocking or use the runtime as needed —
//! the broker code path is already inside `spawn_blocking`.

use std::path::PathBuf;

use crate::error::TopicError;

/// Trait every cold-tier backend implements. Methods are sync because
/// our broker call sites already run on the blocking pool.
pub trait ColdStore: Send + Sync + std::fmt::Debug {
    /// Write `payload` for `(partition, record_id)` to the cold tier.
    /// Idempotent: writing the same key twice is a no-op error-wise
    /// (the second write may overwrite).
    fn put(&self, partition: u32, record_id: u64, payload: &[u8])
        -> Result<(), TopicError>;

    /// Fetch a previously-written record. Returns `Ok(None)` if the
    /// record was never offloaded (i.e. the cold tier doesn't have it).
    fn get(&self, partition: u32, record_id: u64) -> Result<Option<Vec<u8>>, TopicError>;

    /// Reclaim the cold-tier storage for `(partition, record_id)`. Used
    /// during topic deletion. Best-effort.
    fn delete(&self, partition: u32, record_id: u64) -> Result<(), TopicError>;
}

/// Local-filesystem `ColdStore` implementation. Lays records out as
/// `<root>/partition-<n>/<record_id>.rec`. Good enough for development
/// and for shipping data to a network filesystem; an S3 impl plugs in
/// against the same trait.
#[derive(Debug, Clone)]
pub struct LocalFsColdStore {
    root: PathBuf,
}

impl LocalFsColdStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, TopicError> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path(&self, partition: u32, record_id: u64) -> PathBuf {
        self.root
            .join(format!("partition-{partition}"))
            .join(format!("{record_id:020}.rec"))
    }
}

impl ColdStore for LocalFsColdStore {
    fn put(
        &self,
        partition: u32,
        record_id: u64,
        payload: &[u8],
    ) -> Result<(), TopicError> {
        let p = self.path(partition, record_id);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&p, payload)?;
        Ok(())
    }

    fn get(
        &self,
        partition: u32,
        record_id: u64,
    ) -> Result<Option<Vec<u8>>, TopicError> {
        let p = self.path(partition, record_id);
        match std::fs::read(&p) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn delete(&self, partition: u32, record_id: u64) -> Result<(), TopicError> {
        let p = self.path(partition, record_id);
        match std::fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn local_fs_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = LocalFsColdStore::new(tmp.path()).unwrap();
        store.put(3, 42, b"hello cold").unwrap();
        assert_eq!(store.get(3, 42).unwrap().as_deref(), Some(b"hello cold".as_ref()));
        assert!(store.get(3, 999).unwrap().is_none());
        store.delete(3, 42).unwrap();
        assert!(store.get(3, 42).unwrap().is_none());
    }
}
