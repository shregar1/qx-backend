//! Persistent dedup store for Kafka idempotent producers.
//!
//! State per `(producer_id: i64, partition: i32)`:
//!
//! ```text
//! last_base_sequence: i32
//! last_record_count:  i32
//! last_base_offset:   i64
//! ```
//!
//! Stored in a tiny redb file under `data_dir/producer_dedup.redb`. On
//! broker start we warm a HashMap from the on-disk state; on every write
//! we update both the map and the file so a broker restart doesn't lose
//! the sequence picture for retrying producers.

use std::collections::HashMap;
use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use super::ProducerSequenceState;
use crate::error::{BrokerError, Result};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("producer_dedup");

pub struct ProducerDedupStore {
    db: Database,
    cache: std::sync::Mutex<HashMap<(i64, i32), ProducerSequenceState>>,
}

impl ProducerDedupStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("producer_dedup.redb")).map_err(map_err)?;
        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(map_err)?;
            txn.open_table(TABLE).map_err(map_err)?;
            txn.commit().map_err(map_err)?;
        }
        // Warm cache from disk.
        let mut cache = HashMap::new();
        {
            let txn = db.begin_read().map_err(map_err)?;
            let table = txn.open_table(TABLE).map_err(map_err)?;
            for entry in table.iter().map_err(map_err)? {
                let (k, v) = entry.map_err(map_err)?;
                if let (Some(key), Some(state)) = (decode_key(k.value()), decode_state(v.value())) {
                    cache.insert(key, state);
                }
            }
        }
        Ok(Self {
            db,
            cache: std::sync::Mutex::new(cache),
        })
    }

    pub fn get(&self, producer_id: i64, partition: i32) -> Option<ProducerSequenceState> {
        self.cache.lock().unwrap().get(&(producer_id, partition)).copied()
    }

    pub fn put(
        &self,
        producer_id: i64,
        partition: i32,
        state: ProducerSequenceState,
    ) -> Result<()> {
        // Persist before updating the cache so a crash mid-write doesn't
        // leave the cache claiming success the disk hasn't accepted.
        let key = encode_key(producer_id, partition);
        let value = encode_state(&state);
        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut t = txn.open_table(TABLE).map_err(map_err)?;
            t.insert(key.as_slice(), value.as_slice()).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;
        self.cache
            .lock()
            .unwrap()
            .insert((producer_id, partition), state);
        Ok(())
    }
}

fn encode_key(producer_id: i64, partition: i32) -> Vec<u8> {
    let mut k = Vec::with_capacity(12);
    k.extend_from_slice(&producer_id.to_be_bytes());
    k.extend_from_slice(&partition.to_be_bytes());
    k
}

fn decode_key(bytes: &[u8]) -> Option<(i64, i32)> {
    if bytes.len() != 12 {
        return None;
    }
    let producer_id = i64::from_be_bytes(bytes[..8].try_into().ok()?);
    let partition = i32::from_be_bytes(bytes[8..].try_into().ok()?);
    Some((producer_id, partition))
}

fn encode_state(s: &ProducerSequenceState) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&s.last_base_sequence.to_be_bytes());
    v.extend_from_slice(&s.last_record_count.to_be_bytes());
    v.extend_from_slice(&s.last_base_offset.to_be_bytes());
    v
}

fn decode_state(bytes: &[u8]) -> Option<ProducerSequenceState> {
    if bytes.len() != 16 {
        return None;
    }
    Some(ProducerSequenceState {
        last_base_sequence: i32::from_be_bytes(bytes[0..4].try_into().ok()?),
        last_record_count: i32::from_be_bytes(bytes[4..8].try_into().ok()?),
        last_base_offset: i64::from_be_bytes(bytes[8..16].try_into().ok()?),
    })
}

fn map_err<E: std::fmt::Display>(e: E) -> BrokerError {
    BrokerError::Custom(format!("producer_dedup redb: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn round_trip_and_reload() {
        let dir = TempDir::new().unwrap();
        let state = ProducerSequenceState {
            last_base_sequence: 5,
            last_record_count: 3,
            last_base_offset: 42,
        };
        {
            let store = ProducerDedupStore::open(dir.path()).unwrap();
            store.put(100, 0, state).unwrap();
            assert_eq!(store.get(100, 0), Some(state));
        }
        let store = ProducerDedupStore::open(dir.path()).unwrap();
        assert_eq!(store.get(100, 0), Some(state));
        assert_eq!(store.get(100, 1), None);
    }
}
