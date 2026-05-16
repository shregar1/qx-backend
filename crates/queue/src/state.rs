//! Per-message state model and binary encoding for redb storage.
//!
//! Encoding (little-endian):
//!   [u8 status]
//!   [u32 attempts]
//!   [u64 inflight_until_nanos]
//!   [u32 dedup_key_len][N bytes dedup_key]
//!   [u32 compaction_key_len][N bytes compaction_key]

use qx_storage::RecordId;

use crate::error::{QueueError, Result};

const HEADER_SIZE: usize = 1 + 4 + 8 + 4 + 4;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pending = 0,
    InFlight = 1,
    Dead = 2,
}

impl Status {
    fn from_u8(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Status::Pending),
            1 => Ok(Status::InFlight),
            2 => Ok(Status::Dead),
            _ => Err(QueueError::CorruptState),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageState {
    pub status: Status,
    pub attempts: u32,
    pub inflight_until_nanos: u64,
    pub dedup_key: Option<Vec<u8>>,
    pub compaction_key: Option<Vec<u8>>,
}

impl MessageState {
    pub fn pending() -> Self {
        Self {
            status: Status::Pending,
            attempts: 0,
            inflight_until_nanos: 0,
            dedup_key: None,
            compaction_key: None,
        }
    }

    pub fn pending_with_dedup(key: Vec<u8>) -> Self {
        Self {
            status: Status::Pending,
            attempts: 0,
            inflight_until_nanos: 0,
            dedup_key: Some(key),
            compaction_key: None,
        }
    }

    pub fn pending_with_compaction_key(key: Vec<u8>) -> Self {
        Self {
            status: Status::Pending,
            attempts: 0,
            inflight_until_nanos: 0,
            dedup_key: None,
            compaction_key: Some(key),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let dedup_len = self.dedup_key.as_ref().map_or(0, |k| k.len());
        let compact_len = self.compaction_key.as_ref().map_or(0, |k| k.len());
        let mut buf = Vec::with_capacity(HEADER_SIZE + dedup_len + compact_len);
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

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE - 4 {
            return Err(QueueError::CorruptState);
        }
        let status = Status::from_u8(bytes[0])?;
        let attempts = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let inflight_until_nanos = u64::from_le_bytes(bytes[5..13].try_into().unwrap());

        let dedup_len = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
        let dedup_end = 17 + dedup_len;
        if bytes.len() < dedup_end + 4 {
            return Err(QueueError::CorruptState);
        }
        let dedup_key = if dedup_len > 0 {
            Some(bytes[17..dedup_end].to_vec())
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
            status,
            attempts,
            inflight_until_nanos,
            dedup_key,
            compaction_key,
        })
    }

    pub fn is_claimable(&self, now_nanos: u64) -> bool {
        match self.status {
            Status::Pending => true,
            Status::InFlight => self.inflight_until_nanos <= now_nanos,
            Status::Dead => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReceiptHandle {
    pub record_id: RecordId,
    pub attempt: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_encodes_and_decodes() {
        let s = MessageState::pending();
        let bytes = s.encode();
        let decoded = MessageState::decode(&bytes).unwrap();
        assert_eq!(decoded.status, Status::Pending);
        assert!(decoded.dedup_key.is_none());
        assert!(decoded.compaction_key.is_none());
    }

    #[test]
    fn dedup_key_round_trips() {
        let s = MessageState::pending_with_dedup(b"order-42".to_vec());
        let bytes = s.encode();
        let decoded = MessageState::decode(&bytes).unwrap();
        assert_eq!(decoded.dedup_key.as_deref(), Some(b"order-42".as_slice()));
        assert!(decoded.compaction_key.is_none());
    }

    #[test]
    fn compaction_key_round_trips() {
        let s = MessageState::pending_with_compaction_key(b"entity-7".to_vec());
        let bytes = s.encode();
        let decoded = MessageState::decode(&bytes).unwrap();
        assert!(decoded.dedup_key.is_none());
        assert_eq!(decoded.compaction_key.as_deref(), Some(b"entity-7".as_slice()));
    }

    #[test]
    fn both_keys_round_trip() {
        let mut s = MessageState::pending();
        s.dedup_key = Some(b"d".to_vec());
        s.compaction_key = Some(b"c".to_vec());
        let bytes = s.encode();
        let decoded = MessageState::decode(&bytes).unwrap();
        assert_eq!(decoded.dedup_key.as_deref(), Some(b"d".as_slice()));
        assert_eq!(decoded.compaction_key.as_deref(), Some(b"c".as_slice()));
    }

    #[test]
    fn decode_rejects_short_bytes() {
        assert!(MessageState::decode(&[0u8; 5]).is_err());
    }

    #[test]
    fn decode_rejects_invalid_status() {
        let mut bytes = vec![0u8; HEADER_SIZE];
        bytes[0] = 99;
        assert!(MessageState::decode(&bytes).is_err());
    }

    #[test]
    fn claimable_pending_is_always_claimable() {
        assert!(MessageState::pending().is_claimable(0));
        assert!(MessageState::pending().is_claimable(u64::MAX));
    }

    #[test]
    fn claimable_inflight_only_after_deadline() {
        let s = MessageState {
            status: Status::InFlight,
            attempts: 1,
            inflight_until_nanos: 1000,
            dedup_key: None,
            compaction_key: None,
        };
        assert!(!s.is_claimable(500));
        assert!(s.is_claimable(2000));
    }

    #[test]
    fn claimable_dead_is_never_claimable() {
        let s = MessageState {
            status: Status::Dead,
            attempts: 99,
            inflight_until_nanos: 0,
            dedup_key: None,
            compaction_key: None,
        };
        assert!(!s.is_claimable(u64::MAX));
    }
}
