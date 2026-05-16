//! On-disk record framing. Owns the binary layout so `Segment` never
//! touches raw bytes — swapping framing (e.g. adding a timestamp) is
//! a change confined to this module.
//!
//! Layout (little-endian):
//!   [u32 payload_len][u32 crc32 of payload][payload bytes]

use crate::error::{Result, StorageError};

pub const HEADER_SIZE: usize = 8;
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

pub struct Header {
    pub payload_len: usize,
    pub crc: u32,
}

impl Header {
    pub fn decode(bytes: [u8; HEADER_SIZE]) -> Self {
        Self {
            payload_len: u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize,
            crc: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        }
    }

    pub fn frame_size(&self) -> u64 {
        (HEADER_SIZE + self.payload_len) as u64
    }
}

pub fn encode_into(buf: &mut Vec<u8>, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_PAYLOAD {
        return Err(StorageError::PayloadTooLarge {
            size: payload.len(),
            max: MAX_PAYLOAD,
        });
    }
    let len = payload.len() as u32;
    let crc = crc32fast::hash(payload);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(payload);
    Ok(())
}

pub fn verify(crc: u32, payload: &[u8]) -> bool {
    crc32fast::hash(payload) == crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(payload: &[u8]) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
        encode_into(&mut buf, payload)?;
        Ok(buf)
    }

    #[test]
    fn encode_then_decode_header_roundtrip() {
        let bytes = encode(b"hello").unwrap();
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&bytes[..HEADER_SIZE]);
        let header = Header::decode(header_bytes);
        assert_eq!(header.payload_len, 5);
        assert_eq!(header.frame_size(), HEADER_SIZE as u64 + 5);
        assert!(verify(header.crc, b"hello"));
        assert!(!verify(header.crc, b"world"));
    }

    #[test]
    fn empty_payload_encodes() {
        let bytes = encode(b"").unwrap();
        assert_eq!(bytes.len(), HEADER_SIZE);
    }

    #[test]
    fn oversized_payload_rejected() {
        let huge = vec![0u8; MAX_PAYLOAD + 1];
        assert!(matches!(
            encode(&huge),
            Err(StorageError::PayloadTooLarge { .. })
        ));
    }
}
