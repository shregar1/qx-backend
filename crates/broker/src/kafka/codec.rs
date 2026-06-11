//! Kafka wire-protocol primitive codec.
//!
//! All Kafka integers are big-endian. "Compact" variants use unsigned varints
//! for length prefixes (length+1, so 0 encodes null). "Flexible" message
//! versions also carry tagged fields at the end of each struct.

// This is the toolbox every future API handler will reach into; not every
// helper has a caller yet.
#![allow(dead_code)]

use std::io::{Cursor, Read};

use bytes::Bytes;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unexpected end of frame while reading {what}")]
    Eof { what: &'static str },
    #[error("invalid utf-8 in string field")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("varint too long")]
    VarintOverflow,
    #[error("negative length {0} where non-negative required")]
    NegativeLength(i32),
    #[error("invalid bool byte: {0}")]
    InvalidBool(u8),
}

pub type WireResult<T> = std::result::Result<T, WireError>;

/// Read cursor over an in-memory Kafka frame body.
///
/// Two construction modes:
/// - `new(&[u8])`: borrows; the cheap default for callers that don't
///   need refcounted slices (every read that returns `Vec`/`String`).
/// - `with_bytes(&Bytes)`: borrows the slice for cursor logic AND holds
///   a refcount clone, so `read_bytes_slice(n)` can return a `Bytes`
///   view into the original buffer without allocating per call. This
///   is what the Produce decoder uses to skip per-record allocations.
pub struct Reader<'a> {
    cur: Cursor<&'a [u8]>,
    /// When `Some`, `read_bytes_slice` returns refcounted views into
    /// this buffer (the same data the cursor is reading). When `None`,
    /// it falls back to `Bytes::copy_from_slice` so the API works
    /// regardless of how the Reader was constructed.
    owning: Option<Bytes>,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            cur: Cursor::new(buf),
            owning: None,
        }
    }

    /// Construct a Reader that can hand out zero-copy `Bytes` slices.
    /// The Bytes lifetime is borrowed for the cursor but cloned (just
    /// a refcount bump) for `read_bytes_slice` to use later.
    pub fn with_bytes(buf: &'a Bytes) -> Self {
        Self {
            cur: Cursor::new(buf.as_ref()),
            owning: Some(buf.clone()),
        }
    }

    pub fn remaining(&self) -> usize {
        let total = self.cur.get_ref().len() as u64;
        (total - self.cur.position()) as usize
    }

    fn read_exact(&mut self, n: usize, what: &'static str) -> WireResult<Vec<u8>> {
        if self.remaining() < n {
            return Err(WireError::Eof { what });
        }
        let mut buf = vec![0u8; n];
        self.cur.read_exact(&mut buf).map_err(|_| WireError::Eof { what })?;
        Ok(buf)
    }

    /// Compact-encoded byte blob, returning a refcounted slice. Reads
    /// `uvarint(len+1)` then slices `len` bytes via `read_bytes_slice`.
    /// `uvarint(0)` is null. Used by the Produce decoder so partition
    /// record-batch bodies don't allocate.
    pub fn read_compact_bytes_slice(
        &mut self,
        what: &'static str,
    ) -> WireResult<Option<Bytes>> {
        let len_plus_one = self.read_uvarint()?;
        if len_plus_one == 0 {
            return Ok(None);
        }
        let len = (len_plus_one - 1) as usize;
        Ok(Some(self.read_bytes_slice(len, what)?))
    }

    /// Slice `n` bytes out as a refcounted `Bytes`. When the Reader was
    /// constructed via `with_bytes`, this is a refcount bump only — no
    /// allocation, no memcpy. When constructed via `new(&[u8])` (no
    /// owning buffer), it falls back to `Bytes::copy_from_slice`. Use
    /// this in hot-path decoders (record key/value) where the savings
    /// matter; everywhere else, plain `read_raw` is fine.
    pub fn read_bytes_slice(
        &mut self,
        n: usize,
        what: &'static str,
    ) -> WireResult<Bytes> {
        if self.remaining() < n {
            return Err(WireError::Eof { what });
        }
        let pos = self.cur.position() as usize;
        self.cur.set_position((pos + n) as u64);
        match &self.owning {
            Some(b) => Ok(b.slice(pos..pos + n)),
            None => {
                let slice = &self.cur.get_ref()[pos..pos + n];
                Ok(Bytes::copy_from_slice(slice))
            }
        }
    }

    pub fn read_bool(&mut self) -> WireResult<bool> {
        let b = self.read_i8()?;
        match b {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(WireError::InvalidBool(other as u8)),
        }
    }

    pub fn read_i8(&mut self) -> WireResult<i8> {
        let b = self.read_exact(1, "i8")?;
        Ok(b[0] as i8)
    }

    pub fn read_i16(&mut self) -> WireResult<i16> {
        let b = self.read_exact(2, "i16")?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn read_i32(&mut self) -> WireResult<i32> {
        let b = self.read_exact(4, "i32")?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn read_i64(&mut self) -> WireResult<i64> {
        let b = self.read_exact(8, "i64")?;
        Ok(i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    pub fn read_u32(&mut self) -> WireResult<u32> {
        let b = self.read_exact(4, "u32")?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Unsigned varint (LEB128). Up to 5 bytes for u32.
    pub fn read_uvarint(&mut self) -> WireResult<u32> {
        let mut value: u32 = 0;
        let mut shift = 0u32;
        for _ in 0..5 {
            let b = self.read_exact(1, "uvarint")?;
            let byte = b[0];
            value |= ((byte & 0x7f) as u32) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
        Err(WireError::VarintOverflow)
    }

    /// Unsigned varlong (LEB128). Up to 10 bytes for u64.
    pub fn read_uvarlong(&mut self) -> WireResult<u64> {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        for _ in 0..10 {
            let b = self.read_exact(1, "uvarlong")?;
            let byte = b[0];
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
        Err(WireError::VarintOverflow)
    }

    /// Signed zigzag varint (32-bit). Used in Kafka record format for
    /// lengths and deltas.
    pub fn read_varint(&mut self) -> WireResult<i32> {
        let raw = self.read_uvarint()?;
        Ok(((raw >> 1) as i32) ^ -((raw & 1) as i32))
    }

    /// Signed zigzag varlong (64-bit). Used in Kafka record format for
    /// timestamp deltas.
    pub fn read_varlong(&mut self) -> WireResult<i64> {
        let raw = self.read_uvarlong()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    /// Consume exactly `n` bytes and return them. Bounds-checked.
    pub fn read_raw(&mut self, n: usize, what: &'static str) -> WireResult<Vec<u8>> {
        self.read_exact(n, what)
    }

    /// Length-prefixed UTF-8 string. `-1` length means null → `None`.
    pub fn read_nullable_string(&mut self) -> WireResult<Option<String>> {
        let len = self.read_i16()?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.read_exact(len as usize, "string")?;
        Ok(Some(String::from_utf8(bytes)?))
    }

    pub fn read_string(&mut self) -> WireResult<String> {
        self.read_nullable_string()?
            .ok_or(WireError::NegativeLength(-1))
    }

    /// Compact nullable string. Length = uvarint(N+1); 0 → null.
    pub fn read_compact_nullable_string(&mut self) -> WireResult<Option<String>> {
        let raw = self.read_uvarint()?;
        if raw == 0 {
            return Ok(None);
        }
        let len = (raw - 1) as usize;
        let bytes = self.read_exact(len, "compact string")?;
        Ok(Some(String::from_utf8(bytes)?))
    }

    pub fn read_compact_string(&mut self) -> WireResult<String> {
        self.read_compact_nullable_string()?
            .ok_or(WireError::NegativeLength(-1))
    }

    pub fn read_bytes(&mut self) -> WireResult<Option<Vec<u8>>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(self.read_exact(len as usize, "bytes")?))
    }

    pub fn read_compact_bytes(&mut self) -> WireResult<Option<Vec<u8>>> {
        let raw = self.read_uvarint()?;
        if raw == 0 {
            return Ok(None);
        }
        Ok(Some(self.read_exact((raw - 1) as usize, "compact bytes")?))
    }

    /// Tagged fields appendage. Each tag has its own length-prefixed body.
    /// We don't interpret any known tags today; we just skip past them.
    pub fn read_tagged_fields(&mut self) -> WireResult<()> {
        let count = self.read_uvarint()?;
        for _ in 0..count {
            let _tag = self.read_uvarint()?;
            let size = self.read_uvarint()?;
            self.read_exact(size as usize, "tagged field body")?;
        }
        Ok(())
    }
}

/// Write buffer for a Kafka frame body.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self { buf: Vec::with_capacity(n) }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    pub fn write_i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    pub fn write_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_uvarint(&mut self, mut v: u32) {
        while v >= 0x80 {
            self.buf.push(((v & 0x7f) as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    pub fn write_nullable_string(&mut self, s: Option<&str>) {
        match s {
            None => self.write_i16(-1),
            Some(s) => {
                self.write_i16(s.len() as i16);
                self.buf.extend_from_slice(s.as_bytes());
            }
        }
    }

    pub fn write_string(&mut self, s: &str) {
        self.write_nullable_string(Some(s));
    }

    pub fn write_compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            None => self.write_uvarint(0),
            Some(s) => {
                self.write_uvarint(s.len() as u32 + 1);
                self.buf.extend_from_slice(s.as_bytes());
            }
        }
    }

    pub fn write_compact_string(&mut self, s: &str) {
        self.write_compact_nullable_string(Some(s));
    }

    pub fn write_bytes_raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    /// Compact-encoded bytes blob: uvarint(len + 1) then the raw bytes.
    /// `None` writes a single zero byte (the wire encoding for null).
    pub fn write_compact_bytes(&mut self, b: Option<&[u8]>) {
        match b {
            None => self.write_uvarint(0),
            Some(bytes) => {
                self.write_uvarint(bytes.len() as u32 + 1);
                self.buf.extend_from_slice(bytes);
            }
        }
    }

    /// Empty tagged_fields appendage (no tags).
    pub fn write_empty_tagged_fields(&mut self) {
        self.write_uvarint(0);
    }

    /// Compact array length prefix (does NOT write the elements).
    pub fn write_compact_array_len(&mut self, n: u32) {
        self.write_uvarint(n + 1);
    }

    /// Regular array length prefix (does NOT write the elements).
    pub fn write_array_len(&mut self, n: i32) {
        self.write_i32(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_varint_round_trip() {
        for v in [0i32, -1, 1, -2, 2, i32::MAX, i32::MIN, -100, 100] {
            let uv = ((v << 1) ^ (v >> 31)) as u32;
            let mut w = Writer::new();
            w.write_uvarint(uv);
            let buf = w.into_bytes();
            let mut r = Reader::new(&buf);
            assert_eq!(r.read_varint().unwrap(), v);
        }
    }

    #[test]
    fn uvarint_round_trip() {
        for v in [0u32, 1, 127, 128, 16_383, 16_384, 2_097_151, u32::MAX] {
            let mut w = Writer::new();
            w.write_uvarint(v);
            let buf = w.into_bytes();
            let mut r = Reader::new(&buf);
            assert_eq!(r.read_uvarint().unwrap(), v);
            assert_eq!(r.remaining(), 0);
        }
    }

    #[test]
    fn string_round_trip() {
        let mut w = Writer::new();
        w.write_string("kafka");
        w.write_nullable_string(None);
        let buf = w.into_bytes();
        let mut r = Reader::new(&buf);
        assert_eq!(r.read_string().unwrap(), "kafka");
        assert!(r.read_nullable_string().unwrap().is_none());
    }

    #[test]
    fn compact_string_round_trip() {
        let mut w = Writer::new();
        w.write_compact_string("flexible");
        w.write_compact_nullable_string(None);
        let buf = w.into_bytes();
        let mut r = Reader::new(&buf);
        assert_eq!(r.read_compact_string().unwrap(), "flexible");
        assert!(r.read_compact_nullable_string().unwrap().is_none());
    }
}
