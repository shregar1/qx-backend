//! Kafka record batch (magic=2) parser.
//!
//! On-wire layout for one batch:
//!
//! ```text
//! base_offset           i64
//! batch_length          i32
//! partition_leader_epoch i32
//! magic                 i8   (must be 2)
//! crc                   u32  (CRC32C of everything after this field)
//! attributes            i16
//! last_offset_delta     i32
//! base_timestamp        i64
//! max_timestamp         i64
//! producer_id           i64
//! producer_epoch        i16
//! base_sequence         i32
//! records               array<Record>          // count is i32
//! ```
//!
//! Each `Record`:
//!
//! ```text
//! length                 varint (zigzag, total record length)
//! attributes             i8
//! timestamp_delta        varlong
//! offset_delta           varint
//! key_length             varint   (-1 = null key)
//! key                    byte[]
//! value_length           varint   (-1 = null value)
//! value                  byte[]
//! header_count           varint
//! headers                [Header]
//! ```
//!
//! Only uncompressed batches are accepted today; compressed batches return
//! a `Compressed` error so the caller can surface the right Kafka error code.

use super::codec::{Reader, WireError, WireResult};

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("record batch wire format: {0}")]
    Wire(#[from] WireError),
    #[error("unsupported record-batch magic {0} (we only accept magic=2)")]
    UnsupportedMagic(i8),
    #[error("crc32c mismatch: header says {expected:#010x}, computed {computed:#010x}")]
    BadCrc { expected: u32, computed: u32 },
    #[error("unsupported compression codec {0} (gzip/zstd not yet implemented)")]
    UnsupportedCompression(u8),
    #[error("decompression failed ({codec}): {message}")]
    Decompress { codec: &'static str, message: String },
    #[error("batch length {batch_length} doesn't match remaining {remaining}")]
    LengthMismatch { batch_length: i32, remaining: usize },
}

pub type BatchResult<T> = std::result::Result<T, BatchError>;

#[derive(Debug, Clone)]
pub struct DecodedRecord {
    #[allow(dead_code)] // key routing not implemented yet (Kafka client picks the partition)
    pub key: Option<bytes::Bytes>,
    /// Reference-counted slice. Cloning is a refcount bump; passing
    /// this through to the writer task's mpsc avoids the memcpy that
    /// a `Vec<u8>` would incur on every clone or partial move.
    pub value: Option<bytes::Bytes>,
    /// Headers as `(key, optional value)` pairs. Empty for the common path
    /// where Kafka producers send no per-record headers.
    #[allow(dead_code)] // headers will be surfaced once Fetch is implemented
    pub headers: Vec<(String, Option<bytes::Bytes>)>,
}

#[derive(Debug)]
pub struct DecodedBatch {
    // Fields below are parsed off the wire but not yet read — they're the
    // hooks for idempotent/transactional producer support in a follow-up.
    #[allow(dead_code)]
    pub base_offset: i64,
    #[allow(dead_code)]
    pub producer_id: i64,
    #[allow(dead_code)]
    pub producer_epoch: i16,
    #[allow(dead_code)]
    pub base_sequence: i32,
    #[allow(dead_code)]
    pub is_transactional: bool,
    pub records: Vec<DecodedRecord>,
}

/// Decode one record batch starting at `buf[0]`. Returns the decoded batch
/// and the number of bytes consumed (so the caller can advance through a
/// stream of multiple batches if the producer sent more than one).
///
/// `buf` is an owning `Bytes` — record key/value slices in the returned
/// `DecodedBatch` are refcounted views into it. No per-record allocation
/// on the uncompressed path.
pub fn decode_batch(buf: bytes::Bytes) -> BatchResult<(DecodedBatch, usize)> {
    let mut r = Reader::with_bytes(&buf);
    let base_offset = r.read_i64()?;
    let batch_length = r.read_i32()?;
    let _partition_leader_epoch = r.read_i32()?;
    let magic = r.read_i8()?;
    if magic != 2 {
        return Err(BatchError::UnsupportedMagic(magic));
    }
    let stored_crc = r.read_u32()?;

    // CRC32C covers everything from `attributes` onward. Compute it over
    // the remaining batch bytes (batch_length counts everything *after*
    // the `batch_length` field itself: leader_epoch+magic+crc = 9 bytes
    // already consumed, so the body length is batch_length - 9).
    let body_len = (batch_length as usize)
        .checked_sub(4 + 1 + 4) // leader_epoch + magic + crc
        .ok_or(BatchError::LengthMismatch {
            batch_length,
            remaining: r.remaining(),
        })?;

    if body_len > r.remaining() {
        return Err(BatchError::LengthMismatch {
            batch_length,
            remaining: r.remaining(),
        });
    }

    let body_start = buf.len() - r.remaining();
    // Slice the body as a refcounted `Bytes` view — no copy. The inner
    // Reader we build from this can hand out zero-copy slices for
    // record keys/values when the batch is uncompressed.
    let body = buf.slice(body_start..body_start + body_len);
    let computed_crc = crc32c::crc32c(body.as_ref());
    if computed_crc != stored_crc {
        return Err(BatchError::BadCrc {
            expected: stored_crc,
            computed: computed_crc,
        });
    }

    // The body slice is exactly `body_len` bytes. Consume it from the
    // outer reader so the caller's `consumed` count is accurate even when
    // the producer concatenated multiple batches.
    let _ = r.read_raw(body_len, "batch body")?;

    // Parse the body. Fields up through `base_sequence` are always
    // plain-text; only the records section may be compressed.
    let mut br = Reader::with_bytes(&body);
    let attributes = br.read_i16()?;
    let compression = (attributes & 0x07) as u8;
    let is_transactional = (attributes & 0x10) != 0;
    let _last_offset_delta = br.read_i32()?;
    let _base_timestamp = br.read_i64()?;
    let _max_timestamp = br.read_i64()?;
    let producer_id = br.read_i64()?;
    let producer_epoch = br.read_i16()?;
    let base_sequence = br.read_i32()?;

    // The remainder of `body` is the records section, possibly compressed.
    let records_section_offset = body.len() - br.remaining();
    let records_section = body.slice(records_section_offset..);

    // Uncompressed case is the hot path — keep the body's refcount,
    // hand out zero-copy slices. Compressed cases allocate the
    // decompressed buffer once and wrap it in Bytes; record slices
    // off that decompressed buffer are still refcounted (no further
    // per-record alloc).
    let records_bytes: bytes::Bytes = match compression {
        0 => records_section,
        1 => {
            use std::io::Read;
            let mut decoder = flate2::read::GzDecoder::new(records_section.as_ref());
            let mut out = Vec::with_capacity(records_section.len() * 2);
            decoder.read_to_end(&mut out).map_err(|e| BatchError::Decompress {
                codec: "gzip",
                message: e.to_string(),
            })?;
            bytes::Bytes::from(out)
        }
        2 => {
            let out = snap::raw::Decoder::new()
                .decompress_vec(records_section.as_ref())
                .map_err(|e| BatchError::Decompress {
                    codec: "snappy",
                    message: e.to_string(),
                })?;
            bytes::Bytes::from(out)
        }
        3 => {
            use std::io::Read;
            let mut decoder = lz4_flex::frame::FrameDecoder::new(records_section.as_ref());
            let mut out = Vec::with_capacity(records_section.len() * 2);
            decoder.read_to_end(&mut out).map_err(|e| BatchError::Decompress {
                codec: "lz4",
                message: e.to_string(),
            })?;
            bytes::Bytes::from(out)
        }
        4 => {
            let out = zstd::decode_all(records_section.as_ref()).map_err(|e| {
                BatchError::Decompress {
                    codec: "zstd",
                    message: e.to_string(),
                }
            })?;
            bytes::Bytes::from(out)
        }
        other => return Err(BatchError::UnsupportedCompression(other)),
    };

    let mut rr = Reader::with_bytes(&records_bytes);
    let record_count = rr.read_i32()?;
    if record_count < 0 {
        return Err(BatchError::Wire(WireError::NegativeLength(record_count)));
    }
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(decode_record(&mut rr)?);
    }

    let consumed = buf.len() - r.remaining();
    Ok((
        DecodedBatch {
            base_offset,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            records,
        },
        consumed,
    ))
}

fn decode_record(r: &mut Reader<'_>) -> BatchResult<DecodedRecord> {
    let _length = r.read_varint()?;
    let _attributes = r.read_i8()?;
    let _timestamp_delta = r.read_varlong()?;
    let _offset_delta = r.read_varint()?;

    let key_len = r.read_varint()?;
    let key = if key_len < 0 {
        None
    } else {
        // Zero-copy slice into the batch body when the Reader was
        // constructed with a Bytes (the standard Produce path); falls
        // back to Bytes::copy_from_slice otherwise.
        Some(r.read_bytes_slice(key_len as usize, "record key")?)
    };

    let value_len = r.read_varint()?;
    let value = if value_len < 0 {
        None
    } else {
        Some(r.read_bytes_slice(value_len as usize, "record value")?)
    };

    let header_count = r.read_varint()?;
    if header_count < 0 {
        return Err(BatchError::Wire(WireError::NegativeLength(header_count)));
    }
    let mut headers = Vec::with_capacity(header_count as usize);
    for _ in 0..header_count {
        let hk_len = r.read_varint()?;
        let hk_bytes = if hk_len < 0 {
            vec![]
        } else {
            r.read_raw(hk_len as usize, "header key")?
        };
        let hk = String::from_utf8(hk_bytes)
            .map_err(|e| BatchError::Wire(WireError::Utf8(e)))?;
        let hv_len = r.read_varint()?;
        let hv = if hv_len < 0 {
            None
        } else {
            Some(r.read_bytes_slice(hv_len as usize, "header value")?)
        };
        headers.push((hk, hv));
    }

    Ok(DecodedRecord { key, value, headers })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::codec::Writer;

    /// Builds a minimal valid magic=2 batch carrying `records.len()` records
    /// with the given key/value pairs and no headers.
    fn encode_simple_batch(records: &[(&[u8], &[u8])]) -> Vec<u8> {
        // Body: everything after the crc field.
        let mut body = Writer::new();
        body.write_i16(0); // attributes
        body.write_i32((records.len() as i32) - 1); // last_offset_delta
        body.write_i64(0); // base_timestamp
        body.write_i64(0); // max_timestamp
        body.write_i64(-1); // producer_id
        body.write_i16(-1); // producer_epoch
        body.write_i32(-1); // base_sequence
        body.write_i32(records.len() as i32);

        for (i, (k, v)) in records.iter().enumerate() {
            let mut rec = Writer::new();
            rec.write_i8(0); // attributes
            write_zigzag_varlong(&mut rec, 0); // timestamp_delta
            write_zigzag_varint(&mut rec, i as i32); // offset_delta
            write_zigzag_varint(&mut rec, k.len() as i32);
            rec.write_bytes_raw(k);
            write_zigzag_varint(&mut rec, v.len() as i32);
            rec.write_bytes_raw(v);
            write_zigzag_varint(&mut rec, 0); // header count
            let rec_bytes = rec.into_bytes();
            write_zigzag_varint(&mut body, rec_bytes.len() as i32);
            body.write_bytes_raw(&rec_bytes);
        }

        let body_bytes = body.into_bytes();
        let crc = crc32c::crc32c(&body_bytes);

        let mut out = Writer::new();
        out.write_i64(0); // base_offset
        out.write_i32((4 + 1 + 4 + body_bytes.len()) as i32); // batch_length
        out.write_i32(0); // partition_leader_epoch
        out.write_i8(2); // magic
        out.write_u32(crc);
        out.write_bytes_raw(&body_bytes);
        out.into_bytes()
    }

    fn write_zigzag_varint(w: &mut Writer, v: i32) {
        let u = ((v << 1) ^ (v >> 31)) as u32;
        w.write_uvarint(u);
    }

    fn write_zigzag_varlong(w: &mut Writer, v: i64) {
        let u = ((v << 1) ^ (v >> 63)) as u64;
        // Hand-encode u64 LEB128 since Writer only exposes write_uvarint(u32).
        let mut v = u;
        while v >= 0x80 {
            w.write_i8((((v & 0x7f) as u8) | 0x80) as i8);
            v >>= 7;
        }
        w.write_i8(v as i8);
    }

    #[test]
    fn decode_single_record() {
        let buf = encode_simple_batch(&[(b"k1", b"v1")]);
        let (batch, used) = decode_batch(bytes::Bytes::from(buf.clone())).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].key.as_deref(), Some(b"k1".as_ref()));
        assert_eq!(batch.records[0].value.as_deref(), Some(b"v1".as_ref()));
    }

    #[test]
    fn decode_multiple_records() {
        let buf = encode_simple_batch(&[(b"k1", b"v1"), (b"", b"v2"), (b"k3", b"")]);
        let (batch, _) = decode_batch(bytes::Bytes::from(buf.clone())).unwrap();
        assert_eq!(batch.records.len(), 3);
        assert_eq!(batch.records[2].key.as_deref(), Some(b"k3".as_ref()));
    }

    #[test]
    fn rejects_bad_crc() {
        let mut buf = encode_simple_batch(&[(b"k", b"v")]);
        // Corrupt the body, leaving crc untouched.
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        let err = decode_batch(bytes::Bytes::from(buf.clone())).unwrap_err();
        assert!(matches!(err, BatchError::BadCrc { .. }), "got {err:?}");
    }

    /// Build a magic=2 batch with `compression_code` set in attributes and
    /// the records section already compressed by `compress`.
    fn encode_compressed_batch(
        records: &[(&[u8], &[u8])],
        compression_code: u8,
        compress: impl FnOnce(&[u8]) -> Vec<u8>,
    ) -> Vec<u8> {
        let mut records_section = Writer::new();
        records_section.write_i32(records.len() as i32);
        for (i, (k, v)) in records.iter().enumerate() {
            let mut rec = Writer::new();
            rec.write_i8(0);
            write_zigzag_varlong(&mut rec, 0);
            write_zigzag_varint(&mut rec, i as i32);
            write_zigzag_varint(&mut rec, k.len() as i32);
            rec.write_bytes_raw(k);
            write_zigzag_varint(&mut rec, v.len() as i32);
            rec.write_bytes_raw(v);
            write_zigzag_varint(&mut rec, 0);
            let rec_bytes = rec.into_bytes();
            write_zigzag_varint(&mut records_section, rec_bytes.len() as i32);
            records_section.write_bytes_raw(&rec_bytes);
        }
        let compressed = compress(&records_section.into_bytes());

        let mut body = Writer::new();
        body.write_i16(compression_code as i16); // attributes: low 3 bits = codec
        body.write_i32((records.len() as i32) - 1);
        body.write_i64(0);
        body.write_i64(0);
        body.write_i64(-1);
        body.write_i16(-1);
        body.write_i32(-1);
        body.write_bytes_raw(&compressed);
        let body_bytes = body.into_bytes();
        let crc = crc32c::crc32c(&body_bytes);

        let mut out = Writer::new();
        out.write_i64(0);
        out.write_i32((4 + 1 + 4 + body_bytes.len()) as i32);
        out.write_i32(0);
        out.write_i8(2);
        out.write_u32(crc);
        out.write_bytes_raw(&body_bytes);
        out.into_bytes()
    }

    #[test]
    fn decode_snappy_compressed() {
        let buf = encode_compressed_batch(
            &[(b"k", b"hello"), (b"", b"world")],
            2,
            |raw| snap::raw::Encoder::new().compress_vec(raw).unwrap(),
        );
        let (batch, _) = decode_batch(bytes::Bytes::from(buf.clone())).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].value.as_deref(), Some(b"hello".as_ref()));
        assert_eq!(batch.records[1].value.as_deref(), Some(b"world".as_ref()));
    }

    #[test]
    fn decode_lz4_frame_compressed() {
        let buf = encode_compressed_batch(
            &[(b"x", b"alpha"), (b"y", b"beta")],
            3,
            |raw| {
                use std::io::Write;
                let mut out = Vec::new();
                let mut enc = lz4_flex::frame::FrameEncoder::new(&mut out);
                enc.write_all(raw).unwrap();
                enc.finish().unwrap();
                out
            },
        );
        let (batch, _) = decode_batch(bytes::Bytes::from(buf.clone())).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].value.as_deref(), Some(b"alpha".as_ref()));
        assert_eq!(batch.records[1].value.as_deref(), Some(b"beta".as_ref()));
    }

    #[test]
    fn decode_gzip_compressed() {
        let buf = encode_compressed_batch(&[(b"g", b"gzipped")], 1, |raw| {
            use std::io::Write;
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(raw).unwrap();
            enc.finish().unwrap()
        });
        let (batch, _) = decode_batch(bytes::Bytes::from(buf.clone())).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].value.as_deref(), Some(b"gzipped".as_ref()));
    }

    #[test]
    fn decode_zstd_compressed() {
        let buf = encode_compressed_batch(&[(b"z", b"zstd-rocks")], 4, |raw| {
            zstd::encode_all(raw, 3).unwrap()
        });
        let (batch, _) = decode_batch(bytes::Bytes::from(buf.clone())).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].value.as_deref(), Some(b"zstd-rocks".as_ref()));
    }
}
