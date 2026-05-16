//! A single durable append-only segment file.
//!
//! `Offset` is the byte position of a record's start. Reads are O(1)
//! given an offset; record framing lives in `crate::frame`.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::frame::{self, HEADER_SIZE, Header};

pub type Offset = u64;

pub struct Record {
    payload: Vec<u8>,
}

impl Record {
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

const READ_PREFETCH: usize = 4096;

pub struct Segment {
    path: PathBuf,
    file: File,
    write_pos: u64,
    record_count: u64,
    /// Reusable buffer for encoding records before writing — avoids one heap
    /// allocation per append/append_batch call.
    write_buf: Vec<u8>,
}

impl Segment {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            write_pos: 0,
            record_count: 0,
            write_buf: Vec::new(),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        let file_len = file.metadata()?.len();

        let mut count: u64 = 0;
        for offset in OffsetIter::new(&file, file_len) {
            offset?;
            count += 1;
        }

        Ok(Self {
            path,
            file,
            write_pos: file_len,
            record_count: count,
            write_buf: Vec::new(),
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Offset> {
        let offset = self.write_pos;
        self.write_buf.clear();
        frame::encode_into(&mut self.write_buf, payload)?;
        self.file.write_all(&self.write_buf)?;
        self.write_pos += self.write_buf.len() as u64;
        self.record_count += 1;
        Ok(offset)
    }

    /// Read using a prefetched single-pread. For records whose total size
    /// (header + payload) fits in `READ_PREFETCH` we issue exactly one
    /// `pread`; larger records fall back to a second read for the tail.
    pub fn read(&self, offset: Offset) -> Result<Record> {
        if offset >= self.write_pos {
            return Err(StorageError::UnexpectedEof { offset });
        }

        let mut buf = vec![0u8; READ_PREFETCH];
        let n = self
            .file
            .read_at(&mut buf, offset)
            .map_err(|_| StorageError::UnexpectedEof { offset })?;
        if n < HEADER_SIZE {
            return Err(StorageError::UnexpectedEof { offset });
        }

        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        let header = Header::decode(header_bytes);
        let total_size = HEADER_SIZE + header.payload_len;

        let payload = if total_size <= n {
            buf[HEADER_SIZE..total_size].to_vec()
        } else {
            let mut payload = vec![0u8; header.payload_len];
            let already_have = n - HEADER_SIZE;
            payload[..already_have].copy_from_slice(&buf[HEADER_SIZE..n]);
            self.file
                .read_exact_at(
                    &mut payload[already_have..],
                    offset + n as u64,
                )
                .map_err(|_| StorageError::UnexpectedEof { offset })?;
            payload
        };

        if !frame::verify(header.crc, &payload) {
            return Err(StorageError::Corrupt { offset });
        }

        Ok(Record { payload })
    }

    pub fn iter_offsets(&self) -> OffsetIter<'_> {
        OffsetIter::new(&self.file, self.write_pos)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.record_count
    }

    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    pub fn size_bytes(&self) -> u64 {
        self.write_pos
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the segment file to `byte_len` bytes. Caller is responsible
    /// for ensuring `byte_len` falls on a frame boundary — passing a value
    /// mid-frame will leave the segment unreadable past `byte_len`.
    pub fn truncate_to(&mut self, byte_len: u64) -> Result<()> {
        if byte_len > self.write_pos {
            return Ok(());
        }
        self.file.set_len(byte_len)?;
        self.file.sync_all()?;
        self.write_pos = byte_len;
        Ok(())
    }

    pub fn append_batch(&mut self, payloads: &[&[u8]]) -> Result<Vec<Offset>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        self.write_buf.clear();
        let mut offsets = Vec::with_capacity(payloads.len());
        let mut next_offset = self.write_pos;
        for payload in payloads {
            offsets.push(next_offset);
            frame::encode_into(&mut self.write_buf, payload)?;
            next_offset += (HEADER_SIZE + payload.len()) as u64;
        }
        self.file.write_all(&self.write_buf)?;
        self.write_pos = next_offset;
        self.record_count += payloads.len() as u64;
        Ok(offsets)
    }
}

fn read_header(file: &File, offset: u64) -> Result<Header> {
    let mut bytes = [0u8; HEADER_SIZE];
    file.read_exact_at(&mut bytes, offset)
        .map_err(|_| StorageError::UnexpectedEof { offset })?;
    Ok(Header::decode(bytes))
}

pub struct OffsetIter<'a> {
    file: &'a File,
    pos: u64,
    end: u64,
}

impl<'a> OffsetIter<'a> {
    fn new(file: &'a File, end: u64) -> Self {
        Self { file, pos: 0, end }
    }
}

impl<'a> Iterator for OffsetIter<'a> {
    type Item = Result<Offset>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.end {
            return None;
        }
        let offset = self.pos;
        let header = match read_header(self.file, offset) {
            Ok(h) => h,
            Err(e) => return Some(Err(e)),
        };
        let next = offset + header.frame_size();
        if next > self.end {
            return Some(Err(StorageError::UnexpectedEof { offset }));
        }
        self.pos = next;
        Some(Ok(offset))
    }
}
