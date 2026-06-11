//! A single durable append-only segment file.
//!
//! `Offset` is the byte position of a record's start. Reads are O(1)
//! given an offset; record framing lives in `crate::frame`.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::error::{Result, StorageError};
use crate::frame::{self, HEADER_SIZE, Header};

/// Backing buffer for record reads on the mmap fast path. We build
/// the [`bytes::Bytes`] once via [`bytes::Bytes::from_owner`] (which
/// takes the `Mmap` as the owning storage) and then hand out cheap
/// `slice()` views into it. Cloning a `Bytes` is a refcount bump.

pub type Offset = u64;

pub struct Record {
    payload: bytes::Bytes,
}

impl Record {
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_payload(self) -> bytes::Bytes {
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
    /// mmap of the file as it existed at the moment the segment last
    /// "froze" the mmap window (typically: open + every refresh
    /// triggered by a read past the previous window). Reads inside
    /// the window return `Bytes::from_owner(mmap)` — a refcount bump
    /// onto the mmap, no `Vec<u8>` allocation and no `read_at`
    /// syscall. Reads past the window fall through to the slow
    /// `read_at` path. The `RwLock` lets all readers share the mmap
    /// while the very rare "growth" reader installs a new one.
    mmap: RwLock<Option<MmapWindow>>,
}

#[derive(Clone)]
struct MmapWindow {
    /// The full mmap wrapped as a single `Bytes`. Sub-slices are
    /// produced via `Bytes::slice` — both views share the underlying
    /// mmap through the bytes refcount, so the mapping stays alive as
    /// long as any consumer holds a returned payload.
    mmap_bytes: bytes::Bytes,
    /// Length the file had when this mmap was created. Reads at
    /// offsets `< covers` use the mmap; reads past it skip the mmap.
    covers: u64,
}

impl MmapWindow {
    fn new(mmap: memmap2::Mmap, covers: u64) -> Self {
        Self {
            // `from_owner` consumes the `Mmap` and refcounts it via
            // the returned Bytes. The mapping is unmapped only when
            // the last Bytes referencing it is dropped.
            mmap_bytes: bytes::Bytes::from_owner(mmap),
            covers,
        }
    }
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
            mmap: RwLock::new(None),
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

        let mmap = if file_len == 0 {
            None
        } else {
            // SAFETY: we hold the only `File` handle that writes to
            // this path inside the process (every other reader opens
            // via this `Segment`). External mutations would violate
            // the mmap's UnsafeCell story; we document the same
            // append-only constraint that already applied without
            // mmap.
            unsafe { memmap2::MmapOptions::new().map(&file) }
                .ok()
                .map(|m| MmapWindow::new(m, file_len))
        };

        Ok(Self {
            path,
            file,
            write_pos: file_len,
            record_count: count,
            write_buf: Vec::new(),
            mmap: RwLock::new(mmap),
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

    /// Read at `offset`. Fast path: when the record fits inside the
    /// segment's mmap window, the returned [`Record`]'s payload is a
    /// [`bytes::Bytes`] view directly into the mmap — zero `Vec<u8>`
    /// allocation, zero `pread` syscall. Slow path: records whose
    /// frame extends past the mmap window (the active segment grew
    /// after the window was installed) fall through to a single
    /// `pread` into a heap buffer, wrapped via `Bytes::from(vec)`.
    pub fn read(&self, offset: Offset) -> Result<Record> {
        if offset >= self.write_pos {
            return Err(StorageError::UnexpectedEof { offset });
        }

        // mmap fast path. The `read` lock is held for the syscall-
        // free duration of the read — overlapping readers don't
        // contend, and writers (`extend_mmap_to_covering`) only
        // take it long enough to install a new window.
        if let Some(window) = self.mmap.read().expect("segment mmap rwlock poisoned").clone()
            && let Some(record) =
                read_from_mmap(&window, offset, &self.file)?
        {
            return Ok(record);
        }

        // Slow path — used for records past the current mmap window
        // (active segment that grew). Try to grow the mmap once and
        // retry; if growth fails we fall through to the pread path.
        if self.extend_mmap_if_needed(offset) {
            if let Some(window) = self.mmap.read().expect("segment mmap rwlock poisoned").clone()
                && let Some(record) =
                    read_from_mmap(&window, offset, &self.file)?
            {
                return Ok(record);
            }
        }

        // Final fallback: the original `pread` + heap buffer path.
        // Reached only when mmap cannot cover this offset (e.g.,
        // segment that was just appended to and we haven't refreshed
        // the mmap window yet).
        read_via_pread(&self.file, offset)
    }

    fn extend_mmap_if_needed(&self, required_through: u64) -> bool {
        // Re-read the file length under a `write` lock so concurrent
        // readers see a consistent view.
        let new_len = match self.file.metadata() {
            Ok(m) => m.len(),
            Err(_) => return false,
        };
        if new_len <= required_through {
            return false;
        }
        // SAFETY: append-only file, see comments on `open()` / `create()`.
        let m = match unsafe { memmap2::MmapOptions::new().map(&self.file) } {
            Ok(m) => m,
            Err(_) => return false,
        };
        let mut w = self.mmap.write().expect("segment mmap rwlock poisoned");
        *w = Some(MmapWindow::new(m, new_len));
        true
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
        // I tried switching to `write_vectored` here (split headers
        // and payloads into 2N IoSlices, skip the per-record
        // `extend_from_slice(payload)` memcpy). On macOS this
        // regressed ~30% in the produce-profile bench at 100 records
        // × 1 KiB — the per-iovec syscall overhead at 200 iovecs
        // outweighs the ~10 µs of avoided memcpy. The single-buffer
        // write_all path wins on this kernel + drain shape; leave it.
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

/// Try to read a record straight out of `window`. Returns:
///   - `Ok(Some(record))` — the record fit in the mmap and was
///     verified against its CRC.
///   - `Ok(None)` — the record's frame extends past `window.covers`.
///     The caller should grow the mmap (or fall back to pread).
fn read_from_mmap(
    window: &MmapWindow,
    offset: u64,
    _file: &File,
) -> Result<Option<Record>> {
    let off = offset as usize;
    let covers = window.covers as usize;
    if off + HEADER_SIZE > covers {
        return Ok(None);
    }
    let mmap_slice: &[u8] = &window.mmap_bytes;
    let mut header_bytes = [0u8; HEADER_SIZE];
    header_bytes.copy_from_slice(&mmap_slice[off..off + HEADER_SIZE]);
    let header = Header::decode(header_bytes);
    let total = HEADER_SIZE + header.payload_len;
    if off + total > covers {
        return Ok(None);
    }
    let payload_range = (off + HEADER_SIZE)..(off + total);
    if !frame::verify(header.crc, &mmap_slice[payload_range.clone()]) {
        return Err(StorageError::Corrupt { offset });
    }
    // `Bytes::slice` is a refcount-bump view into the existing mmap
    // Bytes — no `Vec<u8>` allocation, no memcpy. The mapping is
    // kept alive as long as any returned payload is held.
    let bytes = window.mmap_bytes.slice(payload_range);
    Ok(Some(Record { payload: bytes }))
}

/// Heap-buffer fallback used when the mmap window doesn't cover the
/// requested record. Same shape as the pre-mmap implementation.
fn read_via_pread(file: &File, offset: u64) -> Result<Record> {
    let mut buf = vec![0u8; READ_PREFETCH];
    let n = file
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
        file.read_exact_at(&mut payload[already_have..], offset + n as u64)
            .map_err(|_| StorageError::UnexpectedEof { offset })?;
        payload
    };
    if !frame::verify(header.crc, &payload) {
        return Err(StorageError::Corrupt { offset });
    }
    Ok(Record {
        payload: bytes::Bytes::from(payload),
    })
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
