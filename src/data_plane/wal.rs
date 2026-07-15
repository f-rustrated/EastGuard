use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::PathBuf;

use bytes::Bytes;

const DEFAULT_MAX_FILE_SIZE: u64 = 64 * 1024 * 1024; // 64MB
const HEADER_SIZE: usize = 13; // crc32(4) + type(1) + record_count(4) + length(4)
const TRAILING_LENGTH_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum WalRecordType {
    Data = 0,
    BatchEnd = 1,
    ConsumerOffset = 2,
}

impl WalRecordType {
    fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0 => Ok(WalRecordType::Data),
            1 => Ok(WalRecordType::BatchEnd),
            2 => Ok(WalRecordType::ConsumerOffset),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown record type",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalRecord {
    pub record_type: WalRecordType,
    /// Number of logical records batched into `payload`. Persisted so a cold
    /// read of a sealed segment file can rebuild the wire `Entry.record_count`
    /// without the in-memory `CachedEntry`
    pub record_count: u32,
    pub payload: Bytes,
}

impl WalRecord {
    pub(crate) fn data(payload: Bytes, record_count: u32) -> Self {
        WalRecord {
            record_type: WalRecordType::Data,
            record_count,
            payload,
        }
    }

    pub(crate) fn batch_end() -> Self {
        WalRecord {
            record_type: WalRecordType::BatchEnd,
            record_count: 0,
            payload: Bytes::new(),
        }
    }

    pub(crate) fn consumer_offset(payload: Bytes) -> Self {
        WalRecord {
            record_type: WalRecordType::ConsumerOffset,
            record_count: 0,
            payload,
        }
    }

    pub(crate) fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.payload.len() + TRAILING_LENGTH_SIZE
    }

    pub(crate) fn encode_to(&self, writer: &mut impl Write) -> io::Result<()> {
        let payload_len = self.payload.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[self.record_type as u8]);
        hasher.update(&self.record_count.to_be_bytes());
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&self.payload);
        let crc = hasher.finalize();

        writer.write_all(&crc.to_be_bytes())?;
        writer.write_all(&[self.record_type as u8])?;
        writer.write_all(&self.record_count.to_be_bytes())?;
        writer.write_all(&payload_len.to_be_bytes())?;
        writer.write_all(&self.payload)?;
        writer.write_all(&payload_len.to_be_bytes())?;
        Ok(())
    }

    pub(crate) fn decode_from(reader: &mut impl Read) -> io::Result<Self> {
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header)?;

        let stored_crc = u32::from_be_bytes(header[0..4].try_into().unwrap());
        let record_type = WalRecordType::from_u8(header[4])?;
        let record_count = u32::from_be_bytes(header[5..9].try_into().unwrap());
        let payload_len = u32::from_be_bytes(header[9..13].try_into().unwrap()) as usize;

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;

        let mut trailing_len = [0u8; TRAILING_LENGTH_SIZE];
        reader.read_exact(&mut trailing_len)?;
        let trailing = u32::from_be_bytes(trailing_len);
        if trailing as usize != payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing length mismatch",
            ));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type as u8]);
        hasher.update(&record_count.to_be_bytes());
        hasher.update(&(payload_len as u32).to_be_bytes());
        hasher.update(&payload);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        Ok(WalRecord {
            record_type,
            record_count,
            payload: Bytes::from(payload),
        })
    }
}

/// Parses `wal-{seq:06}.log` into its sequence number. Shared by the live
/// writer's directory scan and crash recovery's discovery (`recovery::wal_scan`).
pub(crate) fn parse_wal_filename(name: &str) -> Option<u64> {
    name.strip_prefix("wal-")?
        .strip_suffix(".log")?
        .parse::<u64>()
        .ok()
}

pub trait WalStorage {
    fn buf(&mut self) -> &mut Vec<u8>;
    fn clear_buf(&mut self);
    fn next_lsn(&self) -> u64;
    fn flush_batch(&mut self) -> io::Result<u64>;
    fn maybe_rotate(&mut self) -> io::Result<()>;
    fn delete_below(&mut self, watermark_lsn: u64);
    /// Highest LSN in an inactive WAL file, if rotation has made any file
    /// eligible for reclamation. The active file is never reclaimable.
    fn reclaimable_lsn(&self) -> Option<u64>;

    #[cfg(any(test, debug_assertions))]
    fn assert_invariants(&self);
}

pub struct WalWriter {
    files: VecDeque<WalFileEntry>,
    max_file_size: u64,
    next_lsn: u64,
    data_dir: PathBuf,
    next_seq: u64,
    active_writer: Option<BufWriter<File>>,
    batch_buf: Vec<u8>,
}

struct WalFileEntry {
    seq: u64,
    min_lsn: u64,
    max_lsn: u64,
    size: u64,
    path: PathBuf,
}

impl WalWriter {
    pub(crate) fn new(data_dir: PathBuf) -> io::Result<Self> {
        let wal_dir = data_dir.join("wal");
        fs::create_dir_all(&wal_dir)?;

        let mut writer = WalWriter {
            files: VecDeque::new(),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            next_lsn: 1,
            data_dir,
            next_seq: 1,
            active_writer: None,
            batch_buf: Vec::new(),
        };
        writer.open_new_file()?;
        Ok(writer)
    }

    fn open_new_file(&mut self) -> io::Result<()> {
        let seq = self.next_seq;
        let path = self.data_dir.join("wal").join(format!("wal-{seq:06}.log"));

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        self.files.push_back(WalFileEntry {
            seq,
            min_lsn: self.next_lsn,
            max_lsn: self.next_lsn,
            size: 0,
            path,
        });
        self.next_seq += 1;
        self.active_writer = Some(BufWriter::new(file));
        Ok(())
    }

    fn active_entry_mut(&mut self) -> &mut WalFileEntry {
        self.files.back_mut().expect("invariant: files non-empty")
    }

    #[cfg(test)]
    pub(crate) fn set_max_file_size(&mut self, max_file_size: u64) {
        self.max_file_size = max_file_size;
    }
}

impl WalStorage for WalWriter {
    fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.batch_buf
    }

    fn clear_buf(&mut self) {
        self.batch_buf.clear();
    }

    fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    fn flush_batch(&mut self) -> io::Result<u64> {
        let lsn = self.next_lsn;
        let batch_len = self.batch_buf.len() as u64;

        let writer = self
            .active_writer
            .as_mut()
            .expect("invariant: active writer exists");
        writer.write_all(&self.batch_buf)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;

        let entry = self.active_entry_mut();
        entry.size += batch_len;
        entry.max_lsn = lsn;

        self.batch_buf.clear();
        self.next_lsn += 1;

        self.maybe_rotate()?;
        Ok(lsn)
    }

    fn maybe_rotate(&mut self) -> io::Result<()> {
        // Rotate required?
        if let Some(entry) = self.files.back()
            && entry.size >= self.max_file_size
        {
            self.active_writer = None;
            return self.open_new_file();
        }
        Ok(())
    }

    fn delete_below(&mut self, watermark_lsn: u64) {
        while self.files.len() > 1 {
            let should_delete = self
                .files
                .front()
                .is_some_and(|entry| entry.max_lsn <= watermark_lsn);
            if !should_delete {
                break;
            }
            let entry = self.files.pop_front().unwrap();
            let _ = fs::remove_file(&entry.path);
        }
    }

    fn reclaimable_lsn(&self) -> Option<u64> {
        // * Gets second-to-last-wal file
        // * Why second-to-last? last file is the active WAL file and cannot be deleted
        // * the second-to-last file is the newest inactive—and thus potentially reclaimable—file.
        self.files
            .get(self.files.len().checked_sub(2)?)
            .map(|entry| entry.max_lsn)
    }

    #[cfg(any(test, debug_assertions))]
    fn assert_invariants(&self) {
        assert!(!self.files.is_empty(), "WAL must have at least one file");

        assert!(
            self.active_writer.is_some(),
            "active writer must exist while WAL is alive"
        );

        for i in 1..self.files.len() {
            assert!(
                self.files[i].seq > self.files[i - 1].seq,
                "WAL file seq not monotonically increasing: {} then {}",
                self.files[i - 1].seq,
                self.files[i].seq
            );
            assert!(
                self.files[i].min_lsn >= self.files[i - 1].min_lsn,
                "WAL file min_lsn not ordered: {} then {}",
                self.files[i - 1].min_lsn,
                self.files[i].min_lsn
            );
        }

        for entry in &self.files {
            assert!(
                entry.min_lsn <= entry.max_lsn,
                "WAL file seq {} has min_lsn ({}) > max_lsn ({})",
                entry.seq,
                entry.min_lsn,
                entry.max_lsn
            );
        }

        let last = self.files.back().unwrap();
        assert!(
            self.next_lsn > last.max_lsn || last.size == 0,
            "next_lsn ({}) should be > last file max_lsn ({})",
            self.next_lsn,
            last.max_lsn
        );
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    fn wal_write(wal: &mut WalWriter, data: &[u8]) -> io::Result<u64> {
        wal.buf().extend_from_slice(data);
        wal.flush_batch()
    }

    #[test]
    fn wal_writer_basic_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();

        let lsn1 = wal_write(&mut wal, b"record1").unwrap();
        assert_eq!(lsn1, 1);

        let lsn2 = wal_write(&mut wal, b"record2").unwrap();
        assert_eq!(lsn2, 2);

        assert_eq!(wal.files.len(), 1);

        wal.assert_invariants();
    }

    #[test]
    fn wal_rotation_on_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        wal.max_file_size = 100;
        assert_eq!(wal.reclaimable_lsn(), None);

        let big_data = vec![0u8; 150];
        let lsn = wal_write(&mut wal, &big_data).unwrap();

        assert_eq!(wal.files.len(), 2);
        assert_eq!(wal.reclaimable_lsn(), Some(lsn));

        wal.assert_invariants();
    }

    #[test]
    fn wal_delete_below_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        wal.max_file_size = 10;

        wal_write(&mut wal, b"aaaaaaaaaa_plus").unwrap();
        assert_eq!(wal.files.len(), 2);

        wal_write(&mut wal, b"bbbbbbbbbb_plus").unwrap();
        assert_eq!(wal.files.len(), 3);

        let first_max_lsn = wal.files[0].max_lsn;
        wal.delete_below(first_max_lsn);

        assert_eq!(wal.files.len(), 2);
        assert!(wal.files.front().unwrap().min_lsn > first_max_lsn - 1);

        wal.assert_invariants();
    }

    #[test]
    fn wal_never_deletes_active_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();

        wal_write(&mut wal, b"data").unwrap();

        wal.delete_below(u64::MAX);
        assert_eq!(wal.files.len(), 1);

        wal.assert_invariants();
    }

    #[test]
    fn wal_file_naming() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        let wal_dir = dir.path().join("wal");
        assert!(wal_dir.exists());

        let active_path = &wal.files.back().unwrap().path;
        assert!(
            active_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("wal-")
        );
        assert!(
            active_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(".log")
        );

        wal.assert_invariants();
    }

    #[test]
    fn wal_record_roundtrip() {
        let record = WalRecord::data(Bytes::from("hello world"), 7);
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        let decoded = WalRecord::decode_from(&mut &buf[..]).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn wal_batch_end_roundtrip() {
        let record = WalRecord::batch_end();
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        let decoded = WalRecord::decode_from(&mut &buf[..]).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn wal_crc_detects_corruption() {
        let record = WalRecord::data(Bytes::from("test data"), 1);
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        buf[HEADER_SIZE + 2] ^= 0xFF;

        let result = WalRecord::decode_from(&mut &buf[..]);
        assert!(result.is_err());
    }
}
