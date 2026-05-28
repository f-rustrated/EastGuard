use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::PathBuf;

use bytes::Bytes;

const DEFAULT_MAX_FILE_SIZE: u64 = 64 * 1024 * 1024; // 64MB
const HEADER_SIZE: usize = 9; // crc32(4) + type(1) + length(4)
const TRAILING_LENGTH_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum WalRecordType {
    Data = 0,
    BatchEnd = 1,
}

impl WalRecordType {
    fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0 => Ok(WalRecordType::Data),
            1 => Ok(WalRecordType::BatchEnd),
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
    pub payload: Bytes,
}

impl WalRecord {
    pub(crate) fn data(payload: Bytes) -> Self {
        WalRecord {
            record_type: WalRecordType::Data,
            payload,
        }
    }

    pub(crate) fn batch_end() -> Self {
        WalRecord {
            record_type: WalRecordType::BatchEnd,
            payload: Bytes::new(),
        }
    }

    pub(crate) fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.payload.len() + TRAILING_LENGTH_SIZE
    }

    pub(crate) fn encode_to(&self, writer: &mut impl Write) -> io::Result<()> {
        let payload_len = self.payload.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[self.record_type as u8]);
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&self.payload);
        let crc = hasher.finalize();

        writer.write_all(&crc.to_be_bytes())?;
        writer.write_all(&[self.record_type as u8])?;
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
        let payload_len = u32::from_be_bytes(header[5..9].try_into().unwrap()) as usize;

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
        hasher.update(&(payload_len as u32).to_be_bytes());
        hasher.update(&payload);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        Ok(WalRecord {
            record_type,
            payload: Bytes::from(payload),
        })
    }
}

pub trait WalStorage {
    fn buf(&mut self) -> &mut Vec<u8>;
    fn next_lsn(&self) -> u64;
    fn flush_batch(&mut self) -> io::Result<u64>;
    fn maybe_rotate(&mut self) -> io::Result<()>;
    fn delete_below(&mut self, watermark_lsn: u64);

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

    #[allow(dead_code)]
    pub(crate) fn open_existing(data_dir: PathBuf, next_lsn: u64) -> io::Result<Self> {
        let wal_dir = data_dir.join("wal");
        fs::create_dir_all(&wal_dir)?;

        let mut entries: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(seq_str) = name_str
                .strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
                && let Ok(seq) = seq_str.parse::<u64>()
            {
                entries.push((seq, entry.path()));
            }
        }
        entries.sort_by_key(|e| e.0);

        let mut files = VecDeque::new();

        for (seq, path) in &entries {
            let meta = fs::metadata(path)?;
            files.push_back(WalFileEntry {
                seq: *seq,
                min_lsn: 0,
                max_lsn: 0,
                size: meta.len(),
                path: path.clone(),
            });
        }

        let mut writer = WalWriter {
            files,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            next_lsn,
            data_dir,
            next_seq: entries.last().map(|(max_seq, _)| *max_seq).unwrap_or(0) + 1,
            active_writer: None,
            batch_buf: Vec::new(),
        };

        if writer.files.is_empty() {
            writer.open_new_file()?;
        } else {
            let active = writer.files.back().expect("non-empty files");
            let file = OpenOptions::new().append(true).open(&active.path)?;
            writer.active_writer = Some(BufWriter::new(file));
        }

        Ok(writer)
    }

    fn active_entry_mut(&mut self) -> &mut WalFileEntry {
        self.files.back_mut().expect("invariant: files non-empty")
    }

    #[allow(dead_code)]
    fn needs_rotation(&self) -> bool {
        if let Some(entry) = self.files.back() {
            entry.size >= self.max_file_size
        } else {
            false
        }
    }
}

impl WalStorage for WalWriter {
    fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.batch_buf
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

        let big_data = vec![0u8; 150];
        wal_write(&mut wal, &big_data).unwrap();

        assert_eq!(wal.files.len(), 2);

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
        let record = WalRecord::data(Bytes::from("hello world"));
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
        let record = WalRecord::data(Bytes::from("test data"));
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        buf[HEADER_SIZE + 2] ^= 0xFF;

        let result = WalRecord::decode_from(&mut &buf[..]);
        assert!(result.is_err());
    }
}
