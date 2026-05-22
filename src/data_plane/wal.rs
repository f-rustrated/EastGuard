use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

const DEFAULT_MAX_FILE_SIZE: u64 = 64 * 1024 * 1024; // 64MB

pub trait WalStorage {
    fn write_batch(&mut self, records: &[u8]) -> io::Result<u64>;
    fn fsync(&mut self) -> io::Result<()>;
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

    fn needs_rotation(&self) -> bool {
        if let Some(entry) = self.files.back() {
            entry.size >= self.max_file_size
        } else {
            false
        }
    }
}

impl WalStorage for WalWriter {
    fn write_batch(&mut self, records: &[u8]) -> io::Result<u64> {
        let lsn = self.next_lsn;

        let writer = self
            .active_writer
            .as_mut()
            .expect("invariant: active writer exists");
        writer.write_all(records)?;

        let entry = self.active_entry_mut();
        entry.size += records.len() as u64;
        entry.max_lsn = lsn;

        self.next_lsn += 1;

        Ok(lsn)
    }

    fn fsync(&mut self) -> io::Result<()> {
        let writer = self
            .active_writer
            .as_mut()
            .expect("invariant: active writer exists");
        writer.flush()?;
        writer.get_ref().sync_all()?;

        self.maybe_rotate()
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

    pub struct MockWalStorage {
        pub writes: Vec<(Vec<u8>, u64)>,
        pub fsyncs: u32,
        pub rotations: u32,
        pub deletes: Vec<u64>,
        next_lsn: u64,
    }

    impl MockWalStorage {
        pub(crate) fn new() -> Self {
            MockWalStorage {
                writes: Vec::new(),
                fsyncs: 0,
                rotations: 0,
                deletes: Vec::new(),
                next_lsn: 1,
            }
        }
    }

    impl WalStorage for MockWalStorage {
        fn write_batch(&mut self, records: &[u8]) -> io::Result<u64> {
            let lsn = self.next_lsn;
            self.writes.push((records.to_vec(), lsn));
            self.next_lsn += 1;
            Ok(lsn)
        }

        fn fsync(&mut self) -> io::Result<()> {
            self.fsyncs += 1;
            Ok(())
        }

        fn maybe_rotate(&mut self) -> io::Result<()> {
            self.rotations += 1;
            Ok(())
        }

        fn delete_below(&mut self, watermark_lsn: u64) {
            self.deletes.push(watermark_lsn);
        }

        fn assert_invariants(&self) {}
    }

    #[test]
    fn wal_writer_basic_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();

        let lsn1 = wal.write_batch(b"record1").unwrap();
        assert_eq!(lsn1, 1);
        wal.fsync().unwrap();

        let lsn2 = wal.write_batch(b"record2").unwrap();
        assert_eq!(lsn2, 2);
        wal.fsync().unwrap();

        assert_eq!(wal.files.len(), 1);

        wal.assert_invariants();
    }

    #[test]
    fn wal_rotation_on_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        wal.max_file_size = 100;

        let big_data = vec![0u8; 150];
        wal.write_batch(&big_data).unwrap();
        wal.fsync().unwrap();

        assert_eq!(wal.files.len(), 2);

        wal.assert_invariants();
    }

    #[test]
    fn wal_delete_below_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        wal.max_file_size = 10;

        wal.write_batch(b"aaaaaaaaaa_plus").unwrap();
        wal.fsync().unwrap();
        assert_eq!(wal.files.len(), 2);

        wal.write_batch(b"bbbbbbbbbb_plus").unwrap();
        wal.fsync().unwrap();
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

        wal.write_batch(b"data").unwrap();
        wal.fsync().unwrap();

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
}
