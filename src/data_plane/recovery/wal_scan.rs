//! WAL directory discovery for crash recovery
//!
//! Read-only: lists the files replay will scan, in replay order. The oldest
//! surviving file is the checkpoint boundary — WAL files are deleted only
//! once every segment has checkpointed past them, so "still on disk" means
//! "may contain entries the segment files lack".

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::data_plane::wal::parse_wal_filename;

/// One WAL file, as replay will visit it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalFileRef {
    pub(crate) seq: u64,
    pub(crate) path: PathBuf,
}

/// Lists the WAL files under `data_dir/wal`, sorted by sequence number —
/// the order the live writer created them, hence replay order. Gaps in the
/// sequence are normal: checkpoint-driven deletion removes old files.
///
/// A missing WAL directory yields an empty result, not an error: a node
/// that never wrote (or a wiped disk) has nothing to replay. Files that do
/// not match the WAL naming scheme are skipped with a debug log — foreign
/// files must not abort recovery.
pub(crate) fn discover_wal_files(data_dir: &Path) -> io::Result<Box<[WalFileRef]>> {
    let wal_dir = data_dir.join("wal");
    let read_dir = match fs::read_dir(&wal_dir) {
        Ok(read_dir) => read_dir,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Box::new([])),
        Err(e) => return Err(e),
    };

    let mut files = Vec::new();
    for entry in read_dir {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        match parse_wal_filename(&name_str) {
            Some(seq) => files.push(WalFileRef {
                seq,
                path: entry.path(),
            }),
            None => {
                tracing::debug!(file = %name_str, "skipping non-WAL file in WAL directory");
            }
        }
    }

    files.sort_by_key(|f| f.seq);
    Ok(files.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    fn touch(dir: &Path, name: &str) {
        File::create(dir.join(name)).unwrap();
    }

    fn make_wal_dir(root: &Path) -> PathBuf {
        let wal_dir = root.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        wal_dir
    }

    #[test]
    fn missing_wal_dir_yields_empty() {
        let dir = tempfile::tempdir().unwrap();
        let files = discover_wal_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn empty_wal_dir_yields_empty() {
        let dir = tempfile::tempdir().unwrap();
        make_wal_dir(dir.path());
        let files = discover_wal_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn files_sorted_by_seq_with_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = make_wal_dir(dir.path());
        // Gaps are normal: checkpoint-driven deletion removed 1 and 3–6.
        touch(&wal_dir, "wal-000007.log");
        touch(&wal_dir, "wal-000002.log");
        touch(&wal_dir, "wal-000010.log");

        let files = discover_wal_files(dir.path()).unwrap();
        let seqs: Vec<u64> = files.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![2, 7, 10]);
        assert!(files.iter().all(|f| f.path.starts_with(&wal_dir)));
    }

    #[test]
    fn foreign_files_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = make_wal_dir(dir.path());
        touch(&wal_dir, "wal-000001.log");
        touch(&wal_dir, "notes.txt");
        touch(&wal_dir, "wal-abc.log");
        touch(&wal_dir, "wal-000003.log.tmp");

        let files = discover_wal_files(dir.path()).unwrap();
        let seqs: Vec<u64> = files.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![1]);
    }

    #[test]
    fn live_writer_files_are_discovered() {
        // Round-trip against the real writer: whatever WalWriter creates,
        // discovery must list, in creation order.
        use crate::data_plane::wal::{WalStorage, WalWriter};

        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        wal.buf().extend_from_slice(b"data");
        wal.flush_batch().unwrap();

        let files = discover_wal_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].seq, 1);
    }
}
