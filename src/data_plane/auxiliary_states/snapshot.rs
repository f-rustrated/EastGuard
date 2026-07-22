use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use borsh::{BorshDeserialize, BorshSerialize};

use super::consumer_offsets::state::ConsumerOffsets;
use super::producer::ProducerSessions;

const SNAPSHOT_FILE: &str = "auxiliary-state.snapshot";
const SNAPSHOT_TEMP_FILE: &str = "auxiliary-state.snapshot.tmp";

/// Crash-durable end state reconstructed from the shared WAL.
#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub(crate) struct AuxiliarySnapshot {
    pub(crate) consumer_offsets: ConsumerOffsets,
    pub(crate) producer_sessions: ProducerSessions,
}

impl AuxiliarySnapshot {
    pub(crate) fn load(data_dir: &Path) -> io::Result<Self> {
        let bytes = match fs::read(data_dir.join(SNAPSHOT_FILE)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => return Err(error),
        };
        Self::try_from_slice(&bytes).map_err(io::Error::other)
    }

    pub(crate) fn write(&self, data_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(data_dir)?;
        let bytes = borsh::to_vec(self).map_err(io::Error::other)?;
        let temporary = data_dir.join(SNAPSHOT_TEMP_FILE);
        let final_path = data_dir.join(SNAPSHOT_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temporary, final_path)?;
        File::open(data_dir)?.sync_all()
    }
}
