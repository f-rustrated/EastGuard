mod file;
mod memory;

pub use memory::MemoryLogStore;

use crate::raft::{
    interface::LogError,
    log::{COMMAND_LEN, LogEntry, TERM_LEN},
    messages::RaftCommand,
};

/// Position in the log. 1-based; `0` is reserved to mean "before the log begins".
pub type Index = u64;

/// Raw on-disk representation of a log entry. Internal to the storage layer;
/// the public `LogStore` trait speaks in terms of [`LogEntry`].
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) index: Index,
    pub(crate) data: Vec<u8>,
}

impl Entry {
    pub(crate) fn into_log_entry(self) -> Result<LogEntry, LogError> {
        if self.data.len() < COMMAND_LEN + TERM_LEN {
            return Err(LogError::Corruption(format!(
                "entry {} payload too short: expected >= {} bytes, got {}",
                self.index,
                COMMAND_LEN + TERM_LEN,
                self.data.len()
            )));
        }

        let command = match self.data.first() {
            Some(0x00) | None => RaftCommand::Noop,
            Some(&b) => Err(LogError::Corruption(format!(
                "unknown command discriminant: 0x{b:02x}"
            )))?,
        };

        let term = u64::from_le_bytes(
            self.data[COMMAND_LEN..COMMAND_LEN + TERM_LEN]
                .try_into()
                .unwrap(),
        );
        Ok(LogEntry {
            term,
            index: self.index,
            command,
        })
    }

    pub(crate) fn from_entry(entry: &LogEntry) -> Entry {
        let cmd_bytes = entry.command.serialize();
        debug_assert_eq!(
            cmd_bytes.len(),
            COMMAND_LEN,
            "serialize_command must produce exactly COMMAND_LEN bytes"
        );
        let mut data = Vec::with_capacity(COMMAND_LEN + TERM_LEN);
        data.extend_from_slice(&cmd_bytes);
        data.extend_from_slice(&entry.term.to_le_bytes());
        Entry {
            index: entry.index,
            data,
        }
    }
}
