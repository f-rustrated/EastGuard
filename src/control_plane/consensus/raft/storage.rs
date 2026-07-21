use std::collections::HashSet;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::LogMutation;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::raft::states::metadata_state::MetadataSnapshot;
use crate::control_plane::membership::ShardGroupId;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RaftSnapshotHeader {
    pub(crate) last_included_index: u64,
    pub(crate) last_included_term: u64,
    pub(crate) checksum: u32,
    pub(crate) size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct SnapshotData {
    pub(crate) metadata: MetadataSnapshot,
    pub(crate) peers: Box<[NodeId]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftSnapshot {
    pub(crate) header: RaftSnapshotHeader,
    pub(crate) data: SnapshotData,
    encoded_data: Box<[u8]>,
}

impl RaftSnapshot {
    pub(crate) fn new(index: u64, term: u64, data: SnapshotData) -> Self {
        let bytes = borsh::to_vec(&data)
            .expect("encode snapshot data failed")
            .into_boxed_slice();
        Self {
            header: RaftSnapshotHeader {
                last_included_index: index,
                last_included_term: term,
                checksum: crc32fast::hash(&bytes),
                size_bytes: bytes.len() as u64,
            },
            data,
            encoded_data: bytes,
        }
    }

    pub(crate) fn from_encoded(header: RaftSnapshotHeader, encoded_data: Box<[u8]>) -> Self {
        assert_eq!(
            encoded_data.len() as u64,
            header.size_bytes,
            "snapshot size mismatch"
        );
        assert_eq!(
            crc32fast::hash(&encoded_data),
            header.checksum,
            "snapshot checksum mismatch"
        );
        let data = SnapshotData::try_from_slice(&encoded_data).expect("corrupt SnapshotData");
        Self {
            header,
            data,
            encoded_data,
        }
    }

    pub(crate) fn encoded_data(&self) -> &[u8] {
        &self.encoded_data
    }

    pub(crate) fn peers(&self, self_id: &NodeId) -> HashSet<NodeId> {
        self.data
            .peers
            .iter()
            .filter(|peer| *peer != self_id)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RaftPersistentState {
    pub(crate) term: u64,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Vec<LogEntry>,
    pub(crate) snapshot: Option<RaftSnapshot>,
}

impl RaftPersistentState {
    pub(crate) fn stabled_index(&self) -> u64 {
        self.log.last().map_or_else(
            || {
                self.snapshot
                    .as_ref()
                    .map_or(0, |snapshot| snapshot.header.last_included_index)
            },
            |entry| entry.index,
        )
    }
}

pub(crate) trait RaftStorage: Send + Sync {
    fn load_state(&self, group_id: u64) -> RaftPersistentState;
    fn persist_mutations(&self, mutations: Box<[(ShardGroupId, LogMutation)]>);
    fn delete_group(&self, group_id: ShardGroupId);
}
