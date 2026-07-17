/*!
Ownership boundaries for one Raft group's state.

The log half of [`ConsensusState`] owns the in-memory Raft log and its persistence bookkeeping:
[`current_term`], [`voted_for`], the in-memory [`LogState::entries`],
and the local durability watermark [`stabled_index`].
Its [`last_index`] is derived from the final in-memory Raft log entry; it is not a data-plane WAL position.


The transient half of [`ConsensusState`] owns knowledge that may be reconstructed after restart:
the current role, leader identity, replication progress, and [`commit_index`].
A vote is not transient: forgetting [`voted_for`] after a crash could let one replica vote for two candidates in the same term.

[`MetadataState`] owns the committed application state and [`last_applied_index`].

Numeric progress watermarks are ordered as:
```text
last_applied_index <= commit_index <= stabled_index <= last_index
```

Higher indices mean further progress. Expressed as entry sets, the same
relationship is `applied ⊆ committed ⊆ stable ⊆ in-memory log`.

- [`last_index`]: newest entry currently present in the in-memory Raft log.
- [`stabled_index`]: newest Raft log entry successfully persisted to RocksDB.
- [`commit_index`]: newest durable entry known to have quorum agreement.
- [`last_applied_index`]: newest committed entry applied to metadata.

The data-plane WAL is separate: it stores segment records, while these
boundaries describe the control-plane Raft metadata log.

[`ConsensusState`]: self::consensus::ConsensusState
[`current_term`]: self::consensus::ConsensusState::current_term
[`voted_for`]: self::consensus::ConsensusState::voted_for
[`LogState::entries`]: self::consensus::ConsensusState::log_entries
[`stabled_index`]: self::consensus::ConsensusState::stabled_index
[`last_index`]: self::consensus::ConsensusState::last_log_index
[`commit_index`]: self::consensus::ConsensusState::commit_index
[`MetadataState`]: self::metadata_state::MetadataState
[`last_applied_index`]: self::metadata_state::MetadataState::last_applied_index
*/
pub(crate) mod consensus;
pub(crate) mod metadata_state;
