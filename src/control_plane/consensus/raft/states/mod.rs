/*!
Ownership boundaries for one Raft group's state.

[`LogState`] owns the in-memory Raft logs and its persistence bookkeeping:
[`current_term`], [`voted_for`], the in-memory [`LogState::entries`],
and the local durability watermark [`stabled_index`].
Its [`last_index`] is derived from the final in-memory Raft log entry; it is not a data-plane WAL position.


[`TransientState`] owns knowledge that may be reconstructed after restart:
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

[`LogState`]: self::log_state::LogState
[`current_term`]: self::log_state::LogState::current_term
[`voted_for`]: self::log_state::LogState::voted_for
[`LogState::entries`]: self::log_state::LogState::entries
[`LogState::unflushed_mutations`]: self::log_state::LogState::unflushed_mutations
[`stabled_index`]: self::log_state::LogState::stabled_index
[`last_index`]: self::log_state::LogState::last_index
[`TransientState`]: self::transient_state::TransientState
[`commit_index`]: self::transient_state::TransientState::commit_index
[`MetadataState`]: self::metadata_state::MetadataState
[`last_applied_index`]: self::metadata_state::MetadataState::last_applied_index
*/
pub(crate) mod log_state;
pub(crate) mod metadata_state;
pub(crate) mod transient_state;
