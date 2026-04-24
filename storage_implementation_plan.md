# Raft Storage Architecture — RocksDB Integration

## Three-Layer Design

```
MultiRaftActor
    │  routing, timers, transport — zero storage knowledge
    │  no shard group HashMap; delegates everything to MultiRaftStore
    │
    ▼
MultiRaftStore                         ← one instance per node, manages ALL shard groups
    │  owns DB + all RaftGroup instances + all state machines
    │  sole handler for all storage operations
    │
    ▼
RaftGroup                                   ← pure consensus, no generic, no LogStore
    │  step / propose / handle_timeout
    │  all side effects buffered and drained by caller:
    │    take_outbound()           → Vec<OutboundRaftPacket>
    │    take_timer_commands()     → Vec<TimerCommand<RaftTimer>>
    │    take_log_mutations()      → Vec<LogMutation>
    ▼
Vec<LogEntry>                          ← in-memory only, inside RaftGroup
```

**Why one `MultiRaftStore` per node (not per shard):**
Managing N shard groups requires a single entity with visibility into all of them — to batch writes across shards into
one `WriteBatch` + one fdatasync. A per-shard store forces the actor to coordinate storage, which leaks storage
concerns upward. One store owns everything: DB, all RaftGroup instances, all state machines.

**Why `DB` is owned directly (no `Arc`):**
`MultiRaftStore` is the sole owner of `DB`. There is exactly one `MultiRaftStore` per node. No sharing → no `Arc`.
`Arc` would only be needed if multiple owners existed; that was an artifact of the per-shard handle design.

**Why `RaftGroup` has no `LogStore` dependency:**
`RaftGroup` is a pure sync state machine. Storage is not a consensus concern. It buffers log mutations the same way it
buffers outbound RPCs — `MultiRaftStore` drains and persists them. `RaftGroup` never knows a store exists.

---

## Dependency Graph

```
actor          ──→  store        (MultiRaftStore)
                    NO import of state

store          ──→  state        (RaftGroup — no generic, no LogStore)
store          ──→  storage/     (RocksDB, CF ops — internal only)

state          ──→  log, messages
                    NO import of storage/, actor
```

`MultiRaftActor::run` takes one `MultiRaftStore`:

```
MultiRaftActor::run(
    node_id: NodeId,
    mailbox: Receiver<RaftCommand>,
    transport_tx: Sender<RaftTransportCommand>,
    scheduler_tx: Sender<TickerCommand<RaftTimer>>,
    store: MultiRaftStore,             // one store, all shards inside
)
```

---

## MultiRaftStore internals

```
MultiRaftStore {
    db:           DB,                                    // sole owner
    shards:       HashMap<ShardGroupId, ShardGroupState>,
    dirty_shards: HashSet<ShardGroupId>,                 // tracks which shards need flush
    pending_votes: HashMap<ShardGroupId, Vec<(NodeId, RaftRpc)>>, // TODO: buffer msgs for unknown shards
}

ShardGroupState {
    raft:          RaftGroup,
    cf:            CF,
    pending:       Vec<(key, val)>,                      // write buffer
    state_machine: MetadataStateMachine,
    applied_index: u64,                                  // persisted — see §Flush
    stabled:       u64,                                  // highest index flushed to RocksDB
    snap_state:    SnapState,                            // TODO: background snapshot apply
    epoch:         ShardEpoch,                           // conf_ver + version
}
```

### Shard lifecycle

```
ensure_group(group):
    db.create_cf(group.id.to_string(), &opts)   // no-op if exists
    cfs.insert(group.id, cf_handle)
    shards.insert(group.id, ShardGroupState {
        raft:          RaftGroup::new(...),
        cf:            cf_handle,
        pending:       vec![],
        state_machine: MetadataStateMachine::default(),
        applied_index: read_applied_index(cf) or 0,
        stabled:       last_log_index_from_cf(cf) or 0,
        snap_state:    SnapState::Relax,
        epoch:         read_epoch(cf) or ShardEpoch::default(),
    })
    // drain pending_votes[group.id] and step each buffered message
    if let Some(votes) = pending_votes.remove(group.id):
        for (from, rpc) in votes: step(group.id, from, rpc)

remove_group(id):
    shards.remove(id)          // drops RaftGroup, CF handle, MetadataStateMachine atomically
    pending_votes.remove(id)
    db.drop_cf(id.to_string())
```

**Startup:** `DB::list_cf` recovers existing CF names from MANIFEST. `DB::open_cf` reopens them all. No external shard
ID registry needed.

### step / propose / handle_timeout

These methods only buffer mutations — they do **not** sync. The actor controls when sync happens.

```
step(shard_id, from, rpc):
    // if shard not yet created, buffer and return — drained on ensure_group
    if shard_id not in shards:
        pending_votes[shard_id].push((from, rpc))   // TODO: pending vote buffer
        return
    shard = shards[shard_id]
    shard.raft.step(from, rpc)
    // drain log mutations from RaftGroup → buffer into shard.pending
    for mutation in shard.raft.take_log_mutations():
        match mutation:
            Append(entry)      → shard.pending.push(encode(entry))
            TruncateFrom(idx)  → clear shard.pending, buffer a range-delete marker
            HardState(hs)      → shard.pending.push((ShardCfKey::HardState, encode(hs)))
    dirty_shards.insert(shard_id)
    // apply_committed is NOT called here — deferred to after flush()
    // (entries must be stabled before they can be applied; see §Flush Timing)
    // outbound RPCs accumulate in RaftGroup's pending_outbound — NOT sent yet
```

`propose` and `handle_timeout` follow the same pattern: mutate RaftGroup in-memory, buffer log mutations, return. No I/O.

### State machine application

`apply_committed_entries` lives in `MultiRaftStore`, not inside `RaftGroup`. It runs **after** `flush()` so that only
entries with `index ≤ stabled[shard_id]` are applied — entries must be persisted before they are committable. `RaftGroup`
exposes `take_newly_committed(up_to: u64) -> Vec<LogEntry>`:

```
// called after flush(), not inside dispatch()
apply_committed(shard_id):
    shard = shards[shard_id]
    safe_up_to = min(shard.raft.commit_index, shard.stabled)
    for entry in shard.raft.take_newly_committed(safe_up_to):
        match entry.command:
            AddPeer(id)     → shard.raft.add_peer(id)
                              shard.epoch.conf_ver += 1
            RemovePeer(id)  → shard.raft.remove_peer(id)
                              shard.epoch.conf_ver += 1
            CreateTopic(t)  → shard.state_machine.topics.insert(...)
            AssignRange(r)  → shard.state_machine.ranges.insert(...)
            Noop            → skip
        shard.applied_index = entry.index  // updated in-memory; flushed next cycle
    dirty_shards.insert(shard_id)          // applied_index + epoch need to be flushed
```

`applied_indices[shard_id]` is written to RocksDB atomically with the next `flush()` — both the state and the
watermark land in the same `WriteBatch`, preventing double-apply on crash recovery.

### Snapshots

A snapshot is split into two parts: a small in-band metadata struct and a large out-of-band data payload.

**`ShardSnapshotMeta`** — sent inline in the Raft RPC (small):

```
ShardSnapshotMeta {
    last_included_index: u64,
    last_included_term:  u64,
    peers:               HashSet<NodeId>,   // Raft membership at snapshot point
    data_size:           u64,               // expected byte count of SnapData
    data_checksum:       u32,               // CRC32 of SnapData for integrity verification
    epoch:               ShardEpoch,        // conf_ver + version at snapshot point
}
```

**`ShardSnapshotData`** — streamed out-of-band via a dedicated channel (may be large):

```
ShardSnapshotData {
    topics:   HashMap<TopicId, TopicMeta>,
    ranges:   HashMap<RangeId, RangeMeta>,
    segments: HashMap<SegmentId, SegmentMeta>,
}
```

`ShardSnapshotMeta` is stored under `ShardCfKey::SnapMeta` (`0x03`). `ShardSnapshotData` is stored under
`ShardCfKey::SnapData` (`0x04`). After saving, `compact_before(last_included_index)` reclaims log storage.

**Why split meta from data:**
`ShardSnapshotMeta` is small (tens of bytes) and travels in the Raft RPC message. The data payload can be megabytes —
embedding it in the RPC would block the transport layer and stall all other shard groups sharing the same TCP
connection.
The receiver writes data chunks to `.tmp` files, verifies `data_checksum` on completion, then atomic-renames to the
final path before applying. A partial transfer is invisible to the apply path until fully verified.

**Why snapshots cover application state:**
The Raft log is a sequence of commands to the state machine. A snapshot is the full serialized state at index N. After
install, the node needs no log entries before `last_included_index` to serve reads or replicate.

**If `compact_before` fails after `save_snapshot` succeeds:**
The snapshot is already durable. Log entries below `last_included_index` remain on disk but are effectively orphaned —
`RaftGroup` will not re-read them because `first_index` has been advanced past them. No correctness violation occurs: the
snapshot is the authoritative source of truth, and the node can still serve reads and participate in replication. The
only consequence is wasted disk space. Log the error and retry `compact_before` on the next snapshot cycle.

**TODO — Background snapshot application (`SnapState`):**
`SnapState { Relax | Generating | Applying }` will be tracked per shard in `MultiRaftStore`. When
`ShardSnapshotMeta` is received, the shard is marked `Applying` and snapshot work is dispatched to a background
`tokio::spawn` task. `MultiRaftActor` skips all `step`/`propose` calls for that shard until `SnapState → Relax`.
This prevents a large snapshot install from stalling all other shard groups on the node. Scoped to a follow-up.

---

## RocksDB Layout

One `DB` per node. Each shard group gets its own **Column Family**. A single `"meta"` CF holds node-scoped state.

```
DB
├── CF: "meta"        →  0x01  →  NodeId
│                        // TODO: schema versioning — use RocksDB MANIFEST for now
│
├── CF: "shard-0001"  →  0x01 + [u64 BE]  →  LogEntry bytes
│                        0x02             →  HardState bytes
│                        0x03             →  SnapshotMeta bytes
│                        0x04             →  snapshot payload bytes
├── CF: "shard-0002"  →  same layout
└── CF: "shard-N"     →  ...
```

**Key encoding — per-shard CFs:**

```
enum ShardCfKey {
    LogEntry(u64),  // 0x01 + u64 BE  — 9 bytes, lexicographic order = numeric order
    HardState,      // 0x02           — 1 byte
    SnapMeta,       // 0x03           — 1 byte
    SnapData,       // 0x04           — 1 byte
    AppliedIndex,   // 0x05           — 1 byte — persisted applied_index for crash recovery
    Epoch,          // 0x06           — 1 byte — ShardEpoch (conf_ver, version)
}
```

Metadata keys (`0x02`–`0x04`) sort strictly before all log entries (`0x01...`). The compaction range-delete
`LogEntry(1)..LogEntry(index)` only touches log entry keys — metadata is unreachable by range ops.

**Shard discovery at startup:** `DB::list_cf` reads CF names from the RocksDB MANIFEST. Filter out `"meta"` and
`"default"` — everything else is a shard CF to reopen. No external shard ID registry needed.

**Why column families over key prefixes:**

- `compact_range_cf` scopes compaction to one shard — no cross-shard interference.
- `drop_cf` on `RemoveGroup` removes all shard data in O(1) — no per-key deletes, no leak risk when new keys are added.
- Independent compaction policies per shard if needed.

---

## Flush Timing

### When to flush

Not all Raft operations write to the log. Flushing on every operation wastes fsyncs.

| Operation                                       | Log write? | Hard state write?      | Flush needed? |
|-------------------------------------------------|------------|------------------------|---------------|
| `AppendEntries` — follower accepts entries      | yes        | yes (if term advances) | yes           |
| `AppendEntries` — follower rejects (stale term) | no         | no                     | no            |
| `HeartbeatTimeout` — leader, no new entries     | no         | no                     | no            |
| `ElectionTimeout` → noop appended on leader win | yes        | yes (term, voted_for)  | yes           |
| `propose()` on leader                           | yes        | no                     | yes           |
| `RequestVote` — vote granted                    | no         | yes (voted_for)        | yes           |
| `RequestVote` — vote denied / responses         | no         | no                     | no            |
| `AppendEntriesResponse` on leader               | no         | no                     | no            |

`MultiRaftStore` tracks which shards have pending writes. `flush()` is a no-op if nothing is dirty.

### When `MultiRaftActor` calls `flush()`

`MultiRaftActor` drains the mailbox in a batch, then calls `store.flush()` once before dispatching any outbound. All
responses are sent **after** flush — durability is guaranteed. State machine application also runs after flush, gated
by the `stabled` watermark updated inside `flush()`.

```
MultiRaftActor event loop:

    // block on first command
    cmd = mailbox.recv().await
    store.dispatch(cmd)

    // drain any further ready commands (non-blocking)
    while let Ok(cmd) = mailbox.try_recv():
        store.dispatch(cmd)

    // one flush covers all shards touched in this batch
    store.flush()                        // opaque API — MultiRaftActor does not know what this does
                                         // also updates shard.stabled for each dirty shard

    // apply state machine only after entries are persisted
    store.apply_all_committed()          // bounded by min(commit_index, stabled) per shard

    // now safe to send responses
    send_all(store.take_all_outbound())
    schedule_all(store.take_all_timer_commands())
```

`store.flush()` and `store.apply_all_committed()` are the only storage-adjacent calls `MultiRaftActor` makes. Their implementations are fully hidden inside `MultiRaftStore`.

### Cross-shard group commit inside `flush()`

`flush()` combines pending buffers from **all** dirty shards into one `WriteBatch`, then does one `fdatasync`. After the
write succeeds, `stabled[shard_id]` is advanced to the highest persisted log index for each dirty shard. The
`applied_index` written during the previous `apply_all_committed()` is also included in this batch (see below).

```
MultiRaftStore::flush():
    if dirty_shards is empty → return early (no-op)
    batch = new WriteBatch
    for shard_id in dirty_shards.drain():
        shard = shards[shard_id]
        for (k, v) in shard.pending.drain():
            batch.put_cf(shard.cf, k, v)
        // persist applied_index and epoch atomically with log entries
        batch.put_cf(shard.cf, ShardCfKey::AppliedIndex, encode(shard.applied_index))
        batch.put_cf(shard.cf, ShardCfKey::Epoch,        encode(shard.epoch))
    db.write(batch, sync=true)           // one WriteBatch + one fdatasync
    // advance stabled watermarks — now safe to apply up to these indices
    for shard_id in flushed_shards:
        shards[shard_id].stabled = shards[shard_id].raft.last_log_index()
```

N shards modified in one event loop turn → **one `WriteBatch`** → **one fdatasync**.

**Multi-Raft `applied_index` lifecycle:** Each shard group is fully independent. `applied_indices` is a
`HashMap<ShardGroupId, u64>` — there is no global applied index. `ensure_group` reads the persisted value from the
shard's CF (or initializes to 0 for new shards). `remove_group` drops the CF entirely, which removes the key. On crash
recovery, each CF is reopened independently and its `AppliedIndex` key is read to determine the replay start point —
only log entries after that index need to be re-applied to the state machine.

---

## Segmented Logs: RocksDB Compaction

After snapshot at index S:

```
compact_before(shard_id, index):
    cf = cfs[shard_id]
    db.delete_range_cf(cf, encode_key(1), encode_key(index))  // range tombstone — O(1)
    db.compact_range_cf(cf, ...)                               // SST merge, disk reclaimed
```

`delete_range_cf` writes a tombstone — reads through the range immediately return `None`. Background compaction
physically removes covered SST data. WAL files are auto-removed via `wal_ttl_seconds` / `wal_size_limit_mb`.

---

## LogMutation: RaftGroup's Storage Interface

`RaftGroup` uses the buffer-drain pattern for all side effects. Log mutations follow the same contract as outbound RPCs:

```
// RaftGroup buffers these — MultiRaftStore drains and persists
enum LogMutation {
    Append(LogEntry),
    TruncateFrom(Index),
    HardState { term: u64, voted_for: Option<NodeId> },  // term or vote changed
}

RaftGroup::take_log_mutations() -> Vec<LogMutation>
RaftGroup::take_newly_committed() -> Vec<LogEntry>
```

Every `log.push()`, `log.truncate()`, `current_term` change, or `voted_for` change inside `RaftGroup` appends to
`pending_log_mutations`. `RaftGroup` has no import of any storage type.

`MultiRaftStore` persists `HardState` to RocksDB under `ShardCfKey::HardState` in the same `WriteBatch` as log entries —
atomically, in one `flush()`. Hard state and log are never out of sync after a crash.

**Why this matters for correctness:** Raft requires `voted_for` to be durable before sending `RequestVoteResponse` and
`current_term` to be durable before responding to any RPC. Both are covered by `flush()` before `take_all_outbound()`.

---

## Layer Abstraction Boundaries

What each layer is allowed to know:

| Layer             | Knows about                                           | Must NOT know about                           |
|-------------------|-------------------------------------------------------|-----------------------------------------------|
| `MultiRaftActor`  | `MultiRaftStore` public API, transport, timers        | `DB`, `CF`, `WriteBatch`, `LogMutation`       |
| `MultiRaftStore`  | `RaftGroup`, `DB`, `CF`, `pending`, `LogMutation`     | transport, timer scheduling, mailbox          |
| `RaftGroup`       | `Vec<LogEntry>`, `LogMutation` (own data type)        | `DB`, `CF`, `LogStore`, `MultiRaftStore`      |

`flush()` is the boundary where storage implementation crosses into the actor layer — but only as an opaque API name.
`MultiRaftActor` calls `flush()` without knowing it performs a `WriteBatch` write and `fdatasync`.

---

## Durability Boundaries

| Event                                 | Durability required before                     |
|---------------------------------------|------------------------------------------------|
| `AppendEntriesResponse(success=true)` | Entries written to follower's log              |
| `propose()` on leader                 | Entry durable on leader (counts toward quorum) |
| `install_snapshot`                    | Snapshot durable, `compact_before` called      |
| `RemoveGroup`                         | `drop_cf` completes                            |

`MultiRaftActor` calls `store.flush()` after draining the mailbox batch, before dispatching any outbound. This single call
covers all durability requirements for all shards in that batch. No response is sent before flush completes.

---

## Shard Epoch — Stale Routing Detection

Each shard group carries a `ShardEpoch` that increments on membership and topology changes:

```rust
struct ShardEpoch {
    conf_ver: u64,   // incremented on AddPeer / RemovePeer commit
    version: u64,   // incremented on shard split / merge (future)
}
```

Every client request that targets a shard (e.g., `Propose`, `GetLeader`) includes the client's cached epoch.
`MultiRaftStore` validates on entry:

```
validate_epoch(shard_id, client_epoch):
    current = epochs[shard_id]
    if client_epoch.conf_ver < current.conf_ver
    || client_epoch.version  < current.version:
        return Err(EpochNotMatch { current })
    // proceed
```

On `EpochNotMatch`, the caller refreshes its routing cache (re-queries SWIM topology) and retries. This prevents
split-brain scenarios where a client sends commands to a shard group whose membership has already changed.

`conf_ver` is incremented in `apply_committed` when `AddPeer` or `RemovePeer` is applied. `version` is reserved for
future split/merge operations. Both are persisted under `ShardCfKey::Epoch` in the same `WriteBatch` as `applied_index`.

---

## ADR: Column Family Options — Log vs. State Machine Data

**Decision:** Use uniform `ColumnFamilyOptions` for all shard CFs initially. Separate `raft_cfs` and `sm_cfs` per shard
is deferred.

**Context:** Log entries (`0x01...`) are sequential-append heavy with short retention (compacted after snapshot). State
machine data (`AppliedIndex`, `Epoch`, `SnapMeta`, `SnapData`) is small, infrequently written, and read on startup.
TinyKV uses two separate engines for these access patterns. EastGuard encodes both in one CF per shard.

**Considered alternative:** Two CFs per shard — `"shard-N-raft"` (log, HardState) and `"shard-N-sm"` (state machine,
applied index, epoch). Would allow:

- Larger `write_buffer_size` for the log CF (sequential write throughput).
- Bloom filters + block cache tuning on the SM CF (point reads at startup).
- `compact_range_cf` on log CF without touching SM data.

**Why deferred:** At current scale (metadata-only state machines with small payloads), the performance difference is
negligible. The key prefix separation (`0x01` vs `0x02`–`0x06`) already ensures log compaction
(`delete_range_cf` on `0x01..`) never touches metadata keys. Re-evaluate when state machine payloads grow or when
compaction pressure becomes measurable.

**TODO:** If log CF write latency becomes a bottleneck, split into `"shard-N-raft"` + `"shard-N-sm"` CFs with
differentiated `ColumnFamilyOptions`. `MultiRaftStore` would hold `raft_cfs` and `sm_cfs` as separate maps; all
writes remain in one `WriteBatch` across both CFs for atomicity.

---

## Log Entry Lifecycle — In-Memory to Persistent 

### Current state (in-memory only)

`Raft.log: Vec<LogEntry>` holds all entries from index 1 indefinitely. `MultiRaft::flush()` drains `LogMutation`s from
each dirty `Raft` group but **discards them** (`let _ = raft.take_log_mutations()`). The Vec grows unbounded — no
compaction, no snapshot, no eviction. This is acceptable for development but not production.

### After RocksDB integration

Entries are **dual-written**: they remain in `RaftGroup`'s in-memory `Vec<LogEntry>` (for fast reads during replication)
and are persisted to per-shard CFs via `WriteBatch` in `flush()`. The in-memory Vec is the hot path — `log_term_at()`,
`log_entries_from()`, and `log_get()` all index into it directly without touching RocksDB.

Compaction (`compact_before(index)`) only reclaims RocksDB storage. The in-memory Vec must be truncated separately after
a snapshot is saved. After truncation, `RaftGroup` must track a `first_index` offset so that 1-based index arithmetic
still works against the shortened Vec. Reads for indices below `first_index` return `None` — the caller (leader
replicating to a lagged follower) must fall back to `InstallSnapshot`.

### Transition checklist

1. `flush()` writes `LogMutation`s to `WriteBatch` instead of discarding.
2. `compact_before(index)` reclaims RocksDB entries + truncates in-memory Vec.
3. `RaftGroup` tracks `first_index` to adjust Vec indexing after truncation.
4. `log_term_at()`, `log_get()`, `log_entries_from()` must handle `index < first_index` (return `None` / empty).
5. Leader detects missing entries → triggers `InstallSnapshot` instead of decrementing `next_index` forever.

---

## New Member Catchup

### How it works today

When a new node joins via SWIM, `HandleNodeJoin` triggers `AddPeer(new_node_id)` on every group where the leader does
not yet have the new node as a peer. On commit, the leader initializes:

```
PeerState {
    next_index:  last_log_index + 1,   // optimistic — assumes peer is caught up
    match_index: 0,                     // conservative — nothing confirmed
}
```

The next heartbeat sends `AppendEntries` with `prev_log_index = next_index - 1`. The new node has an empty log, so it
rejects. The leader decrements `next_index` by 1 and immediately retries. This repeats until `next_index = 1`, where
`prev_log_index = 0` (the before-log sentinel). The new node accepts and receives all entries from index 1 onward.

**This works because all entries exist in memory.** The leader's Vec has every entry from index 1.

### After compaction — the gap

Once `compact_before(S)` runs, the leader no longer has entries below index S. A new node that needs entries from index 1
will keep getting rejections as `next_index` decrements, but the leader cannot serve entries below S. Without
`InstallSnapshot`, the follower has **no recovery path**.

Required sequence after compaction exists:

```
Leader: send AppendEntries(prev_log_index = next_index - 1)
Follower: reject (no matching entry)
Leader: decrement next_index
  ...repeat...
Leader: next_index < first_index  →  entries no longer available
Leader: switch to InstallSnapshot(snapshot_at_index_S)
Follower: apply snapshot, set log to start from S
Leader: resume normal AppendEntries from S+1
```

### Probing optimization — ConflictTerm / ConflictIndex

Current rejection handling is linear: decrement `next_index` by 1 per round-trip. For a new node with an empty log
joining a leader with 10,000 entries, this means 10,000 round-trips to converge.

**Raft §5.3 optimization:** The follower includes a hint in `AppendEntriesResponse`:

```
AppendEntriesResponse {
    ...
    conflict_term:  Option<u64>,    // term of the conflicting entry, if any
    conflict_index: Option<u64>,    // first index of that term (or last_log_index + 1 if empty)
}
```

Leader uses the hint to skip entire terms:
- If `conflict_term` is `None` (follower has no entry at `prev_log_index`): set `next_index = conflict_index`
  (follower's `last_log_index + 1`). For empty log, jumps straight to 1.
- If `conflict_term` is `Some(ct)`: leader searches its own log for the last entry of term `ct`. If found, set
  `next_index` to that entry's index + 1. If not found, set `next_index = conflict_index`.

Worst case drops from O(log_length) to O(number_of_distinct_terms) round-trips — typically single digits.

---

## Open Questions / TODOs

- **InstallSnapshot RPC** (TODO): Add `InstallSnapshot` to `RaftRpc`. Receiver transitions shard to
  `SnapState::Applying`,
  dispatches apply to background task, blocks further steps on that shard until `Relax`. See §Snapshots for design.
- **Pending vote buffer** (TODO): `step()` buffers `PacketReceived` for unknown shard IDs into `pending_votes`.
  `ensure_group` drains and replays them. Prevents silent message drops when `EnsureGroup` and peer RPCs race.
- **Background snapshot application** (TODO): Implement `SnapState` tracking and `tokio::spawn` dispatch for snapshot
  install. Required before `InstallSnapshot` RPC is wired up.
- **In-memory Vec truncation after compaction** (TODO): After `compact_before(S)`, truncate `RaftGroup`'s in-memory
  `Vec<LogEntry>` and introduce `first_index` offset. All index-based accessors (`log_term_at`, `log_get`,
  `log_entries_from`) must subtract `first_index` for correct Vec indexing. Reads below `first_index` return `None`.
- **ConflictTerm/ConflictIndex hint** (TODO): Add `conflict_term: Option<u64>` and `conflict_index: Option<u64>` to
  `AppendEntriesResponse`. Follower populates on rejection. Leader uses hint to skip terms in bulk instead of
  decrementing `next_index` by 1. See §New Member Catchup → Probing optimization.
- **Snapshot-triggered replication fallback** (TODO): When leader's `next_index` for a peer falls below `first_index`
  (entries compacted), switch from `AppendEntries` to `InstallSnapshot`. Requires `InstallSnapshot` RPC and
  `SnapState` tracking to be implemented first.
- **Log compaction trigger**: `MultiRaftStore` triggers `compact_before` after `save_snapshot` when
  `last_applied - snapshot_index > threshold`. Policy TBD.
- **Safe truncation boundary**: May only compact up to `min(all_replicas.match_index)`. Leader must not compact entries
  a lagging follower still needs unless sending a snapshot instead. Violating this forces a snapshot transfer to the
  lagging follower — expensive but not a correctness failure.
- **CF separation** (enhancement): See ADR §Column Family Options. Re-evaluate if log write latency becomes measurable.
