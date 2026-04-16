# Raft Storage Architecture — RocksDB Integration

## Three-Layer Design

```
RaftActor
    │  routing, timers, transport — zero storage knowledge
    │  no shard group HashMap; delegates everything to ShardGroupHandle
    │
    ▼
ShardGroupHandle                       ← one instance per node, manages ALL shard groups
    │  owns DB + all Raft instances + all state machines
    │  sole handler for all storage operations
    │
    ▼
Raft                                   ← pure consensus, no generic, no LogStore
    │  step / propose / handle_timeout
    │  all side effects buffered and drained by caller:
    │    take_outbound()           → Vec<OutboundRaftPacket>
    │    take_timer_commands()     → Vec<TimerCommand<RaftTimer>>
    │    take_log_mutations()      → Vec<LogMutation>
    ▼
Vec<LogEntry>                          ← in-memory only, inside Raft
```

**Why one `ShardGroupHandle` per node (not per shard):**
Managing N shard groups requires a single entity with visibility into all of them — to batch writes across shards into
one `WriteBatch` + one fdatasync. A per-shard handle forces the actor to coordinate storage, which leaks storage
concerns upward. One handle owns everything: DB, all Raft instances, all state machines.

**Why `DB` is owned directly (no `Arc`):**
`ShardGroupHandle` is the sole owner of `DB`. There is exactly one `ShardGroupHandle` per node. No sharing → no `Arc`.
`Arc` would only be needed if multiple owners existed; that was an artifact of the per-shard handle design.

**Why `Raft` has no `LogStore` dependency:**
`Raft` is a pure sync state machine. Storage is not a consensus concern. It buffers log mutations the same way it
buffers outbound RPCs — `ShardGroupHandle` drains and persists them. `Raft` never knows a store exists.

---

## Module Dependency Graph

```
actor.rs          ──→  shard_group.rs  (ShardGroupHandle)
                        NO import of state.rs

shard_group.rs    ──→  state.rs        (Raft — no generic, no LogStore)
shard_group.rs    ──→  storage/        (RocksDB, CF ops — internal only)

state.rs          ──→  log.rs, messages.rs
                        NO import of storage/, actor.rs
```

`RaftActor::run` takes one `ShardGroupHandle`:

```
RaftActor::run(
    node_id: NodeId,
    mailbox: Receiver<RaftCommand>,
    transport_tx: Sender<RaftTransportCommand>,
    scheduler_tx: Sender<TickerCommand<RaftTimer>>,
    handle: ShardGroupHandle,          // one handle, all shards inside
)
```

---

## ShardGroupHandle internals

```
ShardGroupHandle {
    db:             DB,                                       // sole owner
    groups:         HashMap<ShardGroupId, Raft>,
    cfs:            HashMap<ShardGroupId, CF>,                // CF handle per shard
    pending:        HashMap<ShardGroupId, Vec<(key, val)>>,  // write buffer per shard
    state_machines: HashMap<ShardGroupId, ShardStateMachine>,
}
```

### Shard lifecycle

```
ensure_group(group):
    db.create_cf(group.id.to_string(), &opts)   // no-op if exists
    cfs.insert(group.id, cf_handle)
    groups.insert(group.id, Raft::new(...))
    state_machines.insert(group.id, ShardStateMachine::default())

remove_group(id):
    groups.remove(id)
    state_machines.remove(id)
    pending.remove(id)
    db.drop_cf(id.to_string())
    cfs.remove(id)
```

**Startup:** `DB::list_cf` recovers existing CF names from MANIFEST. `DB::open_cf` reopens them all. No external shard
ID registry needed.

### step / propose / handle_timeout

These methods only buffer mutations — they do **not** sync. The actor controls when sync happens.

```
step(shard_id, from, rpc):
    groups[shard_id].step(from, rpc)
    // drain log mutations from Raft → buffer into pending[shard_id]
    for mutation in groups[shard_id].take_log_mutations():
        match mutation:
            Append(entry)      → pending[shard_id].push(encode(entry))
            TruncateFrom(idx)  → clear pending[shard_id], buffer a range-delete marker
            HardState(hs)      → pending[shard_id].push((ShardCfKey::HardState, encode(hs)))
    // drive state machine — safe here because commit_index is updated in-memory by Raft
    apply_committed(shard_id)
    // outbound RPCs accumulate in Raft's pending_outbound — NOT sent yet
```

`propose` and `handle_timeout` follow the same pattern: mutate Raft in-memory, buffer log mutations, return. No I/O.

### State machine application

`apply_committed_entries` lives in `ShardGroupHandle`, not inside `Raft`. `Raft` exposes
`take_newly_committed() -> Vec<LogEntry>`:

```
apply_committed(shard_id):
    for entry in groups[shard_id].take_newly_committed():
        match entry.command:
            AddPeer(id)     → groups[shard_id].add_peer(id)
            RemovePeer(id)  → groups[shard_id].remove_peer(id)
            CreateTopic(t)  → state_machines[shard_id].topics.insert(...)
            AssignRange(r)  → state_machines[shard_id].ranges.insert(...)
            Noop            → skip
```

### Snapshots

Snapshot covers **both** Raft metadata and the full state machine state for a shard:

```
ShardSnapshot {
    last_included_index: u64,
    last_included_term:  u64,
    // Raft metadata
    peers: HashSet<NodeId>,
    // State machine state
    topics:   HashMap<TopicId, TopicMeta>,
    ranges:   HashMap<RangeId, RangeMeta>,
    segments: HashMap<SegmentId, SegmentMeta>,
}
```

Stored under reserved keys in the shard's CF (`\x00snap_meta`, `\x00snap_data`). After saving,
`compact_before(last_included_index)` reclaims log storage.

**Why snapshots cover application state:**
The Raft log is a sequence of commands to the state machine. A snapshot is the full serialized state at index N. After
install, the node needs no log entries before `last_included_index` to serve reads or replicate.

**If `compact_before` fails after `save_snapshot` succeeds:**
The snapshot is already durable. Log entries below `last_included_index` remain on disk but are effectively orphaned —
`Raft` will not re-read them because `first_index` has been advanced past them. No correctness violation occurs: the
snapshot is the authoritative source of truth, and the node can still serve reads and participate in replication. The
only consequence is wasted disk space. Log the error and retry `compact_before` on the next snapshot cycle.

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

`ShardGroupHandle` tracks which shards have pending writes. `flush()` is a no-op if nothing is dirty.

### When `RaftActor` calls `flush()`

`RaftActor` drains the mailbox in a batch, then calls `handle.flush()` once before dispatching any outbound. All
responses are sent **after** flush — durability is guaranteed.

```
RaftActor event loop:

    // block on first command
    cmd = mailbox.recv().await
    handle.dispatch(cmd)

    // drain any further ready commands (non-blocking)
    while let Ok(cmd) = mailbox.try_recv():
        handle.dispatch(cmd)

    // one flush covers all shards touched in this batch
    handle.flush()                        // opaque API — RaftActor does not know what this does

    // now safe to send responses
    send_all(handle.take_all_outbound())
    schedule_all(handle.take_all_timer_commands())
```

`flush()` is the only storage-adjacent call `RaftActor` makes. Its implementation is fully hidden inside
`ShardGroupHandle`.

### Cross-shard group commit inside `flush()`

`flush()` combines pending buffers from **all** dirty shards into one `WriteBatch`, then does one `fdatasync`. This is
the implementation detail — invisible to `RaftActor`.

```
ShardGroupHandle::flush():
    if no pending writes across any shard → return early (no-op)
    batch = new WriteBatch
    for each dirty shard_id:
        for (k, v) in pending[shard_id].drain():
            batch.put_cf(cfs[shard_id], k, v)
    db.write(batch, sync=true)            // one WriteBatch + one fdatasync
```

N shards modified in one event loop turn → **one `WriteBatch`** → **one fdatasync**.

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

## LogMutation: Raft's Storage Interface

Raft uses the buffer-drain pattern for all side effects. Log mutations follow the same contract as outbound RPCs:

```
// Raft buffers these — ShardGroupHandle drains and persists
enum LogMutation {
    Append(LogEntry),
    TruncateFrom(Index),
    HardState { term: u64, voted_for: Option<NodeId> },  // term or vote changed
}

Raft::take_log_mutations() -> Vec<LogMutation>
Raft::take_newly_committed() -> Vec<LogEntry>
```

Every `log.push()`, `log.truncate()`, `current_term` change, or `voted_for` change inside `Raft` appends to
`pending_log_mutations`. `Raft` has no import of any storage type.

`ShardGroupHandle` persists `HardState` to RocksDB under `b"\x00hard_state"` in the same `WriteBatch` as log entries —
atomically, in one `flush()`. Hard state and log are never out of sync after a crash.

**Why this matters for correctness:** Raft requires `voted_for` to be durable before sending `RequestVoteResponse` and
`current_term` to be durable before responding to any RPC. Both are covered by `flush()` before `take_all_outbound()`.

---

## Layer Abstraction Boundaries

What each layer is allowed to know:

| Layer              | Knows about                                      | Must NOT know about                        |
|--------------------|--------------------------------------------------|--------------------------------------------|
| `RaftActor`        | `ShardGroupHandle` public API, transport, timers | `DB`, `CF`, `WriteBatch`, `LogMutation`    |
| `ShardGroupHandle` | `Raft`, `DB`, `CF`, `pending`, `LogMutation`     | transport, timer scheduling, mailbox       |
| `Raft`             | `Vec<LogEntry>`, `LogMutation` (own data type)   | `DB`, `CF`, `LogStore`, `ShardGroupHandle` |

`flush()` is the boundary where storage implementation crosses into the actor layer — but only as an opaque API name.
`RaftActor` calls `flush()` without knowing it performs a `WriteBatch` write and `fdatasync`.

---

## Durability Boundaries

| Event                                 | Durability required before                     |
|---------------------------------------|------------------------------------------------|
| `AppendEntriesResponse(success=true)` | Entries written to follower's log              |
| `propose()` on leader                 | Entry durable on leader (counts toward quorum) |
| `install_snapshot`                    | Snapshot durable, `compact_before` called      |
| `RemoveGroup`                         | `drop_cf` completes                            |

`RaftActor` calls `handle.flush()` after draining the mailbox batch, before dispatching any outbound. This single call
covers all durability requirements for all shards in that batch. No response is sent before flush completes.

---

## Open Questions

- **InstallSnapshot RPC** (TODO): `ShardGroupHandle` needs `InstallSnapshot` added to `RaftRpc` for follower catch-up
  after compaction. Scoped to a follow-up.
- **Log compaction trigger**: `ShardGroupHandle` triggers `compact_before` after `save_snapshot` when
  `last_applied - snapshot_index > threshold`. Policy TBD.
- **Safe truncation boundary**: May only compact up to `min(all_replicas.match_index)`. Leader must not compact entries
  a lagging follower still needs unless sending a snapshot instead. Violating this forces a snapshot transfer to the
  lagging follower — expensive but not a correctness failure.
