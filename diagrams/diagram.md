# EastGuard Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  CLIENT                                     │
│                      (TCP, bincode-framed request/response)                 │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            BROKER  (any node)                               │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  SWIM                                                                 │  │
│  │  Gossip-based failure detection (Ping / PingReq / Ack over UDP)       │  │
│  │  Members: Alive | Suspect | Dead    Incarnation numbers for refutation │  │
│  └──────────────────────────────┬────────────────────────────────────────┘  │
│                                 │  NodeAlive / NodeDead events               │
│                                 ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  TOPOLOGY                                                             │  │
│  │  TokenRing: BTreeMap<Token, VNode>    murmur3 with fixed seed         │  │
│  │  1 PNode → 256 VNodes                 Physical-node deduplication     │  │
│  │  token_owners_for(key, n) → Vec<VNode>                                │  │
│  └──────────────────────────────┬────────────────────────────────────────┘  │
│                                 │  hash(topic_id / range_id) → VNode        │
│                                 ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  RAFT-TOPOLOGY BRIDGE                                                 │  │
│  │  Translates SWIM membership events into Raft ConfChanges              │  │
│  │  Owns quorum-loss recovery  (authority: clockwise successor on ring)  │  │
│  │  Drives range transfer on ring reshard                                │  │
│  └──────────────────────────────┬────────────────────────────────────────┘  │
│                                 │  ConfChange / MetadataStateMachine commands         │
│                                 ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  COORDINATOR  (MultiRaft RSM — shared Raft log, one entry per group)  │  │
│  │  Manages topic / range / segment metadata via Raft-committed writes   │  │
│  │  GC ticker: sealed segments past retention → Deleting → unlink        │  │
│  └──────────────────────────────┬────────────────────────────────────────┘  │
│                                 │  read / append / replicate                 │
│                                 ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  STORAGE ENGINE                                                       │  │
│  │  Per-segment: WAL + data.seg (O_DIRECT, append-only, ≤ 1 GB)         │  │
│  │  Shared RocksDB: coordinator state + offset index for all vnodes      │  │
│  │  Write: WAL fsync → data.seg + fence fsync → RocksDB WriteBatch       │  │
│  │  Read:  RocksDB index lookup → seek data.seg → stream records         │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Model

```
Topic
├─ topic_id:       u64
├─ name:           String
├─ state:          Active | Sealed | Deleted
├─ storage_policy: StoragePolicy
└─ ranges:         Vec<RangeId>

Range
├─ range_id:       RangeId
├─ topic_id:       u64
├─ keyspace:       [start, end)
├─ state:          Active | Sealed | Deleting
├─ created_at:     Timestamp
├─ split_into:     Option<[RangeId; 2]>
├─ merged_into:    Option<RangeId>
├─ merged_from:    Option<[RangeId; 2]>
└─ segments:       Vec<SegmentId>

Segment
├─ segment_id:     SegmentId
├─ range_id:       RangeId
├─ topic_id:       u64
├─ state:          Active | Sealed | Reassigning | Deleting
├─ replica_set:    Vec<NodeId>
├─ size_bytes:     u64
├─ created_at:     Timestamp
└─ sealed_at:      Option<Timestamp>

Reassigning state carries: { from: NodeId, to: NodeId }
```

---

## Filesystem Layout

```
/base_dir/
├── node_id                          # persisted UUID
│
├── raft/
│   ├── log/                         # Shared Raft log (MultiRaft)
│   │   ├── 00000001.log             # Entries tagged with group_id
│   │   └── 00000002.log
│   └── snapshot/
│       └── {group_id}/
│           └── {log_index}.snap     # RocksDB::checkpoint() per vnode
│
├── db/                              # Shared RocksDB — all vnodes on this node
│   # MetadataStateMachine state:
│   #   "vn:{vnode_id}:raft"              → HardState (term, voted_for, commit_index)
│   #   "vn:{vnode_id}:t:{topic_id}"      → TopicMeta
│   #   "vn:{vnode_id}:r:{range_id}"      → RangeMeta  (incl. lineage)
│   #   "vn:{vnode_id}:s:{segment_id}"    → SegmentMeta
│   # Offset index:
│   #   "ix:{segment_id}:{offset:020}"    → physical_byte_offset (u64)
│   # Housekeeping:
│   #   "hk:indexed:{segment_id}"         → last_indexed_position
│   #   "hk:raft_offset"                  → persisted_last_offset_raft_log
│
└── segments/
    └── {range_id}/
        └── {segment_id}/
            ├── wal.log              # Write-ahead log (fsynced before data.seg write)
            └── data.seg             # Append-only record file (O_DIRECT, ≤ 1 GB)
```

---

## Write Pipeline

```
1.  Accumulate records into batch buffer
2.  Append batch to wal.log
3.  fsync(wal.log)                        ← WAL is now durable
4.  Fan-out batch to (R - Q) followers    ← parallel, majority quorum
5.  Append Fence record to batch          ← fence.wal_offset = current WAL end
6.  Pad batch to block boundary           ← align_up for O_DIRECT
7.  Write batch to data.seg (O_DIRECT)
8.  fsync(data.seg)                       ← data + fence durable on all quorum nodes
9.  RocksDB WriteBatch (atomic):
        index entries per record
        last_indexed_position = fence position
10. Delete wal.log entries up to fence.wal_offset
11. ACK to producer
```

---

## Crash Recovery

```
1.  Scan data.seg backward → find last valid Fence record
2.  Truncate data.seg to fence position   ← discard partial writes after fence
3.  Read last_indexed_position from RocksDB
4.  Scan data.seg forward: last_indexed_position → fence position
5.  Rebuild missing index entries via RocksDB WriteBatch
6.  Re-apply wal.log entries from fence.wal_offset onward
7.  Append new Fence + fsync(data.seg)
8.  Delete wal.log entries up to new fence.wal_offset
```

---

## data.seg Record Format

```
[crc: u32][type: u8][length: u32][payload: bytes][length: u32]
                                                  ↑ trailing length — enables O(1) backward scan

Record types:  Data | Fence
Fence payload: { wal_offset: u64 }
Alignment:     entire batch padded to BLOCK_SIZE before flush (posix_memalign)
```
