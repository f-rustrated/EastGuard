# EastGuard Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  CLIENT                                     │
│                   (TCP, bincode-framed request/response)                    │
└───────────────────────────┬─────────────────────────────────────────────────┘
                            │  CreateTopic / Produce / Consume / Metadata req
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BROKER (any node)                                 │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                            SWIM                                      │  │
│   │                                                                      │  │
│   │   members: HashMap<NodeId, SwimNode>  (Alive / Suspect / Dead)       │  │
│   │   gossip_buffer: GossipBuffer         (exponential decay)            │  │
│   │   ── Ping / PingReq / Ack ──► (UDP, to peers)                        │  │
│   │                                                                      │  │
│   │   Emits: NodeAlive / NodeDead events                                 │  │
│   └────────────────────────┬─────────────────────────────────────────────┘  │
│                            │  NodeAlive → add to ring                       │
│                            │  NodeDead  → remove from ring                  │
│                            ▼                                                │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                         TOPOLOGY                                     │  │
│   │                                                                      │  │
│   │   TokenRing: BTreeMap<VirtualNodeToken, VirtualNode>                 │  │
│   │                                                                      │  │
│   │   ┌─────────────────────────────────────────────────────────────┐    │  │
│   │   │  Physical Node (PNode)                                      │    │  │
│   │   │  ├─ node_id: NodeId                                         │    │  │
│   │   │  ├─ addr: SocketAddr                                        │    │  │
│   │   │  └─ attributes: HashMap<String,String>  (rack, region, …)   │    │  │
│   │   └─────────────────────────────────────────────────────────────┘    │  │
│   │              │  1 PNode → N VNodes  (default: 256)                   │  │
│   │   ┌─────────────────────────────────────────────────────────────┐    │  │
│   │   │  Virtual Node (VNode)                                       │    │  │
│   │   │  ├─ token: murmur3(node_id + replica_index)                 │    │  │
│   │   │  ├─ pnode_id: NodeId                                        │    │  │
│   │   │  └─ [owns a shard of metadata keyspace]                     │    │  │
│   │   └─────────────────────────────────────────────────────────────┘    │  │
│   │                                                                      │  │
│   │   token_owners_for(key, n) → Vec<VNode>   (replica placement)        │  │
│   └────────────────────────┬─────────────────────────────────────────────┘  │
│                            │                                                │
│            ┌───────────────┴─────────────────┐                              │
│            │  hash(topic_name) → vnode       │  (metadata routing)          │
│            │  hash(range_id)   → vnode       │                              │
│            ▼                                 ▼                              │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                        COORDINATOR                                   │  │
│   │               (Raft-backed RSM, one per VNode leader)                │  │
│   │                                                                      │  │
│   │   ┌─────────────────────────────────────────────────────────────┐    │  │
│   │   │  Topic                                                      │    │  │
│   │   │  ├─ name: String                                            │    │  │
│   │   │  ├─ state: Active | Sealed | Deleted                        │    │  │
│   │   │  ├─ storage_policy: StoragePolicy                           │    │  │
│   │   │  └─ ranges: Vec<RangeId>                                    │    │  │
│   │   └───────────────────────┬─────────────────────────────────────┘    │  │
│   │                           │  1 Topic → N Ranges                      │  │
│   │   ┌─────────────────────────────────────────────────────────────┐    │  │
│   │   │  Range                                                      │    │  │
│   │   │  ├─ id: RangeId                                             │    │  │
│   │   │  ├─ keyspace: [start, end)   (contiguous, no interleaving)  │    │  │
│   │   │  ├─ state: Active | Sealed | Deleting                       │    │  │
│   │   │  ├─ topic_name, created_at, retention                       │    │  │
│   │   │  └─ segments: Vec<SegmentId>                                │    │  │
│   │   └───────────────────────┬─────────────────────────────────────┘    │  │
│   │                           │  1 Range → N Segments                    │  │
│   │   ┌─────────────────────────────────────────────────────────────┐    │  │
│   │   │  Segment                                                    │    │  │
│   │   │  ├─ id: SegmentId                                           │    │  │
│   │   │  ├─ state: Active | Sealed | Reassigning                    │    │  │
│   │   │  ├─ replica_set: Vec<NodeId>                                │    │  │
│   │   │  ├─ start_offset, length                                    │    │  │
│   │   │  └─ created_at, sealed_at                                   │    │  │
│   │   └─────────────────────────────────────────────────────────────┘    │  │
│   │                                                                      │  │
│   │   Operations:                                                        │  │
│   │   ├─ CreateTopic → default Range + Segment                           │  │
│   │   ├─ SplitRange  → seal range, create 2 child ranges                 │  │
│   │   ├─ MergeRanges → seal 2 ranges, create 1 child range               │  │
│   │   └─ NodeDead    → detect under-replicated segments → re-replicate   │  │
│   └────────────────────────┬─────────────────────────────────────────────┘  │
│                            │  read / append / replicate                     │
│                            ▼                                                │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                      STORAGE ENGINE  (fps store)                     │  │
│   │                                                                      │  │
│   │   Per-segment on disk:                                               │  │
│   │                                                                      │  │
│   │   Append path:                                                       │  │
│   │   Record(s) → Batch Accumulator ──► WAL write                        │  │
│   │                  (10ms / size / count)   │                           │  │
│   │                                          ▼                           │  │
│   │                                   Segment File   (O_DIRECT, 1 file   │  │
│   │                                   per segment,   per-segment, ≤1GB)  │  │
│   │                                          │                           │  │
│   │                                          ▼                           │  │
│   │                                    fsync (all replicas)              │  │
│   │                                          │                           │  │
│   │                                          ▼                           │  │
│   │                                   RocksDB Index                      │  │
│   │                                   (SegmentId, logical_offset)        │  │
│   │                                    → file byte offset                │  │
│   │                                          │                           │  │
│   │                                          ▼                           │  │
│   │                                        ACK                           │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘

────────────────────────────────────────────────────────
  Data Flow Summary
────────────────────────────────────────────────────────

  [1] Cluster Formation
      Client/Peer ──UDP──► SWIM ──► Topology (add/remove PNode → VNodes)

  [2] Metadata Request (e.g. CreateTopic)
      Client ──TCP──► Broker (proxy)
                         └── hash(topic_name) ──► Topology
                                                    └── token_owners_for() → VNode leader
                                                                               └── Coordinator
                                                                                    └── creates Topic + Range + Segment

  [3] Produce (write)
      Client ──TCP session──► Broker
                                └── Coordinator: which Segment is active? replica_set?
                                      └── Storage Engine: batch → WAL → file → fsync → index → ACK

  [4] Consume (read)
      Client ──TCP session──► Broker
                                └── Coordinator: which Segment holds offset? replica_set?
                                      └── Storage Engine: RocksDB index lookup (offset → file pos)
                                            └── Segment File read ──► stream Records ──► Client

  [5] Replica Failure & Self-Healing
      SWIM: NodeDead ──► Coordinator
                           └── scan segments with dead node in replica_set
                                 └── pick new broker (satisfies StoragePolicy constraints from Topology)
                                       └── Replication session ──► Storage Engine (new replica)
```

────────────────────────────────────────────────────────
File System Layout  ($DATA_DIR = /var/eastguard)
────────────────────────────────────────────────────────

```
/base_dir/
│
├── node_id                         # persisted UUID (already done in config.rs)
│
├── raft/                           # Raft consensus state — one dir per vnode this node participates in
│   └── {vnode_id}/
│       ├── log/                    # Raft log entries  (bincode-encoded LogEntry files)
│       │   ├── 00000000001.log
│       │   └── 00000000002.log
│       └── snapshot/              # Periodic Raft snapshots (full coordinator state at a log index)
│           └── {log_index}.snap
│
├── coordinator/                    # Applied coordinator state — one RocksDB per vnode
│   └── {vnode_id}/
│       └── db/                    # RocksDB instance
│           # Key schema:
│           #   "t:{topic_name}"          → TopicMeta  (state, policy, range ids)
│           #   "r:{range_id}"            → RangeMeta  (state, keyspace, topic, segments)
│           #   "s:{segment_id}"          → SegmentMeta(state, replica_set, offsets, timestamps)
│
├── segments/                       # Actual record data — one subdir per range
│   └── {range_id}/
│       └── {segment_id}/
│           ├── wal.log             # Write-ahead log (crash recovery)
│           │                       #   sequential appends of serialized records before data write
│           │                       #   truncated/deleted after segment is sealed + fsynced
│           └── data.seg            # Segment file (O_DIRECT, append-only, ≤1GB)
│                                   #   binary records, each with: length | key | value | headers
│
└── index/                          # Sparse offset index — one shared RocksDB for all segments on this node
    └── db/
        # Key schema:
        #   "{segment_id}:{logical_offset:020}" → file_byte_offset (u64, little-endian)
        #
        # Sparse: one entry per batch flush, not per record
        # Lookup: seek to largest key ≤ target offset, then scan forward in data.seg
```

────────────────────────────────────────────────────────
Design Rationale
────────────────────────────────────────────────────────

raft/ vs coordinator/
├─ raft/         = the *log* of mutations (how we got here) — needed for Raft consensus
└─ coordinator/  = the *applied state* (what exists now)    — derived from replaying raft/
kept separately so reads never block on Raft log replay at startup

segments/{range_id}/{segment_id}/
├─ Grouped by range_id so that a range split or merge can be done with a directory rename/move
├─ wal.log lives beside data.seg so crash recovery is local to one directory
└─ wal.log is deleted once data.seg is sealed + fsynced (no longer needed)

index/db  (shared RocksDB, not per-segment)
├─ One RocksDB instance handles many open file handles efficiently
├─ Prefix scan on "{segment_id}:" gives all index entries for a segment in order
└─ On segment deletion, batch-delete all keys with that prefix

Crash Recovery Sequence (on startup)
1. Replay raft/{vnode_id}/log/ into coordinator/{vnode_id}/db/  (if snapshot is stale)
2. For each active segment: check wal.log — if wal.log exists but data.seg is incomplete,
   re-apply WAL records to data.seg and rebuild index entries
3. Truncate/remove any wal.log whose records are fully reflected in data.seg

