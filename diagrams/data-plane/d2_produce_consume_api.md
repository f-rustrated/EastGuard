# Phase D2: Produce/Consume Client API

**Goal:** End-to-end path from client produce/consume through to local segment files. Sessionized streaming protocol with pipelining and windowing.

**Depends on:** Phase D1 (storage engine).

---

## Produce Protocol

Sessionized and streaming (following Northguard's approach):

1. Producer opens TCP connection, sends handshake with stream ID
2. Broker responds with initial window size (flow control)
3. Producer sends `Append { stream_id, seq, records }` — multiple in-flight without waiting for ack (pipelining)
4. Broker acks committed records: `Ack { ack_seq, updated_window }` — M acks for N appends (batched)
5. Broker only acks records that have been fsync'd (and replicated, once D3 is implemented)

Windowing prevents producer from overwhelming the broker. Broker adjusts window based on backpressure from disk/replication.

## New Actor: DataActor

Follows the existing actor pattern — `DataSender` typed wrapper (like `RaftSender`, `SwimSender`), tokio task, mailbox-driven event loop.

## Produce Routing

1. `hash(topic_name)` → shard group (metadata ownership)
2. Query MetadataStateMachine: `topic_name_index` → `TopicMeta` → `active_ranges` → find range whose keyspace contains `key`
3. Range's `active_segment` → `SegmentMeta.replica_set` → segment leader
4. If this node is segment leader: write locally via `DataActor`
5. If not: forward to segment leader (1 hop max, same pattern as metadata propose forwarding)

## Consume

Client → any replica in segment's `replica_set` → read from segment file.

Response includes:
- Records (batch of payloads)
- `has_more: bool` (more records available at higher offsets)
- `sealed: bool` (segment/range is sealed — no more writes)
- If sealed: `end_offset`, `split_into` or `merged_into` lineage pointers

### Follower Read Safety: Commit Offset

ALL-replica ack means a follower may briefly have records fsynced locally before the leader has confirmed commitment (waiting for other replicas' acks). To prevent consumers from reading uncommitted records, the leader piggybacks a `commit_offset` on each `ReplicaAppend`:

```
ReplicaAppend {
    ...
    records,
    commit_offset: u64,   ← highest offset committed as of this batch
}
```

Followers track `commit_offset` and only serve consumer reads up to it. Cost: one extra `u64` per replication message — negligible. The window between follower fsync and commit confirmation is typically sub-millisecond, so the read delay is invisible in practice.

This is NOT a Kafka-style high watermark — no ISR tracking, no watermark advancement protocol, no consumer-side watermark awareness. It's a single field piggybacked on replication traffic that the leader already sends. Consumers don't see it — they just get records up to the follower's known commit point.

## Metadata Query for Routing

DataActor needs read access to MetadataStateMachine state (topic → range → segment mapping). Two options:

- **Option A:** DataActor queries MultiRaftActor via channel (request-reply, like existing `GetLeader`)
- **Option B:** MetadataStateMachine state replicated to DataActor via applied-entry events

Option A is simpler for Phase D2. Option B may be needed later for performance (avoid per-produce round-trip to Raft actor).
