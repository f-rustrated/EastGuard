# Phase D4: Consumer Range Tracking

**Goal:** Consumers follow range lifecycle transitions (split, merge, seal) without losing messages.

**Depends on:** Phase D3 (segment roll working, ranges actually transition in production).

---

## Consumer Discovery Flow

```
Consumer                          EastGuard Node
   |                                     |
   |── ListActiveRanges(topic) ────────>|
   |<── [range_id=0, keyspace=full] ────|
   |                                     |
   |── Consume(topic, range=0, off=0) ─>|
   |<── records, has_more=true ─────────|
   |   ...                               |
   |── Consume(topic, range=0, off=N) ─>|
   |<── records=[], sealed=true,         |
   |    end_offset=M,                    |
   |    split_into=[range1, range2]  ────|
   |                                     |
   |── Consume(topic, range=1, off=0) ─>|  (left child)
   |── Consume(topic, range=2, off=0) ─>|  (right child)
```

## Response Enrichment

Consume response for sealed/split/merged ranges includes enough metadata for the consumer to navigate the transition without a separate metadata query:

- `sealed: true` + `end_offset` — consumer knows there are no more records
- `split_into: [RangeId, RangeId]` — consumer follows to child ranges
- `merged_into: RangeId` — consumer follows to merged range
- Child ranges start at offset 0 — clean break, no offset mapping

## Follower Read Visibility

Consumers may read from any replica in the segment's `replica_set`, not only the leader. A follower exposes records up to its local `commit_offset` — records past that boundary are physically on disk but not yet visible.

`commit_offset` on a follower advances via two mechanisms (see D2):

1. **Piggybacked on `ReplicaAppend`** — the leader includes the previous batch's `commit_offset` in each replication message. Under continuous produce traffic, the lag is at most one batch interval.
2. **`CommitNotify`** — sent by the leader to followers before ACKing the producer when no next `ReplicaAppend` is imminent. Ensures `commit_offset` is always current before the topic goes idle.

Because `CommitNotify` is sent before the producer ACK, the guarantee is: *producer ACK ⟹ all followers already have the correct `commit_offset`*. A tailing consumer on a follower is never permanently stuck — once the producer is ACKed, `commit_offset` on every follower is already advanced and the consumer's `notify` fires.

## Metadata Queries

- `ListActiveRanges { topic }` — returns current write-target ranges with keyspace boundaries
- `GetRangeInfo { topic, range_id }` — returns full range state, segment list, lineage, replica set

These query MetadataStateMachine state (read-only, no Raft proposal needed). Any node can serve them by querying its local Raft state machine (follower reads are stale but eventually consistent — acceptable for consumer discovery).
