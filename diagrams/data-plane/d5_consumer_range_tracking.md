# Phase D5: Consumer Range Tracking

**Goal:** Consumers follow range lifecycle transitions (split, merge, seal) without losing messages.

**Depends on:** Phase D4 (segment roll working, ranges actually transition in production).

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

## Metadata Queries

- `ListActiveRanges { topic }` — returns current write-target ranges with keyspace boundaries
- `GetRangeInfo { topic, range_id }` — returns full range state, segment list, lineage, replica set

These query MetadataStateMachine state (read-only, no Raft proposal needed). Any node can serve them by querying its local Raft state machine (follower reads are stale but eventually consistent — acceptable for consumer discovery).
