# EastGuard Metadata Management Roadmap

EastGuard avoids both a global metadata store (etcd, Zookeeper) and a dedicated controller quorum (Kafka KRaft). Both add complexity, become bottlenecks, and require external coordination to move data around. Instead, every node participates in metadata storage through a **Dynamically-Sharded Replicated State Machine (DS-RSM)**: metadata is hash-partitioned across the cluster, each partition is its own small Raft group, and cluster membership is maintained by SWIM gossip.

This roadmap walks the path from raw consensus + membership primitives to a working metadata system that manages topics, ranges, and segments.

For the SWIM ↔ Raft interplay (how SWIM facts become Raft truth, why membership changes go through the log, and the brittleness of reconciliation without rebalancing), see `mental-model.md`.

---

## Phase 1 — Data Model + Metadata State Machine ✅

**Goal.** Define the entity model (topics, ranges, segments — their states and lifecycle transitions) and a pure, in-memory state machine that mutates them. No I/O, no async — a deterministic function from "current state + command" to "new state".

**Shape.**
- A *topic* owns *ranges*; each range covers a contiguous slice of the keyspace.
- A *range* owns *segments*: the active write head plus all sealed segments before it.
- Ownership is structural — ranges nest inside topics, segments nest inside ranges. No back-references.
- ID counters are scoped to the parent: a topic mints range IDs, a range mints segment IDs.

State enums distinguish Active / Sealed / Deleting transitions. The state machine itself is purely passive: it never originates work, only applies committed log entries.

**Depends on.** Nothing.

---

## Phase 2 — Outer Log Payload ✅

**Goal.** Define what kinds of things can be replicated through the Raft log.

**Shape.** Two categories of payload, plus a no-op variant:
- **Metadata commands** — create topic, roll segment, split range, merge range, delete topic. These mutate the application state machine on apply.
- **Membership changes** — add or remove a peer from a shard group's Raft set. These mutate the consensus layer's own peer set on apply.
- **No-op** — used for empty entries (e.g., the first entry a new leader appends to safely commit any prior-term entries).

The peer set is part of the replicated state machine; it must mutate *only* through committed log entries, never through direct gossip-driven mutation. The rationale and the failure scenarios are in `mental-model.md`.

**Depends on.** Phase 1.

---

## Phase 3 — Application State Machine Dispatch ✅

**Goal.** Wire the metadata state machine into Raft's apply path so that committed log entries deterministically advance metadata state on every replica.

### 3a — Direct ownership

Raft owns the metadata state machine. EastGuard is a metadata system, not a generic Raft library — exactly one state-machine type ever exists. No trait, no generic, no plug-in interface.

### 3b — Apply at the consensus boundary

When the log advances and an entry is both committed AND durable, Raft dispatches by payload type: metadata commands go to the state machine's apply step; membership changes mutate the peer set; no-ops do nothing. Apply runs inside Raft itself, guarded so an entry is never applied before it is both committed and stable on disk.

### 3c — Read-only access for tests

A non-mutating accessor lets tests assert the applied state without exposing mutation paths.

### 3d — Membership Changes Through the Log

Every change to a Raft group's peer set is a log entry. SWIM detects node up/down; the shard group leader observes the event and proposes the corresponding membership change; followers apply on commit. Direct gossip-driven mutation of the peer set is forbidden — divergent peer sets across replicas would let two leaders compute different quorums for the same log index and admit a split-brain commit.

When a node becomes leader, it performs a reference-back step: snapshot the current SWIM live set and propose removals for any peer no longer alive. This closes the gap where a leader died before it could itself convert a death event into a Raft log entry.

A removal must be paired with an addition of a replacement — otherwise the group's failure tolerance shrinks monotonically. The replacement-selection half is not yet built; current reconciliation is the first half of a two-step pattern. See `mental-model.md` for the mental model, the failure scenarios, and what the missing half should look like.

**Depends on.** Phase 2.

---

## Phase 4 — Client Propose Pathway ✅

**Goal.** End-to-end path for a client request: client → routing → propose → commit → apply → reply.

**Shape.** Clients submit operations keyed by a resource (e.g., topic name). The receiving node hashes the key to find the owning shard group. If the local instance is leader, it proposes to the group's Raft log and waits for commit before replying. If not leader, the node returns a redirect hint to the client (or forwards transparently — see Phase 5).

Commit-completion tracking lives in the consensus actor: when a proposal is accepted into the log, the actor records the log position and a reply channel. When apply reaches that position, the reply fires with success. On leader stepdown, all pending replies are drained with a not-leader error so callers don't hang.

**Depends on.** Phases 1–3.

---

## Phase 5 — Leader Forwarding + Shard Discovery ✅

**Goal.** Non-leader nodes forward proposals transparently to the leader so clients don't always need to know who the leader is. Clients can also explicitly discover shard-to-leader mappings.

**Shape.**
- A not-leader response carries a leader hint where available. Non-leader nodes can use that hint to forward the proposal to the actual leader over TCP. Forwarding is capped at one hop so a misrouted proposal can't bounce around the cluster.
- A query interface lets a client ask "for this resource key, which shard owns it and who's currently leading?" in a single round trip to SWIM, which already tracks both the hash ring and shard-leader gossip.
- Epoch validation was originally planned and cancelled: SWIM converges fast enough that stale routing is self-healing via the not-leader path. Revisit only if client-side caches become the primary routing source.

**Depends on.** Phase 4.

---

## Phase 6 — Hot Range Detection + Auto-Split / Merge ✅

**Goal.** The system detects ranges with disproportionate load and rebalances them through splits and merges, without external metrics infrastructure.

**Core insight.** Segment rolls are already in the Raft log. Carrying their cause turns existing lifecycle traffic into an ordered load signal: size means pressure, age means idle, and failure/recovery say nothing about demand. No metrics pipeline, probe protocol, or key histogram is required.

**Decision logic.** Each committed active-segment roll carries its cause. Size-limit rolls advance a consecutive pressure streak; age-limit rolls reset pressure and mark the range idle; failure and recovery rolls are neutral. At the pressure threshold the leader proposes a split at the keyspace midpoint. Committing the second idle signal for adjacent ranges queues a merge immediately; a periodic scan remains as a recovery fallback. New split children and merged ranges start unclassified, so absence of observations cannot trigger a merge. Range-level split/merge seals and their boundary-correction plumbing stay outside load classification. See [Event-Driven Range Load Classification](event-driven-range-load.md).

**Anti-recursion.** The apply path never proposes. Auto-proposals are buffered during apply and drained at the start of the next flush cycle, so a split apply can't recursively trigger another split. Stale auto-proposals (e.g., a merge proposed but the range was already split) are rejected by apply-time precondition checks.

Midpoint splitting is good enough for now. Percentile-based split points are in the backlog.

**Depends on.** Phases 1–4. Phase 5 not required.

---

## Phase Dependency Graph

```
Phase 1 (Data Model + Metadata State Machine)        ✅
    │
    ▼
Phase 2 (Outer Log Payload)                          ✅
    │
    ▼
Phase 3 (Application State Machine Dispatch)         ✅
    │
    ├────────────────────────┐
    ▼                        ▼
Phase 4 (Propose Pathway)   Phase 6 (Hot Range Detection ✅)
    │
    ▼
Phase 5 (Leader Forwarding + Shard Discovery)        ✅
```

---

## Backlog

### Hot range detection — better signals

Add only when midpoint splitting proves insufficient:

- **Percentile-based split points** from a key histogram. Better for skewed workloads but requires sampling infrastructure and a probe protocol.
- **Write throughput as a signal**, alongside or instead of seal frequency. More granular; requires data plane reporting.
- **Adaptive thresholds** per topic or per range, based on historical patterns.
- **Predictive splitting** from load-trend detection.

### Consumer protocol for range lifecycle

Out of metadata-management scope, but depends on metadata exposing:

- Final offsets on sealed segments so a consumer knows when it has drained a range.
- Lineage of splits and merges so a consumer can follow a range through its transitions.
- The current set of active ranges so a consumer can find write targets.

### Snapshot subsystem

Durable log persistence is implemented (shared key-value store with composite keys per group, atomic batched writes with fsync, hard state persistence, startup recovery). Remaining: install-snapshot RPC, snapshot creation and restore for the metadata state machine, log compaction, and applied-index persistence. Logs grow unbounded until snapshots land — acceptable for the current scale.

### Membership rebalancing — the missing addition half

Pair every removal with an addition of a replacement, so failure tolerance does not shrink monotonically. The same pattern applies at two layers, both unfinished in different ways:

**Raft-group peers.** Reconciliation currently proposes only removals — both on leader takeover (against SWIM's live set) and on live-leader node death. Each unpaired removal consumes one slot of failure budget; the group walks toward fragility. The missing piece selects a replacement node not already in the group (preferring available capacity) and proposes its addition alongside the removal. See `mental-model.md` for the full argument and the failure scenarios that motivate it.

**Data-plane segment replica sets.** A node's death also touches every active segment that listed it as a replica. The live-leader path already proposes segment rolls that swap the dead node for a healthy replacement, preserving the replication factor. The takeover path does not: deaths that fell into the no-leader gap leave their segments under-replicated until the next ordinary roll — or indefinitely, for segments that take no further writes. The missing piece is the takeover-time counterpart of the live-leader behavior: on becoming leader, find active segments whose replica set still names a node SWIM considers dead and propose rolls that replace it. Sealed-segment repair (restoring redundancy on already-immutable data) is a related but distinct concern at the data-plane layer — see `docs/data-plane/d2_segment_replication.md` §F3.

The two layers should share one replacement-selection policy. Otherwise the same node death produces different replacements for a topic's Raft group and for that topic's segments, with no design reason — just two independent placement heuristics drifting apart.

---

## Key design decisions

1. **Routing lives inside the consensus actor — no separate broker layer.** The actor already holds the per-group Raft instances and can check leadership directly. Hashing a resource key to a shard is a pure function, so routing needs no asynchronous query to gossip.

2. **The metadata state machine lives per shard group, owned by the per-group Raft instance.** Raft here is not a generic consensus library — it is the consensus core of a metadata system. Direct ownership, no traits.

3. **Apply runs inside Raft, not in a buffer drained by the actor.** Side effects (outbound packets, timer commands, log mutations) are buffered for the actor to drain. The application-state apply is not — it happens at the consensus boundary, guarded by both commit and durability.

4. **Commit completion is tracked by log position.** A proposal records its log index plus a reply channel. When apply reaches that index, the reply fires. Leader stepdown drains all pending replies with a not-leader error.

5. **Client-redirect first, transparent forwarding second.** Phase 4 returns a not-leader hint so clients can redirect; Phase 5 added one-hop forwarding so they don't have to. Both paths coexist.

6. **Nested ownership — no flat maps, no back-references.** Ranges live inside topics; segments live inside ranges. Counters scoped to the parent. Deleting a topic is dropping the topic record; the rest cascades structurally.

7. **Segment offsets enable consumer position tracking.** Each sealed segment knows where it ended; consumers know they are done with a range when their position reaches the last segment's end. On split or merge, children start at offset 0 — a clean break from the parent's offset space.

8. **Committed segment-roll causes are the range-load signal.** Size and age rolls already pass through the log, so explicit causes distinguish pressure from idle without a metrics pipeline; failure and recovery remain policy-neutral.

9. **Membership changes go through the log, never via direct mutation.** Detailed in Phase 3d and `mental-model.md`.

---

## Risk areas

| Risk | Mitigation |
|---|---|
| Pending reply channels leak on leader stepdown | Drain and error all pending on stepdown. |
| Hash ring divergence between client and server | Server resolves routing from SWIM (fast convergence). Leader forwarding handles misrouted proposals. |
| Log entry format changes on payload variants | A version byte on log entries when bincode-incompatible changes are needed. |
| Midpoint split does not balance skewed workloads | Acceptable for now; percentile-based split is in the backlog. |
| Pressure streak grows unbounded on long-lived ranges | The committed streak saturates at the split threshold. |
| Two leaders coexisting briefly with divergent peer sets | Membership goes through the Raft log — quorum size advances deterministically with the log index. Split-brain commit is structurally impossible. See Phase 3d. |
| New leader inherits dead peers because the predecessor died before converting the event | Reference-back step on becoming leader (Phase 3d; detailed in `mental-model.md`). |
| Reconciliation shrinks the group's failure budget | Pair every removal with an addition — at both the Raft-peer and segment-replica-set layers. Not yet built. |
| Quorum loss before a leader can act on it | Not recoverable from inside Raft. Requires external intervention (manual reconfiguration or a future external recovery protocol). |
