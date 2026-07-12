# EastGuard Metadata Management — Mental Model

A single conceptual reference for how the metadata management system is built and why. Covers the architecture (DS-RSM), the two coordination layers (SWIM and Raft), the entity model (topic / range / segment), the storage layout, and the SWIM ↔ Raft reconciliation that ties failure detection to consensus.

Paired with `metadata_management_roadmap.md`, which tracks the implementation phases.

---

## Why DS-RSM

A metadata system can be built three ways:

1. **External KV store** (etcd, Zookeeper). Adds an operational dependency, becomes a bottleneck, and forces an external coordination service to move data around.
2. **Dedicated controller quorum** (Kafka KRaft). Removes the external dependency but concentrates metadata write load on a small set of nodes. The controller is still a bottleneck and still needs an external service to move data.
3. **Dynamically-Sharded Replicated State Machine (DS-RSM)**. Metadata is hash-partitioned across the cluster. Each partition is its own small Raft group among the nodes that own that partition. No global metadata store, no controller quorum.

EastGuard chooses (3). Every node participates in metadata storage for the partitions it hosts. Adding a node adds metadata capacity proportionally — no controller to upgrade, no quorum to reshape. The system has no "brain" that can hang the cluster.

The trade is coordination complexity: many small Raft groups must agree about their own membership while the cluster as a whole changes shape. The rest of this doc explains how the pieces fit together to make that work.

---

## The two coordination layers

EastGuard separates two responsibilities:

- **SWIM** — cluster-wide membership and failure detection. Lightweight, gossip-based, eventually consistent.
- **Raft, per shard group** — strongly-consistent agreement within a small set of nodes about that group's state (its log, its peer set, its application data).

SWIM answers "is node X alive in the cluster right now?". Raft answers "what is the agreed state of shard group G at log index K?". These are different questions with different consistency requirements, so they use different protocols.

```
                   SWIM (UDP, membership + gossip)
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
           Node A      Node B      Node C
           ┌─────┐    ┌─────┐    ┌─────┐
           │Group│    │Group│    │Group│
           │ #1  │◄──►│ #1  │◄──►│ #1  │   ← one Raft group (TCP)
           │(L)  │    │(F)  │    │(F)  │
           ├─────┤    ├─────┤    ├─────┤
           │Group│    │Group│    │Group│
           │ #2  │    │ #2  │    │ #2  │   ← another Raft group
           │(F)  │    │(L)  │    │(F)  │
           └─────┘    └─────┘    └─────┘
```

Each node runs many Raft groups simultaneously (a leader of some, a follower of others). A single consensus actor multiplexes all of them — one actor, many state machines — to keep lifecycle management simple. The two protocols use separate transports on separate ports (UDP for SWIM, TCP for Raft); they never mix.

---

## SWIM — cluster membership and failure detection

SWIM (Scalable Weakly-consistent Infection-style Membership) addresses the scaling problem of traditional heartbeat protocols: in a cluster of N nodes, every-node-pings-every-other-node is N² traffic. SWIM is O(N) per node per protocol period.

**Failure detection via probes.** Each protocol period, every node picks a random target and pings it. If the target does not reply, the prober asks K other nodes to ping the target on its behalf — *indirect probing*. This separates "the link to me is bad" from "the node is dead": the indirect probes reach the target via different network paths. Only if all indirect probes also fail is the target moved from Alive to Suspect.

**The node state machine: Alive → Suspect → Dead.** Suspect is a probation state — the suspected node gets a chance to refute the suspicion. If it does not refute within a timeout, it moves to Dead. (In EastGuard, a running node can also transition from `Dead` back to `Alive` at a higher incarnation number to heal network partitions.)

**Dissemination via gossip, not broadcast.** Once a node confirms a death, it does not broadcast — it piggybacks the fact on its ongoing probe traffic. "By the way, C is dead" rides as a small footer on the regular ping packet that was going to be sent anyway. The information spreads infection-style through the cluster.

**Incarnation numbers prevent zombie loops.** Without sequence numbering, mutual gossip about a single node's state can oscillate forever ("A says B is dead." "B refutes." "A receives the refutation but its own gossip from earlier is still propagating."). Each node tags its self-claims with a monotonic incarnation counter. Receivers prefer the message with the higher incarnation. For equal incarnations, Dead overrides Suspect overrides Alive.

**Beyond pure liveness.** SWIM also gossips Raft shard-leader information piggybacked on its probes. When a new Raft leader is elected for some shard group, that fact rides on the next gossip round so the rest of the cluster knows who to contact for that group.

For background, see [the SWIM paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf). For operational invariants, see `.claude/rules/swim.md`.

---

## Topology — the hash ring

The topology is the cluster's map of "which nodes own which keys". Every node maintains a local copy, kept in sync with SWIM membership.

**Consistent hashing with virtual nodes.** Each physical node contributes many virtual nodes (vnodes) at hashed positions on the ring. A resource key (e.g., a topic name) hashes to a position; the nearest vnode clockwise from that position determines the owning shard group. The shard group's members are the N distinct physical nodes whose vnodes follow that position around the ring (where N is the replication factor).

Virtual nodes smooth load distribution: with enough vnodes per physical node, key load is roughly even across physical nodes, and adding or removing a node only re-routes a small slice of keys.

**Shard group identity is deterministic.** Given the same ring state, the same resource key always maps to the same shard group. Different nodes computing routing for the same key arrive at the same answer.

**Shard leader map.** Beyond routing, topology tracks the current Raft leader for each shard group (learned via SWIM piggybacked gossip). Used for forwarding a non-leader proposal to the actual leader. Term-monotonic: a higher-term claim wins; equal or lower terms are rejected.

For operational invariants, see `.claude/rules/topology.md`.

---

## Raft per shard group

Each shard group is its own small Raft instance. A node that hosts G groups runs G independent Raft state machines, all multiplexed through a single consensus actor.

**Each instance is fully independent.** Separate term, voted-for, log, commit index, peer set. Processing an event for one group never touches another group's state.

**Raft is transport-agnostic.** The state machine refers to peers by node ID, never by address. The actor / transport layer resolves node IDs to TCP connections using SWIM's authoritative address book.

**Standard Raft mechanics apply**, with one specialization: the log payload is split into application commands (metadata mutations) and consensus-layer commands (peer set changes). Both flow through the log; both apply deterministically on every replica.

**Persistence and apply.** Log entries and durable state (term, voted-for) survive crashes. An entry is not considered applied until it is both committed *and* persisted locally — durability is woven into the apply path so an entry never affects state on a replica that hasn't stored it yet.

**Why a single actor for many groups.** The alternative is one task per Raft instance. With thousands of groups per node, that's thousands of tasks competing for scheduler time. A single actor that processes events serially across all groups keeps lifecycle simple and avoids contention. It can be split later if a single actor becomes a bottleneck.

For operational invariants: per-instance Raft rules live in `.claude/rules/raft.md`; per-actor (multi-group) rules in `.claude/rules/raft-actor.md`; cross-component system rules in `.claude/rules/ds-rsm.md`.

---

## The entity model

Three nested entities form the metadata hierarchy:

- A **topic** owns ranges and tracks topic-level configuration (storage policy, retention, partition strategy).
- A **range** is a contiguous slice of the topic's keyspace. It owns the segments that hold its data and tracks the active write head.
- A **segment** is the unit of storage. Each one is appended to until it reaches a size threshold, then sealed; sealed segments are immutable.

Ownership is structural — ranges nest inside topics, segments nest inside ranges. No back-references. The parent is always known from traversal context. ID counters are scoped to the parent (a topic mints range IDs; a range mints segment IDs).

```
Topic
├── active ranges (ordered by keyspace start)
└── all ranges (active, sealed, deleting)
        │
        Range
        ├── active segment (Option — the write head)
        ├── all segments (active and sealed)
        └── next offset (monotonic write position within this range)
                │
                Segment
                ├── replica set (data plane nodes that hold this segment)
                ├── size (bytes)
                ├── start offset
                ├── end offset (set on seal, immutable thereafter)
                └── timestamps
```

### Lifecycle states

Each entity has its own state machine, but the shape is consistent: an Active entity may transition to Sealed (no new writes, existing data readable), and a Sealed entity may transition to Deleting (data being physically removed). Deletions cascade downward — deleting a topic marks all its ranges and segments for deletion.

### How range boundaries are determined

A range's keyspace is set by the operation that creates it:

- A new topic gets one initial range covering the entire keyspace.
- A split takes a parent range and produces two children at a chosen split point. Parent → Sealed, both children → Active.
- A merge takes two buddy ranges (adjacent in keyspace, children of the same parent split) and produces a single merged range covering their union.

Users never specify ranges, range IDs, or split points. The system handles all of it.

### Why the entire topic subtree lives in one shard group

Hashing the topic name deterministically routes the topic — *and* all its ranges and segments — to a single shard group. Two different shard groups can never own the same topic. This makes single-topic operations atomic: creating a topic creates the topic record, its initial range, and its initial segment in one log entry on one Raft group.

Cross-topic queries (e.g., list all topics) require scatter-gather across shard groups. Rare admin operation; not hot path.

### The state machine is purely passive

The metadata state machine never originates work. It applies committed log entries, mutating in-memory structures deterministically. The leader may *decide* to propose work (e.g., split a hot range), but the decision goes through the log like everything else, and the state machine on every replica applies the same committed entry the same way.

For operational invariants, see `.claude/rules/metadata-state-machine.md`.

---

## Storage layout

One shared key-value store holds the persisted state for every shard group on the node. Per-group isolation is achieved by key *prefixing* rather than per-group databases.

```
[group_id: 8 bytes BE][key_type: 1 byte][optional index: 8 bytes BE]
```

| Key type | Prefix | Suffix | Purpose |
|---|---|---|---|
| Log entry | 0x01 | log index (BE) | Raft log entries |
| Hard state | 0x02 | none | Term, voted-for — must survive crashes |
| Snap meta / snap data / applied index / epoch | 0x03 – 0x06 | none | Reserved for snapshot subsystem |

**Why a single shared store.** The alternative is one column family per shard group. With ~768 groups per node, each per-group create incurs an fsync on the underlying metadata file — together that's minutes of startup latency. A single shared store with composite keys eliminates the per-group bookkeeping entirely.

**Why big-endian byte ordering.** Keys are sorted lexicographically by the underlying store. Big-endian numeric encoding makes lexicographic order match numeric order. Group 1's keys cluster contiguously; group 7 follows; within each group, log entries sort by index. This enables efficient prefix-bounded range scans for log lookups and group-scoped deletes.

**Atomic batched writes.** A single batch persists all dirty groups' log mutations and hard-state updates together. Either everything in the batch lands or nothing does. A group's *stable index* — the highest log entry guaranteed durable — only advances after the batch succeeds.

For operational invariants, see `.claude/rules/storage-layout.md`.

---

## Eventual consistency is delayed truth, not fake truth

A common misread of "eventually consistent" is "approximate" or "best-effort". SWIM is neither. With incarnation numbers and the suspect-then-confirm machinery, SWIM's view is **eventually correct**. While a node marked dead is treated as dead for ongoing operations, EastGuard allows **partition healing**: a running node that receives gossip claiming it is dead refutes the claim by incrementing its incarnation number, resurrecting itself back to Alive.

Therefore, a node confirmed dead really is dead in the absence of a healed partition. When a partition heals, the resurrected node's higher incarnation overrides the terminal Dead state and the cluster adapts.

This matters because it justifies trusting SWIM's *current snapshot* as a basis for action. We are not acting on a guess; we are acting on a truth that arrived with delay.

---

## Raft is a role-gated converter of SWIM facts into group truth

Two kinds of truth live in the system:

- **SWIM truth** — facts about cluster membership ("X is dead"). Eventually consistent.
- **Raft truth** — facts about a shard group's committed state ("X is no longer in this group's peer set at log position K"). Strongly consistent.

Raft's job for membership is to *convert* SWIM truths into Raft truths by writing them to the log. The conversion is **role-gated**: only the leader can propose. Followers can apply what the leader proposed, but they cannot originate the conversion themselves.

In the normal case, when SWIM detects a death, the event is delivered to every member of the affected group. The one that happens to be leader translates it into a membership-removal log entry, replicates it, and once committed every replica applies it. The SWIM truth has become a Raft truth on every replica.

The interesting case is when the event arrives at every member but **no member holds the authority to convert it** — typically because the previous leader is the one that just died. Every member receives the death event, every member is a follower, every member can do nothing with it. The fact is still true in SWIM. It just did not make it into the Raft log.

After that, gossip moves on. SWIM considers the event already disseminated and will not re-fire it. The fact is now stuck outside Raft — true in the world, absent from the group's committed state.

---

## Reconciliation = refer back to SWIM when your role finally allows you

One-line rule:

> When a node gains the authority to convert (becomes leader), refer back to SWIM and act on facts it could not convert before.

That is the reference-back step. Whenever a node becomes leader of a group, it:

1. Asks SWIM for the current snapshot of live cluster members.
2. Compares against the group's current peer set.
3. For each peer SWIM no longer considers alive, proposes a removal entry into the Raft log.

Note what this is *not* doing: it is not replaying missed events; it is not waiting for re-trigger. It is rereading SWIM's current state and acting on it now that the new leader can. Eventual consistency guarantees that current state is real.

So the two paths look like:

| Path | Trigger | Acts on |
|---|---|---|
| Event-driven (primary) | A SWIM event fires while a leader exists | Just that event |
| State-driven (backup) | A node becomes leader | SWIM's current snapshot — covers anything missed |

Critical point here is that both end in Raft proposals. Both go through commit. Neither lets SWIM update the peer set directly.

**Why tie state-driven to leadership transition specifically?** Because that is when missed conversions are most likely to be sitting around — the typical missed event is "the previous leader died". A periodic check on the sitting leader would catch slower drifts as well; it is just not built. Both are valid implementations of the same "refer back" rule.

---

## Scenarios

### A — 5-node group survives a double death (reconciliation does the work)

Group of five members, quorum = 3. The leader and one follower die together.

```
T=0   Two members die simultaneously.

      The death events fire on every surviving member.
      All survivors are followers at this instant → none can convert.
      The SWIM truth "two members dead" exists, but no node held authority to convert it. Gossip moves on.

T=Δ   Election: one of the three survivors wins (3 votes ≥ quorum).

      The new leader inherits a peer set that still lists the two dead members.

      Reference-back step:
        Snapshot SWIM live set (restricted to this group's members) → three survivors
        peer_set \ live → the two dead members
        Propose a removal entry for each.

      Both commit via the three live nodes. All apply. The peer set now lists
      only the survivors.
```

Without the reference-back step, the SWIM truth never makes it into Raft truth. With it, the new leader rereads SWIM the instant it gains authority, and the conversion happens.

### B — 3-node group, single leader death (reconciliation is hygiene)

Three-member group, the leader dies. The two survivors elect one of themselves.

The SWIM truth "leader is dead" was unconverted while both survivors were followers. The reference-back step on the new leader converts it: a removal entry commits via the two live nodes.

Even without reconciliation, the group functions — commit math against the unpruned peer set still reaches quorum via the two live nodes. So reconciliation here just stops wasted heartbeats to a dead address. Useful, not load-bearing for correctness.

### C — what reconciliation cannot rescue

Three-member group, quorum = 2. Two members die together. Only one survives.

```
The survivor tries to elect: 1 vote (self) < quorum(2). Election fails forever.
No leader ever exists for this group again.
Reconciliation never fires — it is gated on becoming leader.
```

This is unrecoverable from INSIDE Raft. Reconciliation is *only* useful when a leader still exists; it cleans up after losses the group survived, not losses the group did not.
And this is the point where SWIM becomes so useful again.

---

## Why a removal trigger FROM SWIM must come with an addition

Reconciliation as described above removes dead peers from the membership. That shrinks total membership. That shrinks quorum. That **reduces the number of future failures the group can tolerate.**

Walk the failure-tolerance budget down on a 5-node group:

```
Initial    members=5   quorum=3   tolerates 2 deaths
A dies, reconciled out:
After      members=4   quorum=3   tolerates 1 death
B dies, reconciled out:
After      members=3   quorum=2   tolerates 1 death
C dies, reconciled out:
After      members=2   quorum=2   tolerates 0 deaths
D dies:                            GROUP DEAD
```

Each removal consumes one slot of failure budget without replenishing it. Pure reconciliation walks the group toward fragility, not robustness.

The proper paired operation:

> For every removal of a dead member, propose an addition of a healthy replacement. The replacement comes from the hash ring — pick a node not currently in the group, prefer ones with available capacity.

Two log entries, applied in order. The group passes through an intermediate state (one fewer member) before reaching the new stable state (replacement in place). Both states are quorum-decidable, so the transition is safe.

Without the addition half, the failure budget shrinks monotonically. With it, it stays constant.

**This codebase currently implements only the removal half.** The addition-replacement step is future work — it needs replacement-selection logic on the hash ring and coordination with segment-level data movement. Until that lands, a long-running cluster will see its groups shrink monotonically; monitoring should surface this until the runtime can self-correct.

---

## What to internalize

1. **DS-RSM removes the "brain".** No global metadata store, no controller quorum. Every node owns a slice of metadata; the cluster scales by adding nodes, not by upgrading a central component.
2. **Two coordination layers, two consistency models.** SWIM is eventually consistent and gives cluster-wide membership truth. Raft is strongly consistent and gives per-group state truth. They serve different questions.
3. **SWIM facts are real, just delayed.** Eventually consistent ≠ approximate.
4. **Raft converts SWIM facts into group-committed state.** The conversion is role-gated — only the leader can originate it.
5. **When a fact arrives and no node holds authority to convert it, the fact stays outside Raft.** Gossip will not re-fire it.
6. **Reconciliation = refer back to SWIM when your role allows you.** The new leader rereads SWIM's current state and proposes for anything missed.
7. **Reconciliation alone shrinks the group, consuming failure budget.** Pair every removal with an addition of a replacement to keep the budget intact.
8. **Some failures (quorum loss before a leader can act) cannot be repaired by any in-protocol mechanism.** Reconciliation does not pretend otherwise; external action is required.
9. **The metadata state machine is purely passive.** It never originates work, only applies committed log entries. Even leader-initiated work (split, merge, seal) goes through the log so every replica applies it the same way.
10. **The entire topic subtree lives in one shard group.** No cross-shard coordination for single-topic operations.
11. **Storage isolation is by key prefix, not by per-group databases.** Big-endian encoding makes lexicographic order match numeric order; group-scoped scans and deletes are single range operations.
