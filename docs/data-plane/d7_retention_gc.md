# Phase D7: Retention GC — optional age-based bounding of a topic's history

**Goal:** give topics an *optional* cap on how long they keep sealed history, by **age**,
and reclaim the segments past it. EastGuard's default is to keep everything; D7 adds
opt-in time retention for topics that want a bounded history window, deleting whole
sealed segments oldest-first through the metadata log. The active segment (the write
head) is never deleted.

Retention here is purely **logical** (an age policy on a topic's data). It is *not* a
disk-capacity mechanism — bounding a node's physical disk is a separate, node-level
concern (see "Disk capacity is a separate concern" below). There is deliberately no
size-based retention: a range that grows simply **rolls** a new segment (the existing
`segment_size_limit_bytes` seal trigger); growth is never a reason to *delete* data.

**Depends on:** D3 (coordinator-driven segment lifecycle), D5 (crash recovery + orphan
GC — reused here as the deletion backstop).

---

## Retention is optional by design

EastGuard scales storage **horizontally**: capacity is added by adding nodes, not by
throwing data away. New segments are placed by the hash ring, and ranges auto-split
under load, so a growing topic spreads its new segments across new capacity rather
than piling onto one node. **Unbounded retention is a supported, first-class mode** —
a topic with no retention policy keeps all its history forever, and that's a correct
way to run.

So retention here is not a rescue from inevitable disk death; it's a **per-topic
opt-in** for workloads that explicitly want a bounded history window (a time-limited
audit log, a replay buffer of the last N hours). It defaults to off:

- `retention_ms = None` → no age cap; keep all history forever (the default).

With it unset, nothing is ever expired — exactly today's behavior, now an explicit
choice rather than the only choice.

---

## Granularity: whole segments (one file each)

A **segment is one file on disk** — the unit the broker stores and the unit it
deletes. A segment holds many **entries**, each an opaque blob the producer wrote
(carrying a `record_count`); the broker stamps an `entry_id` per entry and never parses
the records inside. The entity hierarchy is **topic → range → segment (file) → entry**.

Retention deletes whole segments, never individual entries — every entry in a segment
shares one lifecycle. (The broker has no per-entry delete at all; an entry's only path
out is its segment file being removed.) This is the conventional unit:

- **Kafka** retains at **segment** granularity — a partition's log is a chain of
  immutable segment files, and time/size retention drops whole files oldest-first. It
  cannot delete individual records (log compaction is a separate mode entirely).
- **Pulsar** retains at **ledger** granularity (a topic is a sequence of BookKeeper
  ledgers); retention drops whole ledgers once consumed and past policy.

The mapping: an EastGuard **segment** ≈ a Kafka segment file / Pulsar ledger; an
EastGuard **entry** ≈ a Kafka record batch (one produced blob); an EastGuard **range**
≈ a Kafka partition. Retention is configured on the topic and enforced **per range** —
each range's segment chain is bounded independently, exactly as Kafka applies a topic's
`retention.*` per partition.

---

## The expiry rule (age only)

A sealed segment is expired when `sealed_at + retention_ms < now`. `sealed_at` — the
moment the segment became immutable — is the right clock-start (the active head has
none and is never expired). Expired segments of a range are deleted **oldest-first**
(see below). There is no size-based variant: a range over a size threshold rolls, it
doesn't shed history.

**The wall clock is contained.** Retention is the one age-based decision in the system,
and it's confined so it threatens neither correctness nor testability:

- **No clock agreement needed.** The decision is leader-only and the resulting command
  names concrete segment ids, so cross-node skew can't make replicas diverge — at
  worst it shifts the floor by the skew margin, harmless for a *floor*. Same
  leader-only-decision / deterministic-apply shape as split/merge
  (metadata-state-machine.md #16).
- **One injectable read.** The expiry decision is a **pure function** `(sealed
  segments, retention_ms, now) → deletable prefix`. Production passes the system clock
  for `now`; tests pass a fixed value. Nothing in the apply path or state machine ever
  reads the clock.

---

## Disk capacity is a separate concern

It is tempting to also cap *bytes*, but byte limits don't belong here. A node's
physical disk is observable only by **the node** (the data plane) — the coordinator
has no view of it — and the right response to a full disk is **stop writing, keep
reading**, not delete a topic's logical history. That is backpressure and placement,
not retention, and it's a different subsystem:

- **Detection: data plane.** The node watches its own disk (segment files + WAL vs
  capacity) and crosses a high-water mark.
- **Reaction: no new writes, reads continue.** Segments it currently leads can't grow,
  so the next produce's fsync fails and the existing write-path-timeout → seal → relocate
  path moves them elsewhere (the "leader disk failure" case). For *new* segments, the
  node advertises an at-capacity bit through SWIM (coarse gossip, like membership), and
  capacity-aware placement excludes it — mirroring `SealRequest`: data plane detects,
  coordinator acts.
- **Relief.** Time retention (this doc) frees space where a policy is set; tiered
  storage (backlog) offloads cold segments without deleting; added nodes absorb new
  ranges.

So: **age retention is a coordinator decision about logical data; disk capacity is a
data-plane decision about physical bytes.** This doc is the former; the latter is its
own design (node capacity / write backpressure).

---

## Who decides

The **coordinator** (the shard group's Raft leader — the authority that already
proposes `RollSegment` and `ReassignSegment`) runs a periodic **retention sweep**,
leader-only, on the existing ring-check cadence. For each range it owns it computes the
deletable prefix by age and proposes a metadata command naming those segments. Apply on
every replica is identical because the command carries ids, not predicates.

---

## The `DeleteSegments` command

There is no segment delete today — only the topic-level `DeleteTopic` cascade. D7 adds
one metadata command that marks an oldest-first **prefix of one range's sealed
segments** `Deleting`. It is plural by nature: a retention sweep expires a *run* of old
segments, not one, so the command carries the whole prefix — `{ topic_id, range_id,
segment_ids }` (oldest-first) — and marks them in one atomic apply. (Every other
segment command touches a single entity; retention is the only set-valued one.)

| Aspect | Behavior |
|---|---|
| Scope | One range; `segment_ids` is a contiguous oldest-first prefix of that range's sealed segments. Multiple ranges → multiple commands. |
| Effect | Each named segment that is `Sealed` transitions `Sealed → Deleting`; nothing else changes — offsets, lineage, `replica_set`, timestamps stay frozen, exactly as `ReassignSegment` leaves them (metadata-state-machine.md #21). |
| Skips, not rejects | An `Active` (write head) or already-`Deleting`/absent id is skipped, not an error — so apply can never delete the write head and is idempotent (sweep re-runs, duplicate dispatch). No-op'd entirely → `Noop`. |
| No production prefix check | The oldest-first/no-hole property is guaranteed by the sweep's prefix construction and enforced as a *structural invariant* (`assert_retention_prefix`, #2), not re-validated on the apply path — a non-prefix would be a proposer bug, which the invariant catches in test/debug (cf. metadata-state-machine.md #11). |

**Mark, don't remove.** The entity stays in metadata in the `Deleting` state rather
than vanishing. Keeping it preserves the range's offset chain (a `Deleting` segment
still carries its offsets, so offset-continuity — metadata-state-machine.md #8 — is
untouched) and gives the data plane a concrete target to reclaim. Reclaiming the
metadata *entity* (to bound metadata RAM and the Raft log) is a separate concern for
Raft snapshotting/compaction, out of scope here — D7 bounds **disk**.

---

## Oldest-first: no holes

Deletion within a range is strictly **oldest-first**: a segment may be deleted only
once every lower-offset segment in its range is already `Deleting`/deleted. The
surviving segments of a range are therefore always a **contiguous suffix** ending at
the active head — never a hole in the middle.

This is what keeps consumption safe. A consumer reading a range forward crosses the
retained boundary exactly once: everything below it is gone, everything at and above it
is present. There is no "deleted s1, present s2, deleted s3" state for a reader to fall
into. The sweep produces a prefix naturally (it take-whiles expired sealed segments in
segment_id order), and the property is pinned as the structural invariant
`assert_retention_prefix` (#2) — so a proposer bug that named a non-prefix set is caught
in test/debug rather than guarded on the hot path (cf. metadata-state-machine.md #11:
precondition failures are a proposer bug, not a state-machine error).

---

## Reclaiming the files

A committed `DeleteSegments` reaches the data plane the same way every committed segment
decision does — the leader dispatches on the post-commit drain (raft-actor.md #3). The
dispatch **groups the deleted segments by `replica_set`** and sends one batched
`DeleteSegments` per distinct set: segments in a range can sit on different sets
(seal-on-failure, reassign, and re-fill move them), but the common case (a range's
segments sharing replicas) collapses to a single message. Each replica, per key it
holds, deletes the segment file, drops its in-memory cache, and range-deletes its
sparse-index entries (skipping any key it no longer holds — idempotent).

```
Coordinator (Raft leader)                     A replica_set's nodes
   |  retention sweep: oldest sealed segments past sealed_at + retention_ms
   |  propose DeleteSegments{ range, ids oldest-first } ──Raft──> committed on [A,B,C]
   |
   |  (post-commit drain, leader only; group keys by replica_set)
   |── DeleteSegments{ keys for this set } ─────> that set's nodes
   |                                              per key: delete file + cache + index
```

**Orphan GC is the backstop.** Dispatch is fire-and-forget, so a replica might miss a
delete (dropped message, restart mid-delete). That's fine: a file the cluster no longer
assigns to a node is, by definition, a stray, and the D5 orphan sweep reclaims strays
lazily and cluster-confirmed. The explicit dispatch is the fast path; orphan GC is the
eventual guarantee — no single dropped message strands a file. (Same shape as the
catch-up design, raft-actor.md #9: an eager one-shot dispatch over a lazy net.)

---

## Interaction with consumers

Retention can delete a sealed segment a slow consumer hasn't finished reading — the
classic retention-vs-lag race. Because retention is opt-in, a topic that can't tolerate
it simply doesn't set a policy. For topics that do, a consumer that falls past the
window sees "segment gone" on its next fetch and skips forward to the oldest surviving
segment (`ListOffsets` gives the surviving start). Retention is a capacity guarantee for
the cluster, not a delivery guarantee for laggards — operators size the policy against
worst-case lag.

---

## Testing

The clock is the only difficulty, and it's confined to one injectable input:

- **Expiry decision, pure unit.** `(sealed segments, retention_ms, now) → deletable
  prefix` is tested synchronously with an injected `now` (and `sealed_at` set on the
  inputs) — no real-time wait, fully deterministic.
- **Apply is clock-free.** `DeleteSegments` names ids and flips `Sealed → Deleting`, so
  the state-machine test (precondition incl. oldest-first, idempotency, invariant
  re-check) is deterministic.
- **File reclamation is synchronous.** The data-plane delete (file + cache + index) is
  covered by the synchronous data-plane state tests.
- **No flaky age-based e2e.** A real-time retention e2e would race the virtual clock;
  it's deliberately avoided. Coverage comes from the decision/apply/reclamation units above, not a wall-clock sim run.

---

## Invariants (proposed)

These extend the metadata-state-machine contract; implementing them needs the usual
invariant sign-off (CLAUDE.md), since they ride in `assert_invariants`.

1. **Only sealed segments are deleted; the active segment never is.** Every range
   always retains its write head, so a topic under retention can still be produced to.
   Snapshot-checkable: no `Deleting` state on a range's active segment.

2. **Deletion is a contiguous prefix (oldest-first).** Within a range, every
   `Deleting`/deleted segment has a lower start offset than every surviving
   (`Active`/`Sealed`) segment — survivors are a contiguous suffix. This is the
   no-holes guarantee a forward reader depends on. Snapshot-checkable per range.

3. **`DeleteSegments` only advances state and freezes the rest** (`Sealed → Deleting`,
   consistent with metadata-state-machine.md #20; offsets/lineage/`replica_set`/
   timestamps unchanged). A retention delete is a state flip, not a rewrite, so a
   sealed segment's history stays trustworthy until its file is gone. (Rule.)

4. **The expiry decision is leader-only; the apply is deterministic.** The leader
   evaluates the age policy and names the segments; replicas apply the named set with
   no policy or clock of their own (metadata-state-machine.md #16). (Rule.)

#1 and #2 are the genuinely new snapshot-checkable invariants; #3 and #4 are rules
about the command's behavior.

---

## What needs to be done

1. **Optional retention config** — make `retention_ms` optional on the storage policy
   (default: off = keep forever).
2. **`DeleteSegments` metadata command** — per-range prefix (`{ topic_id, range_id,
   segment_ids }`); `Sealed → Deleting`, skipping active/absent/already-deleting
   (idempotent, no production prefix/active rejection); add invariant checks #1 and #2
   to `assert_invariants` — they pin the no-hole property.
3. **Retention sweep on the coordinator** — leader-only, on the ring-check cadence:
   per owned range compute the deletable prefix by `sealed_at` age (with injected
   `now`), propose `DeleteSegments` carrying the ids oldest-first.
4. **Data-plane deletion** — dispatch committed `DeleteSegments` grouped by
   `replica_set` (one batched message per set) on the post-commit drain; each node, per
   key it holds, deletes file + cache + sparse-index
   entries; idempotent (already-gone file is a no-op).
5. **Orphan-GC backstop** — confirm a segment dropped from a node's assigned set is
   reclaimed by the existing sweep, so a missed dispatch self-heals.
6. **Consumer "segment gone" handling** — a fetch to a deleted segment returns a
   skip-forward signal; the client advances to the surviving start via `ListOffsets`.
7. **Tests** — expiry-decision unit (injected `now`), `DeleteSegments` apply (sealed-only,
   oldest-first, idempotent, active rejected), data-plane file/cache/index removal.

## Verification

- `cargo clippy --all-targets --all-features -- -D warnings`; `cargo fmt --check`.
- Unit: deletable-prefix selection by age (injected `now`), `DeleteSegments` apply,
  data-plane removal.
- Reasoning check (age): a range with sealed segments aged `2T, 1.5T, 0.5T` (by
  `sealed_at`) plus an active head and `retention_ms = T` — the sweep deletes the first
  two (oldest-first), leaving the third + head as a contiguous suffix.
- Reasoning check (none): a topic with `retention_ms` unset keeps every segment forever
  — no `DeleteSegments` is ever proposed.

## See also

- `d5_crash_recovery.md` — orphan GC (the reclamation backstop) and local inventory.
- `.claude/rules/metadata-state-machine.md` — `Deleting` cascade (#5), state never
  reverts (#20), `ReassignSegment` freeze semantics (#21), leader-only decision /
  deterministic apply (#16), offset continuity (#8).
- `.claude/rules/raft-actor.md` — post-commit dispatch by the leader (#3), periodic
  leader-only sweeps and re-fill (#10).
