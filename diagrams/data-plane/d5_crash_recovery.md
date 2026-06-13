# Phase D5: Crash Recovery

A node crashes and restarts. Its memory is gone; its disk is not. D5 defines what the node does with that disk before it rejoins the cluster.

Depends on D1 (storage engine). Repair flows depend on D2 (replication).

---

## Conceptual Goal

Two facts shape the whole design:

**1. The durability promise lives in the WAL.** A producer was ACKed only after its entry was fsynced to the WAL on *every* replica and committed. So anything the cluster promised is in at least one healthy replica's WAL or segment files. Recovery on a single node never has to reconstruct a promise alone — it has to make its *local* data usable again so the cluster can count it.

**2. A restarted node is a new cluster member.** NodeIds are regenerated on startup. The cluster watched the old identity die (SWIM), sealed every active segment it touched, and moved on. The restarted node does not resume any leadership, any followership, or any active segment. It joins fresh — with a disk full of data from its previous life.

Together these give recovery a simple job description:

> **Make the local data honest, then let the cluster decide what it's worth.**

"Honest" means: every byte the node later claims to have must be complete and verified. Recovery never needs to be *complete* — anything missing or doubtful is filled by copying from a healthy replica (the repair path). Understating what you have costs network transfer; overstating it costs correctness. So every rule below errs toward dropping data, never toward claiming it.

What recovery deliberately does **not** do:

- It does not resume in-flight produces. Un-ACKed entries are the producer's problem — producers retry. ACKed entries are safe by the durability promise.
- It does not warm caches. The node restarts cold; caches refill from traffic.
- It does not track consumer positions. Those are client-owned (D4).
- It does not announce its data to the cluster. The node stays quiet until the coordinator assigns it something; only then does it mention what it already has.

---

## What Survives a Crash

| On disk | Role | Recovery treatment |
|---|---|---|
| WAL files | Source of truth for everything not yet checkpointed | Replayed forward, then discarded |
| Segment files | Checkpointed prefix of each segment | Kept; replay appends the missing suffix |
| Sparse index (RocksDB) | Derived read accelerator | Kept if present; rebuilt from segment files if missing or doubtful |
| Everything in memory | Caches, cursors, trackers, pending replies | Gone; nothing to recover — all derived or client-owned |

The oldest WAL file still on disk marks the checkpoint boundary: WAL files are deleted only once every segment has checkpointed past them. No separate checkpoint file exists or is needed — "what's left in the WAL directory" *is* the record of what might not have reached segment files.

---

## Recovery Sequence

Recovery runs entirely before the node opens any port:

```
1. Scan WAL directory          oldest surviving file → forward
2. Replay into segment files   route each record by its header, skip duplicates
3. Verify + build inventory    per segment: "I have entries up to N, CRC-checked"
4. Rebuild sparse index        for everything appended in step 2
5. Delete old WAL files        everything recovered now lives in segment files
6. Start fresh                 new WAL, new NodeId, join SWIM
7. Wait                        serve catch-up / cold reads when the coordinator asks
```

Steps 1–5 are idempotent: a crash *during* recovery just runs the same sequence again. Replay skips what already reached segment files, so re-running converges to the same result.

---

## WAL Replay

The WAL is one shared, interleaved stream: entries from all segments on the node, in fsync order. Each data record carries a small routing header — which topic, which range, which segment, which entry id. Replay is a single forward scan that demultiplexes this stream back into per-segment files:

```
for each WAL file, oldest → newest:
    for each record:
        CRC check
        route by header to its segment
        skip if the segment file already has this entry_id   (dedup)
        otherwise append to the segment file
```

**Deduplication.** A WAL file is only deleted after *all* its entries are checkpointed, so the surviving files usually overlap with segment file contents. The rule is per segment: determine the last entry id the segment file already holds, then append only WAL entries beyond it. Segment records are bare — they carry no entry id of their own (the routing header is a WAL-only concept) — so that last id is *derived*, not read from the file: the segment's `start_offset` (from the locally-recovered control-plane metadata) plus the count of CRC-verified records, minus one. Entry ids are contiguous and ordered within a segment, so this is a single comparison.

Note that no LSN is needed to know "how far the segment file got": checkpoint appends to a segment file strictly in entry-id order with no gaps (D1 invariant 4), so `start_offset + verified-record-count - 1` *is* the checkpoint frontier. The fsync ordering guarantees the WAL always covers the rest — segment files are fsynced before checkpoint reports, and WAL files are deleted only after.

**The torn tail.** A crash mid-write leaves the last file ending in a partial batch. Batches are written as a unit and fsynced as a unit, with an end-of-batch marker as the last record — so a missing or corrupt marker means the fsync never completed, which means nothing in that batch was ever ACKed. Replay stops at the last complete batch of the last file and discards the rest. Safe by construction.

**Corruption anywhere else** is a different story: a CRC failure in the middle of the stream is real damage, not a torn write. Replay does not try to be clever. It stops crediting the affected segment at the last verified entry and lets the repair path treat everything after as missing. The cluster has healthier copies; honesty beats heroics.

**Entries for sealed segments.** Some replayed entries may belong to segments the cluster sealed while this node was down — including entries beyond the sealed boundary (the crashed leader may have fsynced entries that were never committed). These are bounded by metadata: when the node later learns a segment's sealed end offset, anything local past it is surplus and ignored. This is why duplicate or surplus WAL data is harmless — every entry is routed by its header and clipped by the metadata boundary. The control plane recovers its Raft log independently and first; metadata is the authority on where every sealed segment ends.

---

## A Fresh WAL, Not a Resumed One

After replay, the old WAL files are deleted and the node starts a brand-new WAL from sequence one.

The alternative — reopening the old WAL and continuing where it left off — would require recovering the exact batch sequence number at the point of the crash, which the current format does not store per batch (and would need a file header or a stamped batch marker to support). That bookkeeping buys nothing: batch sequence numbers are node-local, the node's identity has already restarted, and everything the old WAL contained now lives in segment files. Resuming a dead identity's WAL is complexity in service of nothing.

This also keeps the WAL's lifecycle rule trivial: a WAL belongs to one node lifetime. Born empty at startup, deleted at the next recovery.

Restarting at sequence one cannot confuse a *second* recovery, for two stacked reasons. First, ordering: old-WAL deletion completes before the new WAL is created, so the directory never holds two lifetimes at once. Second, even if a crash lands inside that deletion window, the lifetimes cannot collide — a new lifetime only writes entries for newly assigned segments (old ones were sealed and never resume), so leftover old files and new files reference disjoint segments, and dedup by entry id handles the rest. Segment files, unlike the WAL, persist across lifetimes on purpose: they are the inventory.

---

## Local Segment Inventory

The output of recovery is one table, kept in memory:

```
(topic, range, segment)  →  highest verified entry id present locally
```

"Verified" is doing the work in that sentence: an inventory entry is only made after the segment file's records pass CRC. A segment with damage past entry N is listed as having N, not as having what the file size suggests.

The inventory is **passive**. The node never broadcasts it. It is consulted exactly once per segment, at the moment the coordinator assigns this node to that segment's replica set and the node would otherwise copy the whole thing:

- **Full match** — local data reaches the segment's sealed end offset. Verify, report complete. Zero bytes transferred.
- **Partial match** — local data reaches entry N, segment ends later. Request only entries after N from a healthy replica.
- **No match** — full copy, the default path. Recovery added nothing, and nothing breaks.

The node does not need to know it used to be "the F in that replica set." It just has files that happen to match. All the identity problems that plague systems with stable node IDs — zombies, stale claims, split identities — are absent because the cluster never has to decide whether this node *is* the old one. It isn't. It merely has some of the old one's bytes, and the bytes are checked on their own merits.

---

## Rejoining the Cluster

The node joins SWIM as a new member and does nothing else. In particular:

**No active segment resumes.** Every segment this node was actively writing or following was sealed by the cluster when SWIM declared the old identity dead — that is the seal-on-failure model doing its job (D2). Active segments always start fresh, on freshly assigned replica sets, with the leader's uncommitted tail replayed forward by the *surviving* leader, not by this node. The restarted node's local data is therefore only ever relevant to **sealed** segments — which are immutable, which is exactly why byte-level reuse is safe.

**Work arrives, it is not taken.** The coordinator (the metadata leader for each shard group) discovers under-replicated sealed segments on node death and picks replacements by placement policy. A freshly restarted node is simply a candidate like any other — one that happens to win the data-transfer lottery when assigned segments it already holds.

---

## Sealed Segment Repair

The cluster-side counterpart (defined in D2, summarized here because recovery plugs into it):

```
Coordinator           Raft            Healthy replica     Restarted node
    │                  │                    │                   │
    │  reassign sealed segment's            │                   │
    │  replica set: dead → new ──► commit   │                   │
    │                  │                    │                   │
    │  catch-up assignment ────────────────────────────────────►│
    │                  │                    │     check local   │
    │                  │                    │     inventory     │
    │                  │                    │◄─── "have up to N"│
    │                  │                    │                   │
    │                  │                    │  stream entries   │
    │                  │                    │  after N ────────►│
    │                  │                    │                   │
    │                  │                    │        verify, report complete
```

Repair is the safety net that lets every other rule in this document be conservative. Dropped a doubtful WAL tail? Repair copies it from a healthy replica. Understated the inventory? Repair transfers a few more bytes. The only unforgivable error is the opposite one — claiming entries the node cannot actually serve — and the CRC-before-inventory rule exists to make that impossible.

---

## Orphaned Data Cleanup

Some local files will match nothing: segments whose replica sets no longer include this node (the cluster repaired around the old identity before this restart), or topics deleted entirely. After recovery, the node asks the coordinator which of its inventoried segments appear in a replica set naming this node. Everything else is orphaned — eligible for background deletion.

**Deletion is deliberately lazy: triggered by disk pressure or a retention-scale TTL, never at recovery time.** An orphaned-but-intact segment is a free lottery ticket: if the coordinator later assigns this node that segment (a rebalance, another death elsewhere), local data turns a full copy into a delta or a no-op. Eager cleanup converts future cheap catch-ups into full transfers and saves nothing but disk space that nobody asked for. (Making placement actively prefer nodes that already hold a segment's data would push this further — that is a future optimization; the inventory stays passive in D5.)

---

## Failure Cases During Recovery

| Case | Handling |
|---|---|
| Crash during replay | Re-run from step 1. Dedup makes replay idempotent — entries already appended are skipped. |
| Crash after replay, before WAL deletion | Same: re-run, everything dedups, files get deleted this time. |
| Sparse index missing or corrupt | Rebuild by scanning segment files. The index is derived; it can always be reconstructed. |
| Segment file corrupt mid-stream | Inventory credits the segment only up to the last verified entry. Repair fills the rest. |
| Entire disk lost | Recovery degenerates to: join as a truly empty new member. Every assignment is a full copy. Correct, just slow. |
| WAL torn tail | Discard back to the last complete batch — those entries were never fsync-confirmed, hence never ACKed. |

The common thread: there is no recovery failure mode that requires operator intervention to preserve correctness. Worst case is always "copy more bytes from a healthy replica."

---

## Invariants

These are predicates over on-disk and in-memory state — checkable at any moment, suitable for `assert_invariants`-style verification and property tests, in the same spirit as the control-plane state machines.

1. **No batch is partially applied.** For every segment file, the entries present are exactly a prefix of the segment's entry-id sequence — contiguous from the start, no gaps. (A torn or corrupt WAL batch contributes nothing, never a fragment.)

2. **Replay is idempotent.** Running recovery N times over the same disk produces byte-identical segment files as running it once. Directly property-testable: recover, snapshot, recover again, compare.

3. **The inventory never overstates.** For every inventory entry `(segment → N)`, entries 1…N exist in the local segment file and pass CRC. The inventory is always ≤ the verified prefix, never beyond it.

4. **Inventory entries reference only sealed segments.** No active segment ever appears — guaranteed because a restarted identity is in no active replica set, so every local segment predates the restart and was sealed by the cluster.

5. **At most one WAL lifetime exists once the node serves.** After recovery completes, every WAL file on disk was written by the current process. (During the deletion window this may transiently be false; the disjoint-segment argument above makes that window harmless.)

## Rules Enforced by Construction

The remaining guarantees are ordering rules, not state predicates — there is no snapshot of state in which they can be checked, only a sequence that must not be reordered. Testing them against a description is oracle testing; the better treatment is to make violating them inexpressible in the code's shape:

- **Metadata before data.** Sealed-segment boundaries come from the control plane, which recovers its Raft log independently. The data plane clips local surplus by those boundaries at serve time, so it never needs to have replayed "correctly" with respect to seals — the clip is in the read path, not in recovery discipline. The same locally-recovered metadata also supplies each segment's immutable `start_offset` — the base that turns a segment file's CRC-verified record *count* into absolute entry ids during scan and replay, since segment records are bare. The *end* of a segment may have advanced remotely after the node died (more seals, reassignments), which is why the sealed boundary comes from the coordinator later; the *start* never changes, so the node's own last-persisted copy is authoritative for it.

- **Recovery before serving.** Rather than asserting "ports open only after inventory is built," make the serving components constructible only *from* the recovery output (the inventory and recovered segment state are constructor arguments). The rule then holds because no code path can produce a serving node without first producing a finished recovery.

- **Destruction is cluster-confirmed and last.** Orphan GC is the only step that deletes segment data, and it takes the coordinator's answer as a required input — same pattern: the delete function's signature demands the confirmation, instead of a convention demanding the call order.
