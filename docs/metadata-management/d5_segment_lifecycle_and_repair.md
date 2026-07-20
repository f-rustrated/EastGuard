# D5 — Segment Lifecycle and Repair Coordination

**Goal:** Explain the metadata side of segment placement, rolling, failure-boundary recovery, sealed repair, and retention—where the control plane decides and the data plane performs byte work.

**Depends on:** [D1](d1_entity_model_and_lifecycle.md) and [D4](d4_membership_and_shard_reconciliation.md). The byte-level protocols are documented under `docs/data-plane/`.

---

## Metadata is the placement authority

For every segment, committed metadata records an ordered data replica set. The first replica is the write leader. All replicas apply the same chosen order; they never independently re-run placement from their local ring views.

When a segment becomes active, the metadata leader dispatches its assignment to the data plane. Assignments are periodically re-driven because the first notification is not a durable delivery queue. Repetition is safe: a replica reconciles its local state with the committed assignment.

---

## Ordinary roll

The data-plane leader requests a roll after size pressure, idle maintenance, recovery, or another lifecycle trigger. The metadata leader commits one transition that seals the old segment and creates its successor.

```
data leader        metadata leader        shard replicas       data replicas
    │ roll request       │                       │                    │
    ├───────────────────►│ propose               │                    │
    │                    ├──────────────────────►│ replicate/apply    │
    │                    │◄──────────────────────┤                    │
    │                    ├───────────────────────────────────────────►│
    │                    │       seal old + place successor           │
```

The request carries the sealed end when known and the successor replica order. Once applied, the old segment is immutable and the new one begins at the next offset.

---

## Recovering an active segment after failure

If exactly one active replica dies, the coordinator asks surviving replicas for their durable extents before rolling. The minimum durable end is the safe committed boundary: a write was acknowledged only after the required replicas made it durable, so the shared prefix cannot exceed the least-complete survivor.

The most-complete survivor becomes the new write leader, with deterministic node ordering to break ties. These are leader decisions encoded into the roll so followers apply identically.

If several replicas are lost or no trustworthy answer is available, the system may seal with an unknown end and continue. Boundary correction later writes the recovered end once and shifts the successor start to the following offset. Unknown-boundary history is excluded from verified sealed catch-up until that correction lands.

---

## Repairing sealed segments

Sealed data cannot be repaired by opening it for writes. Metadata instead commits a new replica set while leaving the segment sealed and preserving its bounds.

The metadata leader then drives catch-up to every newly assigned member. Receivers copy through the known sealed end and acknowledge completion. The leader re-drives incomplete repairs on heartbeat sweeps and reconstructs repair tracking after takeover, because the delivery tracker itself is leader-local.

When the ring cannot supply enough live nodes, the segment remains below its configured replication factor. Periodic reconciliation refills it when capacity returns. A restarted node that already has the complete bytes can confirm without transferring them again.

---

## Retention and deletion

Retention chooses an oldest-first prefix of sealed segments in one range. The committed metadata transition marks that prefix deleting and groups cleanup work by replica set for efficient dispatch.

Only prefixes are eligible because deleting a hole in the middle of history makes offset-based reasoning ambiguous. Physical cleanup is retryable and follows the replicated logical decision.

---

## Responsibility boundary

| Control plane decides | Data plane executes |
|---|---|
| Segment identity and offset bounds | WAL creation, append, fsync, and reads |
| Ordered replica set and write leader | Replication between data replicas |
| Seal, roll, reassignment, deletion | Byte sealing, catch-up, and file removal |
| Whether repair is complete enough to commit | Transfer progress and durable-extent reports |

This boundary keeps byte I/O out of Raft while ensuring every irreversible placement decision is replicated.

---

## Design rules

1. **Committed metadata is the authority for byte ownership.** Local files alone never make a node a replica.
2. **A failure roll uses durable extents.** A notified cursor can lag durable acknowledged data and would risk truncation.
3. **Sealed repair preserves immutable bounds.** Reassignment changes placement, not content or lifecycle state.
4. **Repair delivery is re-driven until confirmed.** One-shot actor messages are not a liveness mechanism.
5. **Unknown boundaries are corrected once.** Repeated or conflicting correction would destroy offset continuity.
6. **Retention removes oldest-first prefixes.** Historical holes are forbidden.

For the byte protocols, see `docs/data-plane/d2_segment_replication.md`, `d3_segment_roll_integration.md`, `d5_crash_recovery.md`, and `d7_retention_gc.md`.
