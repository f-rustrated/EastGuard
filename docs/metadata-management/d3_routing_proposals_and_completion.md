# D3 — Routing, Proposals, and Completion

**Goal:** Explain how any node routes a metadata request to the correct shard, how leadership redirects are resolved, and when a caller is told the operation succeeded.

**Depends on:** [D2 — Replicated Apply and Durability](d2_replicated_apply_and_durability.md).

---

## Two routing decisions

A request must answer two different questions:

1. Which shard group owns this resource key?
2. Which member currently leads that shard group?

The consistent hash ring answers the first. SWIM's shard-leader gossip accelerates the second, while Raft remains the authority on whether a node is actually leader.

```
resource key
    │ hash on local ring
    ▼
shard group members
    │ leader hint from gossip
    ▼
candidate coordinator
    │ Raft role check
    ▼
accept proposal or return redirect
```

Keeping these questions separate matters: ring ownership changes with cluster membership, while leadership changes with elections. A fresh ring does not imply a fresh leader hint.

---

## Membership-first request handling

The receiving node resolves the resource against its local topology. If the node hosts the owning shard, it can ask its local consensus actor to propose. Otherwise it returns a reachable member of the owning group so the client can retry there.

The next member may still be a follower. In that case, the proposal response includes the current leader hint when known. A one-hop forwarding or redirect path corrects ordinary gossip staleness without allowing requests to bounce indefinitely.

The leader map is a cache, not a correctness dependency. If a one-shot leader announcement was missed, the ring still supplies the group's members. Callers can try or broadcast to those members; only the actual Raft leader acts, while followers reject the proposal.

---

## Completion is tied to apply

Once a leader accepts a command, the consensus actor records its log position and the waiting reply. Apply reaching that position completes the request:

```
client       node         leader Raft       followers
  │ request    │               │                │
  ├───────────►│ route/propose │                │
  │            ├──────────────►│ append         │
  │            │               ├───────────────►│
  │            │               │◄──── acks ─────┤
  │            │               │ commit + apply │
  │            │◄──────────────┤ complete       │
  │◄───────────┤ success       │                │
```

Tracking by log position avoids coupling completion to a particular command type. It also correctly handles multiple identical commands: each accepted proposal has its own place in the history.

If leadership is lost before completion, outstanding replies fail with a not-leader result. The caller retries through current routing instead of waiting forever for an old leader that can no longer commit its suffix.

---

## Reads and discovery

Shard discovery asks SWIM for the group and its best-known leader in one round trip. This supports admin tools and clients that want to cache routes, but cached information is advisory. Server-side role validation remains the final gate.

Some local inventory reads intentionally report only the groups hosted on the contacted node. They are operational views, not a substitute for a cluster-wide metadata scan.

---

## Failure cases

| Condition | Result | Recovery |
|---|---|---|
| Ring has not learned a member | Retriable routing failure | Wait for SWIM convergence. |
| Member is not leader | Redirect with leader hint when available | Retry once at the hinted node. |
| Leader hint is stale | Target rejects based on Raft role | Resolve and retry; no stale write occurs. |
| Leader steps down after append | Pending reply fails | New leader decides the surviving log suffix. |
| Leader announcement is missing | Member fallback | Contact group members; only leader accepts. |

---

## Design rules

1. **The hash ring chooses the shard; Raft chooses the writer.** Conflating them makes topology gossip an accidental consensus mechanism.
2. **Success means committed and applied.** Append acceptance alone is not a durable client outcome.
3. **Leader hints are advisory.** Every target validates its current role before accepting a proposal.
4. **Forwarding is bounded.** A stale route may add one correction hop but cannot create a loop.
5. **Stepdown resolves every pending caller.** No request remains attached to leadership that no longer exists.

Continue with [D4](d4_membership_and_shard_reconciliation.md) for how the ring and each Raft peer set evolve.
