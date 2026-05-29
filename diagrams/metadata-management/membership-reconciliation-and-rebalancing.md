# Membership Reconciliation — Mental Model

How to think about the interplay between SWIM (eventually consistent) and Raft (strongly consistent) for peer-set management, and why removing a dead peer is only half the job.

---

## Eventual consistency is delayed truth, not fake truth

A common misread of "eventually consistent" is "approximate" or "best-effort". SWIM is neither. With incarnation numbers and the suspect-then-confirm machinery, SWIM's view is **monotonic and eventually correct** — a node marked dead really is dead; it just took some gossip rounds for everyone to find out.

So when SWIM says "X is dead", that is a real fact about the world. The only question is when each node learns it.

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

1. SWIM facts are real, just delayed. Eventually consistent ≠ approximate.
2. Raft converts SWIM facts into group-committed state. The conversion is role-gated — only the leader can originate it.
3. When a fact arrives and no node holds authority to convert it, the fact stays outside Raft. Gossip will not re-fire it.
4. Reconciliation = refer back to SWIM when your role allows you. The new leader rereads SWIM's current state and proposes for anything missed.
5. Reconciliation alone shrinks the group, consuming failure budget. Pair every removal with an addition of a replacement to keep the budget intact.
6. Some failures (quorum loss before a leader can act) cannot be repaired by any in-protocol mechanism. Reconciliation does not pretend otherwise.
