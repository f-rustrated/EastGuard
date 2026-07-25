# D8 — Raft RPC Sender Authorization

**Goal:** Ensure that each Raft group accepts an RPC only from a node authorized
for that RPC, without coupling consensus membership to the shared TCP connection.

**Depends on:** [D2 — Replicated Apply and Durability](d2_replicated_apply_and_durability.md),
[D4 — Membership and Shard Reconciliation](d4_membership_and_shard_reconciliation.md),
and the authenticated peer identity defined by the
[security roadmap](../security/roadmap.md).

---

## Boundary

One TCP connection carries Raft traffic for many shard groups. TLS authenticates
the node that owns the connection. It does not decide which groups that node may
access.

```
TLS transport
  validates certificate + connection identity emits 
  { authenticated peer, group, RPC }
                         |
                         v
          MultiRaft routes by group
                         |
                         v
target Raft group authorizes peer for this RPC
                         |
              reject ----+---- accept
                |                 |
                v                 v
          drop one RPC      apply Raft rules
          keep TCP open
```

Transport rejects a frame only when framing fails or its claimed sender differs
from the authenticated connection peer. Group membership, local role, term, vote,
leader, and log checks remain application logic.

**Connection rule:** Rejecting one group's RPC never closes the shared connection.
Healthy traffic for other groups continues.

---

## Gap Closed by D8

Before D8, the transport forwarded the group, claimed sender, and RPC to
MultiRaft. The target Raft state machine applied term and local-role rules before
complete sender authorization.

This creates four concrete gaps:

- a non-voter can send a higher-term vote request and make a member step down;
- a vote request can claim a candidate different from its envelope sender;
- vote responses are counted without voter identity deduplication;
- append and snapshot messages do not consistently bind their claimed leader to
  the authenticated sender.

D8 binds each envelope sender to the connection peer and makes every Raft handler
authorize that peer before mutating consensus state. The current handshake
provides this identity seam. Security S1 will replace its trust basis with TLS
without changing the Raft API.

---

## RPC Authorization Matrix

| RPC | Required sender authority |
|---|---|
| Vote request | Authenticated peer equals the candidate; candidate is a committed voter |
| Vote response | Authenticated peer equals the responder; responder is a committed voter; local node is a candidate; voter has not already responded in this term |
| Append entries | Authenticated peer equals the claimed leader; Raft validates term and log continuity |
| Append response | Authenticated peer equals the responder; responder is a committed voter or staged learner; local node is leader |
| Install snapshot | Authenticated peer equals the claimed leader; Raft validates term and snapshot continuity |
| Snapshot response | Authenticated peer equals the responder; responder is a committed voter or staged learner; local node is leader |

Voters may campaign, vote, lead, replicate, and acknowledge replication. Learners
may receive logs or snapshots and acknowledge their progress. Learners cannot
campaign, vote, lead, or count toward quorum.

This document uses three precise terms:

- **Claimed leader:** The node named as leader inside an incoming append or
  snapshot request. Raft has not recognized it as the current leader yet.
- **Committed voter:** A node in the applied Raft voting configuration.
- **Staged learner:** A leader-local replication target catching up before a
  committed promotion. It is not part of committed group membership.

Claimed-leader replication requests deliberately do not consult the receiver's
local voter set. A lagging replica may need an append or snapshot to learn the
configuration that added the current leader. Requiring that configuration before
accepting the replication creates a circular dependency and can strand the
replica permanently.

An unknown local group or unauthorized sender causes a single-RPC rejection. The
application emits a rate-limited audit event and leaves the shared connection
alone.

---

## Validation Order

Sender authorization runs before any RPC can affect term, role, timers, votes,
leader recognition, log state, snapshot state, or replication progress.

```
authenticated peer + target group + RPC
                  │
                  ▼
      target group exists locally?
                  │
              No  ├──► drop RPC
                  │
             Yes  │
                  ▼
     embedded identity matches peer?
                  │
              No  ├──► drop RPC + audit
                  │
             Yes  │
                  ▼
   response or election sender is
   an authorized committed voter or staged learner?
                  │
              No  ├──► drop RPC + audit
                  │
             Yes  │
                  ▼
apply term, role, log, and snapshot rules
```

This order matters. A rejected election sender cannot advance the term, reset an
election timer, grant or count a vote, or update replication progress. A leader
replication request proceeds to Raft only when its claimed leader matches the
authenticated peer; term, log, and snapshot validation then determine whether it
can change state.

---

## Vote Identity and Deduplication

A candidate begins each election with its own vote and an empty set of remote
voters. A granted response adds the authenticated voter identity to that set.
Adding the same voter again is a no-op.

```
term 42 votes = {self}

response from B, granted  -> {self, B}
duplicate from B          -> {self, B}
response from outsider X  -> rejected
response from C, granted  -> {self, B, C}
```

The vote set is cleared when the node leaves candidate state or advances term.
Leadership follows from the size of the unique authorized vote set, never from a
raw response counter.

---

## Membership Changes

Sender authorization reads the target group's current committed voter set for
elections, quorum acknowledgements, and learner progress. Membership remains a
Raft decision:

- removing a voter takes effect when the removal entry applies;
- adding a voter takes effect when the promotion entry applies;
- a staged learner is authorized only for learner replication traffic;
- SWIM or ring intent cannot directly authorize a Raft RPC.

Claimed-leader replication requests are the exception to the local-membership
lookup, not an exception to identity authentication. The receiver binds the
claimed leader to the connection peer and lets Raft validate the request. This is
necessary because the receiver's committed configuration may lag behind the
leader's while the receiver is catching up.

Temporary disagreement is handled as a stale RPC, not a transport failure. For
example, a recently removed voter may send one last response over an otherwise
healthy node-to-node connection. The old group rejects that response while other
groups continue using the connection.

---

## Implementation Shape

1. Transport carries the connection peer identity with each inbound Raft RPC.
2. The target Raft state-machine boundary performs one authorization step.
3. Candidate, claimed-leader, and responder identities must match that peer.
4. Election and response RPCs enforce the committed-voter or staged-learner
   requirement from the matrix.
5. Candidate votes are unique voter identities rather than a scalar count.
6. Unauthorized election and response RPCs return before term or state
   processing; claimed-leader replication requests continue through ordinary
   Raft validation.

The transport remains unaware of Raft membership and role. MultiRaft remains a
group router. The target Raft instance owns the decision because only it has the
committed voter set, staged learners, and current local role.

TLS authentication and structured security audit events remain security-roadmap
work. They replace the handshake trust basis and rejection diagnostics,
respectively; they do not change the application authorization rules.

---

## Validation

- Send a higher-term vote request from a non-voter. Term, role, vote, and election
  timer remain unchanged.
- Send a vote request whose candidate differs from the authenticated peer.
- Replay one granted vote response until quorum would be reached without
  deduplication. It contributes one vote.
- Send append entries from a non-voter and from a voter claiming another leader.
- Send append and snapshot responses from unknown nodes.
- Verify a learner can acknowledge catch-up but cannot campaign or vote.
- Remove a voter, then deliver its delayed RPC after the removal applies.
- Reject an RPC for one group, then process a valid RPC for another group over the
  same TCP connection without reconnecting.
- Fuzz unauthorized RPCs and verify that rejection does not mutate consensus
  state or create unbounded audit output.

---

## Design Rules

1. **Transport authenticates; Raft authorizes.** TLS proves the peer identity, but
   only the target group can interpret voter, learner, leader, and term state.
2. **Authorization precedes consensus mutation.** An unauthorized RPC cannot alter
   term, role, timer, vote, leader, log, snapshot, or replication progress.
3. **RPC authority is group- and operation-specific.** Cluster membership alone
   does not grant universal Raft authority.
4. **Votes are identities, not a counter.** One committed voter contributes at
   most one vote in a term.
5. **Learners have replication authority only.** Catch-up does not confer election
   or quorum authority.
6. **Application rejection preserves transport.** A stale or unauthorized RPC
   drops one message and never disrupts unrelated groups on the shared connection.

See `.agents/rules/raft.md` for the exact consensus contracts and
`.agents/rules/raft-transport.md` for the transport boundary.
