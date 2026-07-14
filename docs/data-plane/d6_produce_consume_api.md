# Phase D6: Produce / Consume API — server-side routing

**Goal:** finish the small server-side piece the produce/consume API still needs.
When a write lands on the wrong node, the server resolves the right node and returns
it as a **redirect** (`NotWriteLeader` / `ShardNotLocal`); the client retries there.
The server never forwards or proxies the request.

**Depends on:** D4 (consumer range tracking), D5 (crash recovery).

---

## Scope: most of "D6" is client-side; this is the server part

The produce/consume API has two sides, and most of it is not server work:

- **Server (data plane) — this doc.** The handlers and the *entire consume path* are
  already built in D1–D5; the only gap is making **writes return redirects instead of
  `InternalError`**. A small redirect completion — the thin terminal data-plane item.
- **Client — a separate track.** The client API, routing cache, connection pooling,
  redirect-following, range actors, and consumer groups are documented under
  [clients/](../clients/client_roadmap.md). They ride on the redirects this server phase
  defines. Durable group progress is described in
  [D8](d8_consumer_offset_management.md).

## The model: redirects, not proxy

When a request lands on the wrong node the server resolves the correct target,
returns it, and the client retries there — it never forwards internally. This keeps
the server simple (no forward path, no proxy connections), matches what's already
there (DescribeTopic already redirects; the consume path returns `ShardNotLocal` on a
stale target), and lets the client cache the real layout.

A redirect points at a node, and *which* node differs by request (CLAUDE.md "Two
replica sets"): **produce** → the segment's write leader (`replica_set[0]` of the
active segment); **control-plane writes** → the shard group's Raft leader.

---

## What needs to be done

1. **Produce redirects** — rewrite the produce handler to be membership-first
   (mirroring the DescribeTopic redirect), returning, in order:
   - ring can't map the key (topology not converged here) → `ShardNotLocal` with no
     hint — retriable, *not* `TopicNotFound`;
   - this node not a member → `ShardNotLocal` with a member's address (no hint yet if
     SWIM hasn't resolved one — still retriable);
   - metadata lookup absent (authoritative now we know we're a member) → `TopicNotFound`
     — the SWIM membership check **must** come first, because the metadata lookup
     can't tell "topic absent" from "not hosted here";
   - routing key finds no active range (the topic isn't active — being torn down) →
     `TopicNotFound`. An active topic's ranges tile the whole keyspace, so an active
     topic always hits;
   - active segment's `replica_set[0]` ≠ this node → `NotWriteLeader { leader_addr }`
     (`None` if a just-dead leader can't be resolved);
   - otherwise dispatch to the data plane and map the ack to `Produced`. Map the data
     plane's transient "not leader / segment not found" rejection (metadata names us
     leader but the assignment hasn't applied yet) to a **retriable**
     `NotWriteLeader { leader_addr: None }`, not `InternalError`.

   Both redirect variants — `ShardNotLocal` (wrong host) and `NotWriteLeader` (wrong
   role) — carry an *optional* address and are uniformly retriable: addresses come from
   SWIM, which converges eventually, so "no hint yet" means back off and retry. The
   leadership check lives at the connection layer, from the metadata it already
   fetches — a misroute returns a redirect with no data-plane round-trip; the data
   plane's own role check stays as the not-ready backstop. (There is deliberately no
   "mid-split" redirect: a split applies atomically, so a produce simply routes to
   the active child.)

2. **Address resolution (reuse).** Use the SWIM `NodeId → client address` resolution
   the DescribeTopic redirect already uses (resolve-one and resolve-any). Both return
   an optional address, matching the optional hint both redirect variants carry. No
   new code.

3. **Control-plane write redirects** (separable; land after produce). CreateTopic /
   DeleteTopic return `InternalError` on a non-leader today. Mirror the two planes'
   wrong-host vs wrong-role split: not a member → `TopicMetadataRedirect` (the same
   structural redirect DescribeTopic already returns); a member but not the Raft
   leader → a new control-plane `NotRaftLeader { leader_addr }` — named to keep it
   distinct from the data plane's `NotWriteLeader` (segment write leader, not Raft).
   Every proposal failure (not leader, group not ready/removed here) collapses to
   that retriable redirect.

4. **Tests.**
   - Replace the produce-to-every-node test helpers with a **redirect-follower**:
     `Produced` → done; `NotWriteLeader` / `ShardNotLocal` with an address → retry
     there; no address or a transient error → brief backoff and retry. Sweeps all
     nodes too, so it stays robust when a node is down (crash/repair tests).
   - Collapse the create-topic retry-all loops to "send to any node, follow the
     redirect."
   - **Keep** the replication-lag / cluster-visibility polling loops — they wait on
     eventual state, not routing.
   - Unit tests on the produce handler: ring-unresolved, not-a-member, not-leader,
     dispatches-when-leader; plus a control-plane redirect.

## Decision: no routing cache on the server

The server holds **no** routing cache — it resolves per request and redirects,
staying stateless about who-talks-to-whom. Caching belongs on the **client**: it
caches what `DescribeTopic` returns and routes directly; a stale cache is corrected
by the next redirect (the Kafka model). A server-side cache would only duplicate
state the client already holds, add invalidation wiring, and at best save one
redirect. It's a client-SDK concern (future work), needing nothing further here.

## Verification

- `cargo clippy --all-targets --all-features -- -D warnings`; `cargo fmt --check`.
- Unit: the produce-handler redirect cases.
- e2e (turmoil): the produce-driven tests (hot produce+fetch, create-topic +
  describe, delete-topic, and the repair tests that produce) exercise the
  redirect-follower.
- Reasoning check: on a 3-node fresh topic, a produce to a non-leader returns
  `NotWriteLeader` carrying the leader's client address, and the second hop lands `Produced`.
