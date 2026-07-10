# Phase C1: Routing & Connections

**Goal:** the foundation every other client phase rides on — pooled TCP connections
with request-id multiplexing, a routing cache built from `DescribeTopic`, and one
uniform redirect-follow loop that keeps the cache honest. Get this right and the
producer and consumer become thin layers on top.

**Depends on:** the server-side D6 routing protocol (redirects, `DescribeTopic`).

---

## Connection model

Clients talk to the cluster over `client_port` (TCP). A connection is **multiplexed**:
many requests can be in flight on one socket, and responses may come back in any
order. The protocol that makes this work is already defined server-side —

- Each request carries a per-connection, monotonically increasing **request id**.
- The client keeps an in-flight map: request id → the waiter expecting that response.
- The server echoes the request id back unchanged (it is stateless about it).
- On a response, the client matches the id to its waiter and delivers it.

So the client needs, per node it talks to: one connection, a request-id counter, an
in-flight map, and a background read loop that demultiplexes responses to waiters. A
**connection pool** keys these by node address, opening lazily and reconnecting on
drop.

```
        produce/fetch/admin calls
                  |
          ┌───────┴────────┐
          v                v
   in-flight map      request-id counter
          |                |
          └──────> frame + send ──────> socket ──> node
                                           |
   waiters  <──── demux by request-id <── read loop
```

The wire framing (length prefix + request id + payload) and the reader/writer halves
already exist; the SDK reuses them rather than re-implementing the frame format.

---

## The routing cache

`DescribeTopic` returns everything the client needs to route a topic without further
metadata round-trips:

- the resolved **topic id** (so later fetches go *by id*, hitting any replica
  directly — no name re-resolution on the data node);
- every **range** with its keyspace `[start, end)` and lineage pointers
  (split-into / merged-into / merged-from);
- each active segment's **replica set as resolved client addresses**, in
  `replica_set` order (position 0 is the write leader, the rest are read replicas);
- *(for historical / `earliest` reads)* each range's **sealed segments** too — their
  offset spans and replica sets — so a cold reader can place an old `(range, offset)` on
  a node that actually holds it. Gated: a tail-only consumer omits it and pays nothing
  for a long history. Surfacing sealed-segment placement is a small server-side
  `DescribeTopic` extension — the metadata group already tracks every segment's
  `replica_set` (repair and reassignment maintain it); today the response just doesn't
  expose the sealed ones (see `d4`).

The client caches this per topic and routes from it:

- **Produce** → match the routing key to the owning active range (the one with the
  greatest `keyspace_start ≤ key` — the same rule the server applies; keys are compared
  as raw bytes, not hashed), send to that range's write leader (`replica_set[0]`).
- **Consume** → for each range of interest, map the offset to the segment that holds it
  (the active one, or — reading history — a sealed one) and fetch from any replica in
  that segment's set (nearest/preferred). Without sealed-segment placement a cold reader
  can only guess the active segment's replicas and lean on "not local" to correct it;
  with it, the first hop lands.

What the cache actually *retains* is the routing projection of the snapshot: the topic
id, and per active range its `keyspace_start` plus the active segment's replica
addresses. The full keyspace bounds and split/merge lineage are the **consumer's**
concern, not the router's — they ride the cursor library (bootstrapped once from the
snapshot) and the in-band range-transition signal each fetch carries (see `c3` / `d4`),
so they don't need to live in the routing cache.

Only a member of the topic's metadata shard group can answer `DescribeTopic`
authoritatively, so the *first* describe may itself be redirected (see below). Once
cached, the hot path touches no metadata.

**Refresh.** The cache is a hint, not truth. It's refreshed when a redirect says it's
stale (lazy, the common case), and may be refreshed proactively on a coarse TTL. The
client never assumes the cache is correct — it assumes the *server* will correct it.

---

## The redirect-follow loop

This is the heart of C1. The server returns five typed redirects; the client maps
each to one action. They split cleanly into *wrong host* (structural, from the SWIM
ring) and *wrong role* (dynamic leadership), and every one is retriable — a present
address is a hint to jump to, an absent address means "re-resolve and back off."

| Redirect | Plane | Meaning | Client action |
|---|---|---|---|
| `ShardNotLocal { hint_node }` | data | not a member of the topic's shard | jump to `hint_node`; if `None`, re-`DescribeTopic` |
| `NotWriteLeader { leader_addr }` | data (produce) | member, not the segment's write leader | jump to `leader_addr`; if `None`, re-resolve |
| `TopicMetadataRedirect { owner }` | control | this node doesn't host the topic's metadata | retry the metadata op at `owner` |
| `NotRaftLeader { leader_addr }` | control (write) | member, not the metadata Raft leader | jump to `leader_addr`; if `None`, retry a member |
| `TopicNotFound` | both | topic absent on the member we hit | re-resolve (may be propagating); surface only if still absent at the deadline |

The single rule: **follow the hint if present, else re-resolve with exponential
backoff — until a wall-clock deadline.** A misrouted request converges in O(1) extra
hops (the redirect target either serves it or points once more at the real owner).
`TopicNotFound` is *not* immediately terminal: a just-created topic may not have
propagated to the member we hit, so it's re-resolved like any transient and surfaces
as not-found only if the whole deadline elapses still seeing it — the same call a
Kafka producer makes on `UNKNOWN_TOPIC`. Because redirects are returned *before* any
data-plane dispatch, a redirected produce never wrote, so following it can't
double-write (full idempotency is C2's concern).

```
send to best-known node                 (retry until the deadline)
   │
   ├─ ok / data        → done
   ├─ redirect + addr  → jump there, retry (no wait — a hint is actionable)
   ├─ redirect, no addr→ re-resolve to a seed, exponential backoff, retry
   ├─ topic-not-found  → re-resolve (propagation lag); not-found only at the deadline
   └─ unreachable      → drop the dead conn, re-resolve, backoff, retry
```

**The deadline is the SDK's, not the caller's.** A retry policy (deadline + backoff,
with a sane default) lives on the client, so one `produce` / `describe` / `create`
call already rides out a leader election, metadata propagation lag, or a node crash —
callers don't wrap calls in their own retry loops. The loop surfaces only what a
caller must decide on: success, `TopicNotFound`, or a deadline `Timeout`.

The integration tests already encode a minimal version of exactly this
(`produce_until_acked` follows `NotWriteLeader`/`ShardNotLocal`; `describe_topic`
follows `TopicMetadataRedirect`) — C1 generalizes it into the SDK's one retry path,
shared by producer, consumer, and admin.

---

## Admin (folded in)

The control-plane ops — create / delete / describe / list — are the same
redirect-follow loop over the control plane: `DescribeTopic` and reads follow
`TopicMetadataRedirect` to a member; writes (`CreateTopic` / `DeleteTopic`)
additionally follow `NotRaftLeader` to the Raft leader. No separate machinery; admin
is C1's loop pointed at control-plane requests. (Cluster/shard introspection —
`DescribeCluster` and friends — is an existing operator/debug affordance on the wire,
not part of the C1 client surface.)

---

## What needs to be done

1. **Connection pool** — lazy per-node connections over `client_port`, each with a
   request-id counter, an in-flight map, and a demultiplexing read loop; reconnect on
   drop.
2. **Routing cache** — the routing projection of the `DescribeTopic` snapshot (topic
   id, per active range `keyspace_start` + active-segment replica addresses); lookup by
   routing key (produce) and by range (consume). Full keyspace bounds and lineage stay
   with the cursor library + in-band fetch signals, not the cache.
3. **Redirect-follow loop** — the one retry path mapping each redirect to its action,
   bounded by a client-level retry policy (deadline + exponential backoff), with
   cache-refresh on stale/absent hints and `TopicNotFound` retried within the deadline
   (propagation lag), surfaced only if still absent when it expires.
4. **Admin calls** — create/delete/describe/list over the same loop.
5. **Tests** — against the simulated cluster: first-contact resolution via redirect,
   stale-cache correction in O(1) hops, reconnect after a node drop, concurrent
   in-flight requests on one connection demuxed correctly.

## See also

- `client_roadmap.md` — where C1 sits; the smart-client principles.
- `d6_produce_consume_api.md` — the server side of these redirects (the contract C1
  consumes).
- `c2_producer.md`, `c3_consumer.md` — the layers built on this foundation.
