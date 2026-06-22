# EastGuard Client SDK Roadmap

This roadmap covers **client** library applications use to produce, consume, and
administer topics. .

The server was built to be routed *by the client*, not to proxy on the client's
behalf. Every handler that lands on the wrong node returns a **redirect** rather than
forwarding (see d6_produce_consume_api.md). So the client is not a thin socket wrapper
— it owns a metadata cache, follows redirects, manages connections, and (for consume)
walks range lineage. This is the Kafka model: a smart client over a cluster that tells
it where to go.

---

## What already exists

The client doesn't start from zero — three pieces are built and tested server-side:

- **The wire protocol.** The full request/response surface (control plane, data
  plane, admin), the framing, and the request-id multiplexing protocol all exist and
  are exercised by the integration tests. See `c1_routing_and_connections.md`.
- **The redirects.** Produce and the control-plane writes return typed redirects
  (`NotWriteLeader`, `ShardNotLocal`, `NotRaftLeader`, `TopicMetadataRedirect`),
  each carrying an optional resolved address. `DescribeTopic` returns the full
  routing snapshot (range keyspaces, segment replica sets with client addresses,
  lineage pointers). The client's job is to *act* on these.
- **The consumer cursor library.** A pure, synchronous state machine that tracks a
  consumer's position across a topic's ranges and walks split/merge lineage from the
  in-band signal each fetch carries. No network, no async, no offset storage — it
  decides *what to fetch next*; the consumer wraps it with I/O. See
  `c3_consumer.md`.

The integration tests already contain a *de-facto* client — `send_request` plus the
redirect-following helpers (`produce_until_acked`, `describe_topic`). That's the
minimal mechanics; the SDK turns it into a real, reusable library.

---

## What the client must add

```
                         Application
                              |
          ┌───────────────────┼───────────────────┐
          v                   v                    v
      Producer            Consumer              Admin
   (routing-key →      (cursor library +      (create/delete/
    write leader)       lineage walk)          describe/list)
          \                   |                   /
           \                  v                  /
            \         Routing + metadata cache  /
             \      (DescribeTopic snapshot,   /
              \      redirect-follow, refresh) /
               \                |             /
                v               v            v
                   Connection pool (client_port, TCP)
                   request-id multiplexing, one conn per node
```

- **Routing + connection layer** — caches what `DescribeTopic` returns, routes
  directly to the right node, follows redirects when the cache is stale, and pools
  connections. The foundation both producer and consumer ride on.
- **Producer** — hashes the routing key to a range, sends to that range's write
  leader, follows `NotWriteLeader` / `ShardNotLocal` on a miss, and (later) carries
  idempotency keys.
- **Consumer** — bootstraps cursors from a topic's metadata, fetches each range from
  the nearest replica, feeds every response's progress signal to the cursor library,
  and follows the lineage transitions it returns.
- **Admin** — create/delete/describe/list, following the control-plane redirects.

---

## Phases

| Phase | Goal | Depends on | Details |
|---|---|---|---|
| [C1: Routing & Connections](c1_routing_and_connections.md) | Connection pool, request-id multiplexing, metadata/routing cache, redirect-following, refresh | server D6 | The foundation both producer and consumer build on |
| [C2: Producer](c2_producer.md) | Routing-key → write leader, redirect handling, batching hook, idempotency hook | C1 | Produce client |
| [C3: Consumer](c3_consumer.md) | Wrap the cursor library, fetch routing, lineage walk, start policies, by-id fetch | C1 | Consume client |

Admin is small enough to fold into C1 (it's the same redirect-follow over the control
plane) rather than its own phase.

### Dependency graph

```
C1 (Routing & Connections)
 ├──────────────┐
 v              v
C2 (Producer)  C3 (Consumer)
```

---

## Design principles

- **The client owns routing; the server never proxies.** Correctness comes from the
  server's per-request membership/leadership check — a stale client cache costs an
  extra redirect, never a wrong write. The cache is an accelerator, not the source of
  truth (the same stance the topology shard-leader map takes server-side).
- **Every redirect is retriable.** Addresses come from SWIM, which converges
  eventually, so a missing or stale hint means "back off and retry / re-resolve,"
  never a hard failure. The client's retry loop treats all redirect variants
  uniformly: follow the hint if present, otherwise re-`DescribeTopic`.
- **Resolution is by name, placement is by id.** A client resolves a topic *name*
  once via `DescribeTopic` (which only a metadata-group member can answer), caches the
  resolved `topic_id` and segment replica addresses, then fetches *by id* straight
  from any replica holding the data — no further metadata round-trips on the hot path.
- **The cursor library is the consumer's brain; the SDK is its body.** All lineage and
  ordering logic lives in the pure state machine. The SDK only adds connections,
  retries, and scheduling — keeping the hard correctness in a synchronously-testable
  core.

---

## Backlog (beyond a working client)

- **Consumer groups** — sharing ranges across consumers, rebalancing, durable offset
  commit. Offset storage is its own concern, not the log layer.
- **Idempotent / exactly-once produce** — producer session + sequence numbers, dedup
  at the segment leader (the server-side half is its own backlog item).
- **Client-side batching & compression** — amortize round-trips and bytes.
- **Adaptive replica selection** — pick the fastest/nearest replica per range from
  observed latency, not just `replica_set` order.
