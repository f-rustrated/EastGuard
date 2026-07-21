# Phase C2: Producer

**Goal:** a produce client that turns "append this record to this topic" into a write
landing on the right segment's write leader — hashing the routing key to a range,
sending to that range's leader, and following redirects when its routing cache is
stale. Thin by design: all the routing and connection machinery is C1's; C2 adds the
produce-shaped layer on top.

**Depends on:** C1 (routing cache, connection pool, redirect-follow loop).

---

## What a produce is

The client sends, for one record (or batch): the **topic name**, a **routing key**,
the opaque **payload bytes** (optionally compressed — see Compression), and a
**record count**. The broker stamps an entry id
and stores/replicates the payload as-is — it never parses it. The routing key is used
only to locate the target range; it is not stored.

The reply is either the committed **entry id** or a redirect/error the C1 loop already
knows how to handle.

---

## Routing a produce

Two questions, answered from the C1 routing cache:

1. **Which range owns the key?** The topic's active ranges tile the keyspace
   `[MIN, MAX]` contiguously; the owning range is the one whose `[start, end)` contains
   the routing key. The client hashes/maps its application key to the routing-key bytes
   and finds that range in the cache.
2. **Which node leads that range's segment?** The range's active segment's
   `replica_set[0]` — the write leader. The client sends the produce straight there.

```
record (key, bytes)
   │  hash key → routing-key bytes
   v
routing cache: range whose [start,end) covers the key
   │
   v
that range's active segment → replica_set[0] (write leader)
   │
   v
send Produce to the write leader ──> Produced { entry_id }
```

When the cache is right, that's one hop to the correct leader with no metadata
round-trip. When it's stale, the server redirects and C1's loop corrects it.

---

## Following redirects (produce-specific)

The produce path can hit two data-plane redirects, both handled by C1's uniform loop:

- **`ShardNotLocal { hint_node }`** — the client hit a node that doesn't even host the
  topic's shard (cache badly stale, or first contact). Jump to `hint_node`, or
  re-`DescribeTopic` if absent.
- **`NotWriteLeader { leader_addr }`** — right shard, wrong segment leader (a roll or
  failover moved leadership). Jump to `leader_addr`, or re-resolve if absent. The
  server also returns this for its own transient "named leader but the assignment
  hasn't applied yet" window — same action: brief back off and retry.

`TopicNotFound` is terminal — surfaced to the caller. A split that moved the key to a
child range needs no special handling: the key simply maps to the child on the next
resolve, and produce continues there (the server has no "mid-split" produce signal
because a split applies atomically — see d6).

**Safety:** redirects are returned before any write happens, so a redirected produce
never landed — following it cannot duplicate. (Duplicates from a *timeout after the
write committed but before the ack* are the idempotency concern below, not a redirect
concern.)

---

## Batching hook

The wire already carries a `record_count`, so one produce can ship many records as a
single opaque blob. The producer batches records destined for the **same range** (same
write leader) within a small time/size window, serializes them into one payload, and
sends one produce — amortizing the round-trip and the leader's fsync. Batching is a
client-side concern that rides on the existing wire shape; no protocol change. (Server
already batches at the WAL; client batching stacks on top.)

---

## Compression

Compression is **optional and end-to-end**, and by design the **server never
decompresses** — it stamps an entry id and then stores, replicates, and serves the
payload exactly as the producer wrote it (the broker-opaque payload contract the storage
engine already guarantees — `d1`). Keeping (de)compression at the edges spares the
broker's hot path the CPU and lets each producer trade compute for bytes on its own
terms.

Because the broker is opaque, the codec has to be **self-describing** — and it can't sit
*inside* the compressed bytes, or the consumer would have to decompress to learn how to
decompress. So the producer writes a one-byte **codec tag** *in the clear* at the front
of the entry, ahead of the compressed block (`none` / `lz4` / `zstd`); only the records
after it are compressed. The consumer reads that plaintext byte first, then inflates the
rest. Carried this way the tag survives wherever the bytes do — hot cache, follower
replica, sealed segment file — and stays opaque to the broker, which still sees one blob.
It deliberately does *not* ride the broker's routing header beside `record_count`: that
header is stripped when the entry is checkpointed to a segment file, so a cold reader
would never see it — harmless for `record_count` (re-derived by parsing) but fatal for a
codec the consumer must know *before* parsing. `none` is first-class (small or already-
compressed batches skip the codec), which is why the tag is explicit rather than assumed.

Compression runs **once per entry**, over the records block (everything after the tag),
amortized across every record in it — so it stacks cleanly on batching: coalesce
same-range records into one payload (above), compress that block once, ship one produce.
It's orthogonal to the idempotency seam — a session/sequence stamp says *which* entry
this is; the codec tag says *how to read* it.

---

## Delivery contract and idempotency seam

Today produce is explicitly **at-least-once**: a produce that times out after the
leader committed but before the client saw the acknowledgment can be retried and stored
twice. The UUID and per-record counter currently allocated by the producer do not cross
the wire and do not participate in a broker decision.

The future protocol is specified in
[D10: Idempotent Production](../data-plane/d10_idempotent_production.md). It assigns a
sequence to each immutable broker batch, orders independently routed batches in lanes,
uses metadata-backed incarnations for fencing, and retains range-scoped deduplication
frontiers across rolls, failover, and lineage changes. A bounded recent-result window
returns exact positions for normal retries without retaining one position forever per
request. This is intentionally described as idempotent production rather than end-to-
end exactly-once processing.

Until D10 is implemented end to end, callers must not interpret construction with a
stable producer UUID as a delivery guarantee.

---

## What needs to be done

1. **Produce API** — `send(topic, key, payload, record_count)` returning the committed
   entry id (or a terminal error), built on C1's connection pool and redirect loop.
2. **Key routing** — map the application key to routing-key bytes, find the owning
   range in the cache, resolve the active segment's write leader.
3. **Redirect handling** — `ShardNotLocal` / `NotWriteLeader` via C1's loop;
   `TopicNotFound` terminal; transient not-ready → backoff retry.
4. **Batching** — coalesce same-range records into one produce within a time/size
   window.
5. **Compression** — optional client-side codec over the (batched) records, with a
   cleartext codec tag prefixing the compressed block so the consumer can decompress; the
   broker stays opaque and never decompresses.
6. **Idempotency seam** — a no-op-today identity allocation point. D10 will replace the
   per-record counter with immutable batch identity and carry it through the broker
   protocol.
7. **Tests** — against the simulated cluster: produce to the right leader from a warm
   cache (one hop), correction after a roll/failover (`NotWriteLeader` follow),
   correction from a cold/stale cache (`ShardNotLocal` follow), `TopicNotFound`
   surfaced, batched produce of N records acked as one, a compressed batch tagged with
   its codec and stored by the server byte-for-byte.

## See also

- `c1_routing_and_connections.md` — the routing cache and redirect loop this builds on.
- `d6_produce_consume_api.md` — the server's produce routing and redirect contract.
- `d1_storage_engine.md` — the broker-opaque entry payload and end-to-end compression
  this stamps a codec into.
- `../data-plane/d10_idempotent_production.md` — session fencing, ordered lanes,
  durable range ledgers, topology handoff, and bounded retry guarantees.
- `client_roadmap.md` — the idempotency / batching / compression backlog context.
