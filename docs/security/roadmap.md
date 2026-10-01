# EastGuard Security Roadmap

**Goal:** Encrypt traffic, authenticate brokers and clients, and allow clients
only explicitly granted actions. Use ordinary certificate-based broker trust,
with no live metadata read required to establish a broker connection.

**Depends on:** SWIM, metadata Raft, data placement, and client routing.

Secure production startup is **not available yet**. It fails before opening
listeners. Plaintext requires the explicit `trusted-development` setting and
an isolated environment. Section 7 separates working code from remaining work.

---

## 1. Trust Model

A broker certificate is a school ID: it proves who may enter, not who may lead
the class or use every toy. Raft membership, data placement, and client
permissions answer those separate questions.

Protect against network snooping, changed or replayed traffic, forged identities,
and fallback to plaintext. Certificate holders are trusted to run the broker
protocol correctly. Compromised brokers and Byzantine consensus are outside
this design.

**Accepted limitation:** two processes holding the same valid certificate and
private key are equally trusted. A restart does not revoke the old process.
There is no 60-second process-replacement guarantee. Ordinary Raft checks do not
make a stolen broker credential safe; credential expiry and revocation are
separate operational responsibilities.

This follows the certificate-based transport foundation documented by
[CockroachDB](https://www.cockroachlabs.com/docs/v26.2/create-security-certificates-custom-ca)
and [etcd](https://etcd.io/docs/v3.6/op-guide/security/). It is not a claim that
EastGuard's separate data-replication protocol has their correctness guarantees.

| Listener | Default port | Target protection |
| --- | --- | --- |
| Client | TCP 2921 | TLS 1.3, client certificate, request ACL |
| Raft | TCP 2922 | TLS 1.3, node certificate, certificate-bound node ID |
| Data | TCP 2923 | Same identity checks as Raft |
| SWIM | UDP 2922 | Authenticated, encrypted, replay-protected datagrams; deferred |

Use cluster-specific trust roots and a different private key for each broker.
Do not use a shared broker identity or a general public CA trust store.
A redirect is only an address hint: its destination must authenticate and
authorize again.

## 2. Who Checks What

```
Local certificates and trust roots
                |
                v
     Broker mutual TLS + node ID
                |
          +-----+----------------------+
          |                            |
          v                            v
Raft can exchange messages     Client permission checks
and recover a quorum           read Raft-backed ACLs
          |                            |
          v                            v
Committed protocol state       Allow or deny one request
decides operation authority
```

Transport checks identity, sender, frame size, and I/O timeouts. A malformed
frame or forged sender closes the connection. An ACL denial rejects one client
request. An unavailable ACL shard must not stop brokers establishing Raft
connections.

Raft keeps its existing term, log, voter, and learner checks. In particular,
leader replication requests still accept updates from leaders missing from a
lagging replica's local membership; do not mistake that recovery path for
protection against a malicious certified broker.

Data replication and repair must use committed placement, never just the
replica list supplied by a sender. Raft peers and data replicas are different
sets. **This remains a gap:** an append can currently create a follower's local
segment tracker from its supplied replica list. The follower needs a trusted
committed placement source before that path meets the target.

## 3. Broker Connections and Restarts

| Identity | Meaning |
| --- | --- |
| Certificate principal | Broker name from exactly one `urn:eastguard:node:<principal>` URI in the certificate |
| Node ID | That broker's name plus a fresh process suffix: `<principal>::<suffix>` |
| SWIM incarnation | One process's counter for refuting stale liveness gossip |

In secure mode, `--node-id-prefix` must match the certificate principal.
A normal start still generates a fresh suffix. Certificate identity stays
stable across restarts; process identity does not.

```
Connecting broker                       Accepting broker
       |---------- mutual TLS ----------------|
       |---------- its node ID -------------->|
       |<--------- its node ID ---------------|
       |     each checks certificate prefix   |
       |     caller checks exact destination  |
       |---------- application frames ------->|
```

Both sides exchange bounded node IDs inside the authenticated TLS connection.
A certificate for `broker-a` may claim `broker-a::1`, but not `broker-b::1`.
The connecting side also checks the exact expected node ID before sending a
Raft request, ACL read, or data message. An outdated address must not silently
select a different process.

Every subsequent Raft or data frame's sender must match the connection's node ID.
Replacing a connection closes its reader and writer together; a stale
reader-close event cannot remove the replacement.

There are no process signing keys, admission epochs, admission caches,
admission-read endpoints, or recurring admission leases. TLS already proves
possession of the certificate's private key. Keep TLS early data disabled.

The old design required Raft approval to open Raft connections. Removing that
dependency lets transport connect with no metadata service available, including
after more than a minute offline. It does **not** establish full-cluster restart
safety: genesis, durable recovery, and secure membership still need their
end-to-end tests.

SWIM continues to drive membership discovery and reconciliation. Changes to a
Raft group's voting membership still commit through its log. Gossip is not an
independent grant of voter or data-replica authority.

## 4. Client Permissions and the ACL Cache

Client identity comes from exactly one
`urn:eastguard:client:<principal>` certificate URI. Grants are exact, with no
wildcards or inherited permissions. Unknown identities and missing grants deny.

| Resource | Permission |
| --- | --- |
| `cluster` | Create/list topics and inspect membership, topology, and ordinary diagnostics |
| `topic-admin/{topic-id}` | Describe or delete a topic |
| `topic-data/{topic-id}` | Produce, fetch, and read offset bounds |
| `consumer-group/{topic-id}/{group-id}` | Coordinate that group and read/commit its offsets |
| `security/cluster` | Manage ACLs and security administration |

Fetching group data also requires topic-data permission. Producer-session
creation and renewal currently use topic-data permission and bind the session
to its creator. A separate producer-session resource exists in storage but is
not an enforced permission.

ACL records stay in ordinary metadata shards. Each change commits through the
owning shard's Raft group. Reads prove current leadership through a quorum;
an isolated replica's old committed value is not sufficient.

One security actor owns the bounded ACL cache. It reads locally or tries the
owning shard's replicas, starting with the known leader. Reads run in the
background, and identical pending reads share one result.

The actor allows 16 active reads. Its cache holds at most 4,096 records and
16 MiB of accounted entries; one entry is limited to 4 MiB and 4,096 principals.
Each pending fetch has at most 256 waiters. Full queues, unavailable owners,
malformed records, and timeouts deny access.

Cache entries expire 60 seconds after the read **started**, not when the reply
arrived. They track the source shard and revision. Changing the source shard
invalidates an entry. An empty ACL is a cacheable denial; a failed read is not
cached. These limits and checks remain after removing the admission cache.

Authorize before returning placement or redirects. Clients send data directly
to data replicas; brokers must not proxy client data. A data replica must
authorize by stable topic ID even when it does not host the topic's metadata.

**Remaining gaps:** some name-based APIs discover placement before checking an
ACL, produce still depends on local topic metadata, and the Rust client still
needs mTLS for initial connections and redirects. Exact ACL administration
comes before global listing, which needs pagination and partial-failure handling.

Legacy admission records remain in snapshots solely to preserve the existing
storage format. They are not read for authentication. Stored revocation records
also do not enforce revocation by themselves.

## 5. Bootstrap and Credential Operations

Credentials and trust policy must be locally available before Raft starts.
Their verification must not require a fresh read through the Raft connection
being authenticated. Use maintained TLS/PKI facilities, not another custom
signature protocol.

Secure genesis is explicit cluster formation, not an automatic reaction to an
empty directory. Initial members must agree on initial membership and the first
operator grant. Persist that initialization was applied and reject conflicting
input on restart.

Shard recovery must preserve ACL records as membership changes. Hashing a
record path does not transfer its state to a new owner.

| Operation | Required result |
| --- | --- |
| Process restart | Fresh node ID under the same certified broker name; no metadata admission |
| Leaf or CA rotation | Online reload, overlapping old/new trust roots, no process identity change |
| Certificate revocation | Reject new sessions and close matching active sessions within 60 seconds; distribution and freshness policy still need design |
| Certificate expiry | Reject new sessions and close active ones by expiry, with a bounded clock-skew policy |
| Recovery | Procedures for lost operator keys, accidental revocation, trust-root replacement, full restart, and partition healing |

Revocation must not recreate the admission loop. A bounded revocation promise
requires independently refreshed, locally verifiable policy and fail-closed
behavior when that policy becomes too old. A Raft row or a long-lived
certificate alone cannot provide that promise. This remains a production gate,
not an implemented guarantee.

Credentials currently load once, and certificate lifetime is checked during
the TLS handshake. Online reload, revocation enforcement, and active-session
certificate expiry are not implemented.

## 6. Resource Limits, UDP, and Audit

Bound work before spawning tasks or allocating payloads. A slow handshake must
not block an entire listener. These are capacity limits, not per-principal rates.

The client listener allows 1,024 connections, including up to 128 pending TLS
handshakes. It owns handshake and session tasks separately; collecting a finished
task frees its slot. Full listeners close new sockets without an application
waiting queue.

Each connection runs at most 32 request handlers. When full, it returns a busy
reply before dispatching the new request and keeps the connection open. The SDK
retries through its existing backoff and deadline. There is no extra node-wide
request cap: up to 32,768 handlers can run across all connections.

A busy reply means only that attempt was not dispatched. A timeout or lost
connection can still leave a write's outcome unknown; producer deduplication
is separate from transport request IDs, which only match replies to callers.

Client TLS handshakes time out after 10 seconds, handlers after 30 seconds, and
response writes after 5 seconds. Data and Raft each accept at most 128 pending
handshakes, with total deadlines of 16 and 24 seconds respectively. Those totals
include TLS and the remaining opening protocol. Load-test these initial limits
before production.

Keep audit off the protocol's critical path: use a bounded queue, count dropped
events, and sample repeated failures. Never log private keys or message payloads.
Per-principal rates remain deferred until shared application identities and
autoscaling are defined.

### Secure SWIM remains unfinished

Keep UDP message boundaries and visible packet loss. Select a maintained,
permissively licensed datagram-security implementation providing encryption,
broker authentication, and replay rejection. Do not invent custom cryptography.

It must run through EastGuard's UDP abstraction and virtual time so turmoil can
test loss, retries, replay, and expiry. Define validation of relayed membership
facts under the trusted-broker model, without restoring process admission.

Bound handshake, session, and replay-tracking memory. Per-peer state is
acceptable; eviction must not make captured packets valid again. Limit payloads
to avoid IP fragmentation. A single cluster-wide key does not identify individual
brokers, and network isolation alone is development protection.

## 7. Delivery Plan

| Phase | Current state | Complete when |
| --- | --- | --- |
| S0 — Configuration | Secure is default; credential and node-prefix checks exist; startup fails before listeners | No secure configuration opens a plaintext listener |
| S1 — ACLs | Records, internal grant/revoke, quorum reads, and bounded cache exist | Authorized wire administration and durable recovery across ownership changes work |
| S2 — Cluster TCP | Raft/data use mTLS and certificate-bound node IDs without admission reads | Full restart, partition healing, and committed placement tests pass |
| S3 — SWIM | Development UDP is plaintext | Authenticated, encrypted, replay-protected membership works under simulation |
| S4 — Clients | Server mTLS, ACL checks, and request bounds exist | Client mTLS, authorization before redirects, and stable-ID data routing cover every API |
| S5 — Operations | Incomplete | Genesis, rotation, revocation, active-session expiry, recovery, and bounded audit work |
| S6 — Production | Blocked | Earlier gates, malformed-input/fuzz tests, and measured resource bounds pass |

Do next:

1. Define and test secure genesis and durable recovery using certificate-authenticated transport.
2. Finish exact ACL administration, client mTLS, and authorization/routing.
3. Enforce committed data placement and implement credential lifecycle operations.
4. Select secure SWIM, then run full-cluster restart, partition, replay, and load tests.

Keep secure startup closed until these gates pass. Passing transport tests
does not establish a secure, recoverable production cluster.

The internal opening TCP frames changed. This is not a rolling-compatible
upgrade of the old development protocol; update all brokers together.
Update clients to recognize the new retryable busy reply. Existing error tags
are unchanged, but older clients cannot decode that new reply.
