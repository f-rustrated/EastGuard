# EastGuard Security Roadmap

**Goal:** Integrate standard authentication, enforce the application's required
permissions, and bound protocol work. Delegate credential infrastructure to the
operator. Choose the deployment trust model before adding security features.

**Depends on:** SWIM, metadata Raft, data placement, and client routing.

Secure production startup is **not available yet**. It fails before opening
listeners. Plaintext requires the explicit `trusted-development` setting and
an isolated environment. Section 7 separates working code from remaining work.

---

## 1. Trust Model

The proposed near-term scope is authenticated clients with operator-managed
static permissions. Runtime grant/revoke is an extension for deployments that
need it. This proposal does not describe a new working configuration: the
current permission checks still use persisted ACLs, and secure startup still
aborts before opening listeners.

| Responsibility | Owner |
| --- | --- |
| Identity provisioning, certificate issuance, secret distribution, and rotation procedures | Operator's identity and security infrastructure |
| Encryption and network access restrictions | Deployment infrastructure and/or standard TLS libraries |
| Topic, consumer-group, and administration permissions | EastGuard |
| Malformed input, bounded allocations, and protocol correctness | EastGuard |

If every admitted client is deliberately trusted with every operation, a
deployment-enforced access boundary is another possible product scope. It
provides no topic-level separation between those clients. That choice must be
explicit and tested; the existing trusted-development mode is not automatically
a supported production deployment.

A TCP proxy cannot decide whether a request may fetch or delete a topic without
understanding EastGuard's protocol. Where permissions are required, the broker
needs a trustworthy client identity even if infrastructure terminates TLS.
Persisted permissions are an ordinary broker design: [Kafka stores ACLs in its
metadata log](https://kafka.apache.org/42/security/authorization-and-acls/), and
[Pulsar provides authentication and authorization](https://pulsar.apache.org/docs/4.2.x/security-overview/).
Their existence alone does not justify removing them; deployment requirements
decide whether EastGuard needs their administration and recovery machinery.

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
| SWIM | UDP 2922 | Deployment protection covering UDP, or a standard secure datagram transport |

Use cluster-specific trust roots and a different private key for each broker.
Do not use a shared broker identity or a general public CA trust store.
A redirect is only an address hint: its destination must authenticate and
authorize again.

## 2. Who Checks What

The current implementation uses Raft-backed permissions:

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

## 4. Client Permissions and the Existing ACL Cache

For the proposed static-permission baseline, operators would distribute the
permission policy with deployment configuration. Missing or invalid policy must
deny access. Policy changes would use a documented deployment or restart
procedure. Static policy loading is not implemented yet; it would replace the
authorization source, while keeping checks at the broker request boundary.

The existing persisted path below remains in use until that transition is
implemented. Preserve existing snapshot readability. If a deployment keeps
persisted permissions, their recovery remains required even when wire
administration is deferred.

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
creation and renewal use topic-data permission and bind the session to its
creator. Appends also verify that owner. When local session metadata is absent,
authenticated appends fail closed because the recovered data ledger cannot
prove ownership; trusted-development recovery can still use that ledger.
A separate producer-session resource exists in storage but is not an enforced
permission.

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
needs mTLS for initial connections and redirects. Dynamic ACL administration,
global listing, pagination, and their recovery requirements belong to the
runtime grant/revoke extension, rather than the proposed static baseline.

Legacy admission records remain in snapshots solely to preserve the existing
storage format. They are not read for authentication. Stored revocation records
also do not enforce revocation by themselves.

## 5. Bootstrap and Operator-Managed Credentials

Credentials and trust policy must be locally available before Raft starts.
Their verification must not require a fresh read through the Raft connection
being authenticated. Use maintained TLS/PKI facilities, not another custom
signature protocol.

Secure genesis is explicit cluster formation, not an automatic reaction to an
empty directory. Initial members must agree on initial membership and the first
operator grant. Persist that initialization was applied and reject conflicting
input on restart.

Shard recovery must preserve any ACL records still used for authorization as
membership changes. Hashing a record path does not transfer its state to a new
owner. A static policy source removes that particular dependency only after
the broker actually uses it.

EastGuard consumes credentials; it does not issue certificates, distribute
private keys, or replace the operator's identity service. The deployment
contract must state how credentials change and how access is withdrawn.

| Operation | Integration requirement |
| --- | --- |
| Process restart | Fresh node ID under the same certified broker name; no metadata admission |
| Leaf or CA rotation | Operator distributes credentials and chooses a tested restart procedure or reload integration |
| Certificate revocation | State who enforces it and whether existing sessions are closed; promise a deadline only when implemented and tested |
| Certificate expiry | Standard TLS checks new sessions; choose and document treatment of sessions that outlive the certificate |
| Recovery | Operator owns credential recovery; EastGuard owns cluster formation, durable state recovery, and partition behavior |

Revocation must not recreate the admission loop. A bounded revocation promise
requires independently refreshed, locally verifiable policy and fail-closed
behavior when that policy becomes too old. A Raft row or a long-lived
certificate alone cannot provide that promise. The earlier 60-second closure
target is an optional service guarantee, not a universal release requirement.
Deployments requiring it must supply and test its enforcement before release.

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

Client frame bodies are limited to 4 MiB, including the request ID. Writers
measure the encoded size before allocating the serialized body. Hot and cold
fetches budget entry headers and range metadata before assembling a response;
even the first entry must fit the wire limit. Large requests return a bounded
page and a continuation position.

SDK decoded batches are limited to 4 MiB for uncompressed, LZ4, and Zstd data.
Record counts must fit both that limit and the payload's minimum eight bytes
per record before the record array is allocated. Failed SDK writes log request
kind, request ID, destination, and error, without message contents.

Never log private keys or message payloads. Add an audit pipeline when a
deployment needs one; keep it off the protocol's critical path with a bounded
queue and observable drops. Per-principal rates remain deferred until shared
application identities and autoscaling are defined.

### SWIM needs an explicit deployment boundary

SWIM is currently plaintext UDP. Deployment infrastructure may protect that
traffic if the chosen trust model accepts its broker admission boundary and
prevents untrusted access. Verify UDP coverage separately: [Istio does not proxy
UDP](https://istio.io/latest/docs/ops/configuration/traffic-management/protocol-selection/).

If the deployment requires EastGuard itself to authenticate individual datagrams,
select a maintained, permissively licensed secure datagram implementation.
Preserve UDP message boundaries and visible packet loss, and test authentication,
replay rejection, and expiry through virtual time. Do not invent custom
cryptography or restore process admission through metadata.

That integration must bound handshake, session, and replay-tracking memory.
Eviction must not make captured packets valid again. A shared cluster key does
not identify individual brokers. Protecting transport does not replace correct
membership and placement checks inside EastGuard.

## 7. Delivery Plan

| Work | Current state | Scope decision |
| --- | --- | --- |
| Deployment contract | Secure startup aborts; explicit trusted-development mode exists | Select which clients and brokers are trusted, and who protects every listener |
| Protocol hardening | Frame, fetch, decompression, and record-count bounds; payload-free write-error logs | Required under every trust model; malformed-input tests and load measurements remain relevant |
| Standard authentication | Server and broker mTLS exist; SDK integration is unfinished | Complete the identity path required by the selected deployment, including redirects |
| Minimal permissions | Broker checks use persisted ACLs | Proposed baseline: operator-managed static policy; implement and test that source before retiring the existing path |
| Cluster correctness | Sender-supplied placement and restart/recovery gaps remain | Required wherever those protocols operate, regardless of who secures transport |
| SWIM protection | Plaintext UDP | Prove deployment coverage, or integrate a standard secure datagram transport when required |
| Dynamic ACL administration | Internal grant/revoke, quorum reads, and cache exist | Add wire administration when runtime permission changes are required; preserve durability while persisted ACLs are in use |
| Credential lifecycle and audit | Credentials load at startup; advanced session controls are unfinished | Delegate infrastructure; add reload, revocation deadlines, session expiry, and audit features to satisfy explicit deployment commitments |

Do next:

1. Confirm the deployment trust model and its credential and TCP/UDP boundaries.
2. Complete standard client authentication and minimal permission checks for that model.
3. Prove cluster formation, recovery, and committed data placement under that boundary.
4. Run malformed-input, restart, partition, and resource-bound tests. Add dynamic administration and advanced credential operations only for identified requirements.

The current secure startup guard stays closed. Enabling a supported deployment
requires implementing and testing its chosen boundary; completing every optional
extension in this document is not a universal production requirement. Passing
transport tests alone does not establish a recoverable cluster.

The internal opening TCP frames changed. This is not a rolling-compatible
upgrade of the old development protocol; update all brokers together.
Update clients to recognize the new retryable busy reply. Existing error tags
are unchanged, but older clients cannot decode that new reply.
