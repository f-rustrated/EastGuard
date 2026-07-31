# EastGuard Security Roadmap

**Goal:** Provide a secure, default-deny production boundary for all nodes and clients without sacrificing online certificate rotation, node restarts, or network partition recovery.

**Depends on:** SWIM membership, Raft consensus, data-plane placement, client protocol, and client redirect handling.

---

## 1. Overview & Threat Model

| Mode | Boundary |
| :--- | :--- |
| **Secure (default)** | TLS 1.3 protects TCP. Plaintext, downgrade, invalid configuration, and unauthenticated or unauthorized traffic are rejected. Startup fails while secure SWIM UDP is unavailable. |
| **Trusted development** | Explicit opt-in permits plaintext only in isolated test environments. |

### Threat Model

The boundary covers eavesdropping, modification, replay, UDP spoofing,
connection injection, and downgrade. Compromised brokers, authorized clients,
and Byzantine consensus are out of scope.

### Listener Architecture

| Listener | Port | Protocol | Peer Authentication | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Client** | TCP 2921 | TLS 1.3 | Mutual X.509 | Metadata queries, administration, produce, fetch |
| **Raft** | TCP 2922 | TLS 1.3 | Mutual X.509 + process admission | Metadata shard consensus, one-shot ACL-cache refreshes, and limited admission-record reads between brokers |
| **Data** | TCP 2923 | TLS 1.3 | Mutual X.509 | Segment replication, repair, and coordination |
| **SWIM** | UDP 2922 | Secure datagrams (deferred) | Mutual X.509 | Membership gossip and failure detection |

---

## 2. Layered Architecture

Security checks are split across transport, one broker security actor, and the
application state machines. This keeps security I/O out of state machines:

```
[ Authenticated Transport Layer ]
       TCP: TLS 1.3
       UDP: secure datagrams (deferred)
  - Authenticate peer X.509 certificates
  - Prove that a node connection belongs to the currently admitted process
  - Enforce framing, datagram MTU, and resource limits
  - Bind connection envelope sender to verified identity
                     │
                     │ (Drop connection on transport failure)
                     ▼
[ Broker Security Actor ]
  - Cache admission and ACL records
  - Route record reads to local Raft or one remote broker
  - Authorize the authenticated principal
                     │
                     │ (Deny one request on authorization failure)
                     ▼
[ Application Layer State Machines ]
  - Execute SWIM / Raft / Data-Plane state transitions
```

| Boundary | Rule |
| :--- | :--- |
| Transport | Authenticates, bounds frames and handshakes, and binds senders to verified identities. Failure closes the connection. |
| Security actor | Owns caches, record reads, and authorization. Denial drops one request, not the connection. |
| State machine | Applies only authenticated and authorized operations. |
| Data placement | Replication and repair use local committed placement, never a sender-asserted replica list. |
| Redirect | Carries only an address hint. The destination repeats authentication and authorization. |

The actor owns only shared, non-durable state: both caches, record routing, and
identical-read combining. Certificates, connection lifetimes, frame parsing,
and durable records stay outside. Slow remote reads run in the background.
Admission and ACL refreshes share one active-read limit; saturation fails
closed instead of creating another queue or actor.

---

## 3. Node Identity & Admission Model

To allow safe node restarts and hardware replacement without exposing the cluster to stale process replay, EastGuard decouples reusable identity from running process instances.

### Identity Hierarchy

| Term | Scope | Lifetime / Ordering | Function |
| :--- | :--- | :--- | :--- |
| **Node Certificate Principal** | X.509 Certificate | Long-lived / Reused | Stable admission-record key read from the certificate. |
| **Admission Epoch** | Node Certificate Principal | Increasing `u64` | Metadata Raft assigns it; a higher value fences older processes. |
| **NodeId** | Running Process | Single process lifetime | Startup-generated ID used by SWIM, topology, Raft, and placement. |
| **Process Key** | Running Process | Single process lifetime | Proves that the connection belongs to the process admitted for this epoch. |
| **SWIM Incarnation** | Running Process | Increasing counter | The same process increments it to refute stale liveness gossip. |

The Node Certificate Principal is the value after `urn:eastguard:node:` in one
URI Subject Alternative Name. A node certificate must contain exactly one such
URI; missing or repeated values fail authentication.

### Resolution Rules

Node identity conflicts and stale gossip are resolved in this order:

```
 1. Admission Epoch   ──► Higher epoch always wins & fences older processes
          │ (If equal)
          ▼
 2. SWIM Incarnation  ──► Higher incarnation refutes stale liveness facts
          │ (If equal)
          ▼
 3. Liveness State    ──► Dead > Suspect > Alive
```

### Partition Recovery vs. Node Restart

An **authorized operator** is a person or automation whose client-certificate
principal has the `security/cluster` grant.

| Event | Identity and recovery |
| :--- | :--- |
| Healed partition; same process | Keep the `NodeId` and epoch. Increase the SWIM incarnation to refute stale `Suspect` or `Dead` gossip. |
| Restart or replacement | Create a new `NodeId` and process key. The operator approves them; metadata Raft commits a higher epoch. Cache expiry fences the old process everywhere within 60 seconds. |

### Admission Gate & SWIM Separation

SWIM liveness gossip is decoupled from cluster admission authority to prevent network partitions from admitting unauthorized nodes:

```
  [ Authorized Operator ] ──► Approve Joining Node's NodeId & Process Key
                                             │
                                             ▼
  [ Joining Node ] ──► Limited Admission Endpoint ──► Metadata Raft
                                                        │
                                                        ▼
                                  [ Epoch + NodeId + Process Public Key ]
                                                        │
                                                        ▼
  [ SWIM Actor ] ◄──────────── Check Admission Cache ───┘
           │
           ▼ (Accepted)
  [ SWIM State Machine ] ──► Liveness Gossip (Alive / Suspect / Dead)
           │
           ▼
  [ Raft Reconciliation ] ──► Commit AddPeer / RemovePeer
```

- Metadata Raft decides admission; SWIM reports only liveness.
- The transport authenticates the immediate sender. A relayed fact is accepted
  only when its subject `NodeId` and epoch match an active admission.
- The local admission cache expires within 60 seconds and fails closed when its
  owning shard is unavailable.

### Why a TLS Session Proof Is Necessary

A node certificate identifies a reusable broker principal, not one process
start. A UUID prevents accidental identity collisions, but it is public cluster
data. An old process that still has the reusable certificate can observe and
claim the current `NodeId` and `Admission Epoch`.

```
NodeId       ──► which process the record names
Epoch        ──► which admission is newer
Process key  ──► proof that the speaker owns that admission
TLS value    ──► proof is valid only on this connection
```

The admission record therefore stores a public key for one process start. It is
approved and committed before the connection. The broker sends a signature, not
a replacement key. Both sides prove their keys because Raft traffic is
bidirectional.

```
Connecting broker                         Accepting broker
       │                                         │
       │◄────────────── mTLS ───────────────────►│
       │ derive the same fresh TLS session value │
       │── process proof + Raft / ACL ──────────►│
       │                                         │ verify current admission
       │◄──────────── process proof ─────────────│
       │ verify current admission                │
       │◄──────── admitted connection ──────────►│
```

A TLS exporter derives application-specific bytes from a completed handshake:

```
connection 1: broker A derives X    broker B derives X
connection 2: broker A derives Y    broker B derives Y
                                      X != Y
network:      sends signatures over X or Y, never X or Y itself
```

“Shared” means both ends of one connection derive the same value. A signature
over `X` fails on a connection using `Y`; this removes the need for another
challenge. Missing or stale admission, identity mismatch, or bad signature
closes the connection before Raft or ACL dispatch.

### Why Admission Lookup Has a Narrow Wire Path

The admission record may live on another broker. A normal cluster connection
would recurse:

```
Need record ──► open admitted connection ──► need record ──► loop
```

A narrow pre-admission path breaks the loop:

```
Accepting broker                         Admission shard host
       │                                          │
  1. Open ─────────────── mTLS ──────────────────►│
       │                                          │
  2. Ask ─────── one admission-record key ───────►│
       │                                          │ read committed state
  3. Return ◄────────── record or no record ──────│
       │                                          │
       └──────────────── connection closes ───────┘
```

- **Authentication:** Secure mode requires mTLS. It authenticates the reusable
  node certificate, not the running process.
- **One purpose:** The connection reads one admission record. It cannot carry
  Raft messages, ACL reads, client requests, or admission writes.
- **Bounded work:** The broker security actor combines simultaneous reads for
  the same record. Admission and ACL reads share one active-read limit.
- **Cache result:** A record or confirmed missing record is cached for at most
  60 seconds. A missing record denies admission.
- **Do not cache failure:** Timeout, routing failure, or an unavailable shard
  denies the current connection but is retried by a later lookup.

---

## 4. Authorization & Sharded Security Records

ACLs are exact and default deny, with no wildcards or inheritance. A principal
comes from the client certificate; its text grants no authority by itself.

### ACL Resource Catalog

| Resource Key Format | Granted Actions |
| :--- | :--- |
| `cluster` | Create and list topics, membership inspection, topology lookup, operator diagnostics |
| `topic-admin/{topic-id}` | Delete and describe topic metadata |
| `topic-data/{topic-id}` | Produce, fetch, list offsets for topic |
| `consumer-group/{topic-id}/{group-id}` | Coordinate the group and read/commit its offsets |
| `producer-session/{topic-id}/{producer-id}` | Renew the session, bound to its creator for the session lifetime |
| `security/cluster` | Read/write ACLs, manage admissions and revocations, inspect security audit |

Consumer-group access covers coordination and offsets. Fetching records also
requires `topic-data/{topic-id}`. Text keys exist only at routing and
administrative boundaries; replicated state stores typed resources.

### Sharded Metadata Storage

Admission, ACL, and revocation paths hash to standard metadata shards:

```
Request ──► hash record path ──► shard host? ─┬─► yes: commit through Raft
                                              └─► no: return owner redirect
```

```
security/node/{node-certificate-principal}
                 │
                 └── Admission Epoch + NodeId + Process Public Key
```

| Property | Rule |
| :--- | :--- |
| No controller | Every record is owned and replicated by its ordinary metadata shard. |
| Stable admission key | Restart changes the `NodeId` and process key, not the certificate principal, so one record atomically replaces the old process. |
| Local authorization | Cached ACLs and stable topic IDs let a data replica authorize without hosting topic metadata. |
| Freshness | Cache entries carry source shard, revision, and a monotonic deadline no later than 60 seconds. Expiry triggers refresh or denial. |

---

## 5. Operations & Credential Lifecycle

### Bootstrap & Node Joining

| Moment | Operator and cluster action |
| :--- | :--- |
| First broker | Create the trust root and node certificate. Initial metadata stores the first process admission, operator principal, and `security/cluster` grant. |
| Every later start | The process creates a new `NodeId` and process key. An authorized operator approves both. The owning shard increments the admission epoch and replaces the old process atomically. |
| Cluster connection | Each side follows the admission gate in Section 3. Raft or ACL traffic starts only after mutual process proof succeeds. |

The reusable node certificate alone cannot replace an admitted process. SWIM
also remains blocked until the secure datagram admission gate in S3 exists.

### Online Credential Rotation

| Operation | Required behavior |
| :--- | :--- |
| CA or leaf rotation | Load old and new trust chains together; reload leaves without restart or process-identity change. |
| Revocation or expiry | Commit revocations through metadata Raft; close active sessions within cache expiry; allow bounded clock skew. |
| Recovery | Cover lost operator access or issuing keys, accidental revocation, trust-root replacement, and cold restart. |

---

## 6. Resource Limits & Security Audit

### Rate & Memory Bounds

| Boundary | Limit |
| :--- | :--- |
| Listener | Bound unauthenticated handshakes, connections, in-flight frames, and allocations. |
| Client requests | Rate limiting is deferred until principal sharing and node-wide limits are defined for autoscaling workloads. |
| Future secure UDP | Keep protected payloads below the IP-fragmentation threshold. |

### Client Request Boundary

Clients normally route directly to the data replica named by their current topic
metadata. A redirect is only recovery from stale routing; brokers never proxy a
produce or fetch to another data node.

```
Client request
      │
      ▼
Authenticate certificate
      │
      ▼
Check local ACL cache
      ├── Current grant
      ├── Current denial ─► Return unauthorized
      └── Missing / expired
                   │
                   ▼
      Read committed ACL state
            ├── Local shard ─────────► Local metadata read
            └── Remote shard ────────► One authenticated node request
                  │
                  ▼
             Refresh cache
                  │
            ┌─────┴─────┐
            ▼           ▼
          Grant      Deny / unavailable ──► Fail closed
            │
            ▼
 Does this node serve the requested data?
      ├── No  ──► Return data-node redirect
      └── Yes ──► Execute locally
```

| Decision | Rule |
| :--- | :--- |
| Authorize before redirect | An ungranted client cannot discover placement through stale-route responses. |
| Refresh on cache miss | Read local committed state or make one authenticated ACL-only request to the shard host. Never proxy client data. |
| Fail closed | Cache a missing record as a bounded denial. An unavailable owner denies without caching the failure. |

Pull-on-miss avoids a second connection pool for reads needed at most once per
cache window. A future push or hybrid design may update only the same cache and
must remain fail closed.

The broker security actor owns these remote reads and combines simultaneous
requests for the same ACL record. When its shared read limit is full, stopped,
or too slow, callers deny rather than opening more connections:

```
many cache misses
       │
       ▼
 broker security actor
       ├── same shard + resource ──► one read, reply to all waiters
       ├── different records ──────► bounded background reads
       └── full / unavailable ─────► deny
```

Only identical records combine. If distinct records to one shard become costly,
batch those resource keys in one request; do not delay mailbox reads or add a
connection pool.

Client rate limiting remains deferred:

| Principal model | Problem with a fixed per-principal limit |
| :--- | :--- |
| Shared by application replicas | One limit represents an autoscaling workload. |
| Unique per replica | Principal state grows with replica count. |

Identity granularity, node-wide capacity, and cache distribution must be chosen
together before adding a limit.

### Secure UDP Decision

EastGuard retains UDP for SWIM because connection-oriented transport does not
fit membership at cluster scale:

```
       SWIM probes one peer per interval
                    │
                    ▼
              stateless UDP
                    │
       ┌────────────┴────────────┐
       ▼                         ▼
constant socket count      packet loss remains
per node                   visible to SWIM
```

| Alternative | Why it is not selected now |
| :--- | :--- |
| TCP | Either keeps a connection mesh or causes handshake churn, kernel tracking, and head-of-line blocking. |
| QUIC datagrams | Preserve loss, but add per-peer connection state and complexity for sparse probes. |
| Current DTLS libraries | Do not yet combine maturity, permissive licensing, Rust integration, and deterministic simulation. |

Secure SWIM remains deferred. Trusted development may use plaintext UDP in
isolation; secure mode fails startup and never falls back to it.

### Acceptance Criteria for a Future Secure UDP Transport

The selected transport must:

| Requirement | Reason |
| :--- | :--- |
| Preserve datagram boundaries and loss | SWIM timeouts and indirect probes must observe loss rather than transport retransmission delays |
| Keep per-node transport state bounded independently of cluster size | Membership must remain viable for clusters with thousands of nodes |
| Authenticate node certificates and expose the certificate principal | Admission must bind each packet source to a verified node identity |
| Reject replay and spoofed source traffic | Old or forged membership packets must not alter liveness |
| Avoid IP fragmentation | One lost fragment must not discard an oversized protected packet |
| Run over EastGuard's UDP abstraction | Production and turmoil must exercise the same protocol state machine |
| Use virtual time in deterministic tests | Handshake retry, expiry, and packet loss must be reproducible |
| Use a mature, maintainable, permissively licensed dependency | Cluster security must not rely on an unaudited or incompatible implementation |

### Audit Subsystem
- **Non-Blocking Execution:** Security audit events (authentication success/failure, ACL denials, admissions) are queued asynchronously. Audit backpressure never blocks protocol execution or consensus.
- **Rate-Limited Flood Protection:** High-frequency audit events use aggregate counters and sampled detail logging.
- **Credential Hygiene:** Audit logs never record private keys, tokens, credentials, or message payloads.

---

## 7. Delivery Plan (S0–S6)

| Phase | Complete when |
| :--- | :--- |
| **S0 — Configuration** | Secure configuration loads certificates, opens no plaintext listener, and fails startup when a required secure listener is unavailable. |
| **S1 — Records** | Sharded admission, ACL, and revocation records survive snapshot and recovery. |
| **S2 — Cluster TCP** | TLS 1.3 and session-bound process proof admit Raft, ACL, and data traffic only from the current process. |
| **S3 — SWIM (deferred)** | A transport meeting Section 6's UDP criteria provides authenticated gossip and partition-safe admission fencing. |
| **S4 — Clients** | Client mTLS and ACLs enforce default deny on every API. |
| **S5 — Operations** | Rotation, revocation, expiry, recovery, and non-blocking audit work online. |
| **S6 — Production** | Impersonation, replay, downgrade, fuzzing, resource bounds, recovery, and reproducible secure-SWIM tests pass. S3 must be complete. |
