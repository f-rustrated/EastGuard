# EastGuard Security Roadmap

**Goal:** Keep authentication integration, minimal broker permissions, and
protocol hardening. Delegate credential infrastructure and network protection.

## Deployment scope

Trusted brokers run inside an operator-protected network covering TCP **and SWIM
UDP**. Standard mTLS supplies verified principals; EastGuard checks their static
permissions. Policy and credential changes use deployment/restart procedures.
Compromised brokers are outside this trust model.

The [deployment contract](deployment.md) defines certificates, policy format,
credential replacement, and closing existing sessions when access is withdrawn.

## Implemented

- Bounded startup policy, with exact topic-data, topic-admin, consumer-group,
  and cluster grants. Missing identity or permission denies access.
- Authorization before redirects and topic-ID checks at committed apply, including
  after recovery. Producer-owner checks; discovery without deletion permission.
- SDK and operator CLI mTLS across seeds, redirects, and reconnects.
- Distributed ACL actors, quorum reads, caches, expiry bookkeeping, persisted
  security records, and grant/revoke commands removed. No legacy compatibility:
  this project has never been deployed.
- Bounded record counts, decompression, fetch assembly, and wire frames;
  payload-free error logs, handler limits, and transport timeouts retained.
- Peer IDs bound to authenticated connections. Replication, acknowledgments,
  deletion, and catch-up checked against coordinator-published committed data
  placement. Missing, older, and conflicting placement denied.
- mTLS cluster formation, partition healing, and restart with persisted metadata
  and data verified in simulation. Secure startup enabled.

Trusted development remains explicit. Keep production-code and test-code changes
visible when reporting; simplification must remove replaced code and tests.

## Deferred until required

Dynamic persisted ACL administration, online reload, broker-managed revocation,
fixed session-closure deadlines, a separate audit pipeline, and EastGuard-owned
secure datagrams. These are deployment-specific extensions, not prerequisites.
