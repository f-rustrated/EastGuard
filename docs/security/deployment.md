# Static Security Deployment

**Status:** Policy loading and SDK mTLS are implemented. Secure startup remains
disabled until the [remaining placement and recovery work](roadmap.md) is verified.

## Trust boundary

Operators admit trusted brokers and protect **both TCP and SWIM UDP**. SWIM is
plaintext: use an authenticated, encrypted private network and restrict its
membership. A TCP-only mesh is insufficient. Compromised brokers are outside
this trust model.

EastGuard uses rustls for mutual TLS. Operators supply the certificate chain,
private key, and trust roots. Broker certificates carry one URI SAN,
`urn:eastguard:node:<principal>`; the node-ID prefix must equal that principal.
Client certificates carry `urn:eastguard:client:<principal>`. Only verified
certificates supply principals; request fields cannot assert an identity.

The SDK's `Client::connect_secure` accepts a standard rustls client configuration
with trust roots and a client certificate. Broker certificates must also cover
their advertised IP addresses. Seeds, redirects, and reconnects use the same TLS
configuration. Plaintext SDK constructors are for trusted development.

## Permissions

Install the same JSON file on every broker and set `--permissions-path` (or
`PERMISSIONS_PATH`). The policy is read once, before listeners open, with a 1 MiB
limit. Missing files, malformed policy, duplicate keys, unmapped IDs, and invalid
identifiers fail startup. Missing principals or grants deny access.

```json
{
  "topics": {"orders": 42},
  "grants": {
    "cluster": ["operator"],
    "topic-admin/42": ["operator"],
    "topic-data/42": ["order-service"],
    "consumer-group/42/billing": ["order-service"]
  }
}
```

Cluster permission allows creation, cluster inspection, topic listing, and
metadata discovery. Topic-admin allows description and deletion; topic-data
allows produce, fetch, offsets, producer sessions, and routing metadata.
Consumer-group permission allows group membership and its offset operations.
Grants are exact and independent; cluster permission does not imply topic-data
or deletion permission. Producer-session ownership remains enforced on append.

To provision a topic, an operator creates it and discovers its ID using cluster
permission, then installs its name/ID mapping and grants on all brokers. The
binding allows authorization before redirects and rejects a recreated topic
until its new ID is configured. There are no runtime grant/revoke endpoints.

## Rotation and access withdrawal

Policy and credentials are immutable for a running broker. Deploy replacements
and restart brokers to reload them; replace SDK clients to reload client
credentials. Certificate expiry is checked at handshake, not continuously.

To withdraw client access, restrict admission during the rollout, remove its
grants on every broker, and close existing sessions by restarting all serving
brokers before reopening admission. To withdraw broker access, isolate it from
TCP **and UDP**, replace affected trust material, and close existing peer
connections. A shared CA may require CA rotation to exclude one issued
certificate; this release has no broker-managed revocation service.

No deployment exists yet. Old security snapshots, log commands, and wire wrappers
are removed without migration support. Use fresh development storage with this
format.
