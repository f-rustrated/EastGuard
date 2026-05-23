# Raft Transport

## Purpose

`RaftTransportActor` — async TCP transport for Raft RPCs. Manages persistent bidirectional connections between nodes. Each connection split into `RaftReader` (spawned as tokio task) and `RaftWriter` (held in `RaftWriters` map).

Separate from SWIM's UDP transport (`SwimTransportActor` on `cluster_port`). Raft uses TCP on `raft_port` for reliable, ordered delivery.

## Architecture

```
RaftTransportActor
├── listener: TcpListener           (accepts inbound connections on raft_port)
├── RaftWriters
│   ├── writers: HashMap<NodeId, OwnedWriteHalf>  (one per connected peer)
│   ├── addr_cache: HashMap<NodeId, NodeAddress>   (resolved addresses)
│   └── dead_peers: HashSet<NodeId>                (explicitly disconnected)
└── cleanup_interval: 300s          (periodic dead_peers clear)
```

## Wire Protocol

Length-prefixed bincode frames:
1. **Handshake**: Initiator sends `[len: u32][NodeId: bincode]`
2. **Messages**: Each frame is `[len: u32][WireRaftMessage: bincode]`
3. **WireRaftMessage** carries `shard_group_id` so transport routes to correct Raft group

Size guards: NodeId frames capped at 1KB, message frames at 4MB.

## Connection Lifecycle

1. **Outbound**: On first RPC to a peer, resolve `NodeId → SocketAddr` via SWIM (`ResolveAddress`), connect, handshake, send, spawn reader task.
2. **Inbound**: Accept connection, read handshake `NodeId`, register writer, spawn reader task.
3. **Disconnect**: On `DisconnectPeer` command (from SWIM node death), remove writer + addr cache, add to `dead_peers`.
4. **Cleanup**: Every 300s, clear `dead_peers` set — GC to prevent unbounded growth. Restarted peers already bypass this with a new NodeId (UUID regenerated on every start).

## Invariants

1. **Lower NodeId wins on simultaneous connect.** When both sides connect to each other, the connection initiated by the lower `NodeId` wins. Acceptor checks: if writer for peer already exists AND `peer_id > self.node_id`, the incoming connection is dropped.

2. **Dead peers silently dropped.** Outbound RPCs to nodes in `dead_peers` set are silently skipped (no error, no retry). Cleared periodically (300s) as GC — restarted peers already connect with a new NodeId (UUID regenerated on every start) and bypass the set entirely.

3. **SWIM is authoritative for address resolution.** `NodeId → SocketAddr` resolved via `SwimQueryCommand::ResolveAddress`. Address cached locally in `addr_cache`. Cache cleared on `DisconnectPeer`.

4. **One writer per peer.** `writers` map is `HashMap<NodeId, OwnedWriteHalf>` — at most one write half per peer. On write failure, writer removed from map (next send triggers reconnect).

5. **Reader tasks are fire-and-forget.** Each accepted or established connection spawns a `RaftReader::run()` tokio task. Reader feeds received messages into `raft_tx` channel. On read failure or EOF, task exits silently — no reconnection from reader side.

6. **Messages batched per target.** `send()` groups outbound packets by `target NodeId`, encodes all messages for a target into a single buffer, writes once. Minimizes syscalls.

7. **Transport is Raft-agnostic.** No consensus logic in transport. All inbound messages forwarded as `MultiRaftCommand::PacketReceived`. Transport never inspects or modifies RPC contents.
