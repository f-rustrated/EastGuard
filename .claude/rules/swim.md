# SwimActor

## Purpose

`SwimActor` — protocol layer actor driving SWIM failure-detection state machine (`Swim`). Owns async boundary: receives commands from mailbox, feeds into synchronous `Swim`, flushes side effects (outbound packets + timer commands) to external channels.

Actor has no protocol logic. All decisions in `Swim`.

## Architecture

```
                       mpsc::Receiver<SwimCommand>
                                 |
                                 v
                          +-----------+
                          | SwimActor |
                          +-----------+
                          |  mailbox  |  <-- receives SwimCommand
                          +-----------+
                                 |
                          calls into Swim
                                 |
                          state.take_events()
                                 |
                        SwimEvent routing
                       /         |         \
              Packet         Timer      Membership
                 |              |            |
                 v              v            v
          transport_tx    scheduler_tx    raft_tx
```


## Message Flow

`SwimCommand` has three variants:

| Variant | Source | Swim method called |
|---|---|---|
| `PacketReceived { src, packet }` | Transport layer (UDP) | `state.step(src, packet)` |
| `Timeout(SwimTimeOutCallback)` | Ticker (timer expiry) | `state.handle_timeout(tick_event)` |
| `Query(SwimQueryCommand)` | External callers (e.g., HTTP API) | `state.handle_query(command)` |

### PacketReceived

Handles inbound `SwimPacket` (Ping, Ack, PingReq). `Swim` processes gossip, updates membership, buffers response packets (Ack replies, forwarded proxy Acks).

### Timeout

Dispatched by ticker on timer expiry. Drives probe lifecycle:
- `ProtocolPeriodElapsed` -- start new probe round (Ping next node)
- `DirectProbe` timed out -- escalate to indirect probing (PingReq to K peers)
- `IndirectProbe` timed out -- mark target Suspect
- `Suspect` timed out -- mark target Dead
- `ProxyPing` timed out -- clean up stale proxy ping entry
- `JoinTry` timed out -- retry or give up bootstrap join

## Outbound Side Effects

After every event, `flush_outbound_commands` drains three buffers from `Swim` and sends concurrently via `tokio::join!`:

1. **Timer commands** (`TimerCommand<SwimTimer>`) -- sent to `scheduler_tx`
   - `SetSchedule { seq, timer }` -- register new timer
   - `CancelSchedule { seq }` -- cancel existing timer
2. **Outbound packets** (`OutboundPacket`) -- sent to `transport_tx`
   - Each packet has `target: SocketAddr` and `SwimPacket` payload
3. **Raft commands** (from `MembershipEvent`s) -- sent to `raft_tx`
   - `NodeDead` → `RaftCommand::HandleNodeDeath` (leader proposes RemovePeer)
   - `NodeAlive` → query `topology.shard_groups_for_node()` locally → `RaftCommand::HandleNodeJoin` (leader proposes AddPeer, EnsureGroup for new memberships)
   - No event on Suspect (not actionable for Raft group lifecycle)

Flush also runs once at startup (before event loop) to drain commands produced during `Swim::new()` init.


## Invariants

1. **Actor is sole driver of `Swim` state machine.** No other code calls `step()`, `handle_timeout()`, or `handle_query()` on `Swim` in production. Ensures single-threaded access without locks.

2. **`Swim` fully synchronous.** No I/O, no awaits, no channels. Separation makes protocol logic deterministically testable.

3. **Timer commands flow through scheduler, not actor.** Actor converts `TimerCommand` into `TickerCommand` (via `.into()`) and sends to ticker actor. Ticker fires `SwimTimeOutCallback` back into actor mailbox on expiry.

4. **Sequence numbers (`seq`) correlate probes to responses.** Each probe gets unique `seq`. Ack messages carry same `seq` to cancel corresponding timer. Also how proxy pings correlated.