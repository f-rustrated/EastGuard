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
                    +------------+------------+
                    |                         |
        state.take_timer_commands()   state.take_outbound()
                    |                         |
                    v                         v
    mpsc::Sender<TickerCommand>   mpsc::Sender<OutboundPacket>
       (scheduler_tx)                 (transport_tx)
```

**Fields:**
- `mailbox` -- `mpsc::Receiver<SwimCommand>` -- inbound event stream
- `transport_tx` -- `mpsc::Sender<OutboundPacket>` -- sends UDP packets to transport actor
- `scheduler_tx` -- `mpsc::Sender<TickerCommand<SwimTimer>>` -- sends timer set/cancel commands to ticker

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

### Query

Read-only queries (GetMembers, GetTopology) answered via oneshot reply channels. No state mutation.

## Outbound Side Effects

After every event, `flush_outbound_commands` drains two buffers from `Swim` and sends concurrently via `tokio::join!`:

1. **Timer commands** (`TimerCommand<SwimTimer>`) -- sent to `scheduler_tx`
   - `SetSchedule { seq, timer }` -- register new timer
   - `CancelSchedule { seq }` -- cancel existing timer
2. **Outbound packets** (`OutboundPacket`) -- sent to `transport_tx`
   - Each packet has `target: SocketAddr` and `SwimPacket` payload

Flush also runs once at startup (before event loop) to drain commands produced during `Swim::new()` init.

## Key Dependencies

| Dependency | Role |
|---|---|
| `Swim` (swim.rs) | Pure state machine. All protocol logic. No async, no I/O. |
| `GossipBuffer` | Piggybacks membership updates onto outbound packets, O(log N) dissemination. |
| `LiveNodeTracker` | Round-robin probe target selection, excluding dead/suspect nodes. |
| `Topology` | Consistent hash ring updated on membership changes. |
| `Bootstrapper` / `JoinAttempt` (peer_discovery.rs) | Seed-node join w/ exponential backoff. |
| `SwimTimer` (messages.rs) | Timer definitions w/ tick counts per probe phase. |

## Invariants

1. **Every event must follow with `flush_outbound_commands`.** `Swim` buffers all side effects internally. Skip flush = packets and timer commands silently lost. `run()` loop enforces this — no code path processes event without flushing.

2. **Actor is sole driver of `Swim` state machine.** No other code calls `step()`, `handle_timeout()`, or `handle_query()` on `Swim` in production. Ensures single-threaded access without locks.

3. **`Swim` fully synchronous.** No I/O, no awaits, no channels. Separation makes protocol logic deterministically testable.

4. **Timer commands flow through scheduler, not actor.** Actor converts `TimerCommand` into `TickerCommand` (via `.into()`) and sends to ticker actor. Ticker fires `SwimTimeOutCallback` back into actor mailbox on expiry.

5. **Sequence numbers (`seq`) correlate probes to responses.** Each probe gets unique `seq`. Ack messages carry same `seq` to cancel corresponding timer. Also how proxy pings correlated.