# SwimActor

## Purpose

`SwimActor` is the protocol layer actor that drives the SWIM failure-detection state machine (`Swim`). It owns the async boundary: it receives commands from its mailbox, feeds them into the synchronous `Swim` state machine, and flushes all resulting side effects (outbound packets and timer commands) to external channels.

The actor itself contains no protocol logic. All decisions live in `Swim`.

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
- `transport_tx` -- `mpsc::Sender<OutboundPacket>` -- sends UDP packets to the transport actor
- `scheduler_tx` -- `mpsc::Sender<TickerCommand<SwimTimer>>` -- sends timer set/cancel commands to the ticker

## Message Flow

`SwimCommand` has three variants:

| Variant | Source | Swim method called |
|---|---|---|
| `PacketReceived { src, packet }` | Transport layer (UDP) | `state.step(src, packet)` |
| `Timeout(SwimTimeOutCallback)` | Ticker (timer expiry) | `state.handle_timeout(tick_event)` |
| `Query(SwimQueryCommand)` | External callers (e.g., HTTP API) | `state.handle_query(command)` |

### PacketReceived

Handles inbound `SwimPacket` (Ping, Ack, PingReq). The `Swim` state machine processes gossip, updates membership, and buffers any response packets (e.g., Ack replies, forwarded proxy Acks).

### Timeout

Dispatched by the ticker when a timer expires. Drives the probe lifecycle:
- `ProtocolPeriodElapsed` -- start a new probe round (send Ping to next node)
- `DirectProbe` timed out -- escalate to indirect probing (send PingReq to K peers)
- `IndirectProbe` timed out -- mark target as Suspect
- `Suspect` timed out -- mark target as Dead
- `ProxyPing` timed out -- clean up stale proxy ping entry
- `JoinTry` timed out -- retry or give up on bootstrap join

### Query

Read-only queries (GetMembers, GetTopology) answered via oneshot reply channels. No state mutation.

## Outbound Side Effects

After every event, `flush_outbound_commands` drains two buffers from the `Swim` state machine and sends them concurrently via `tokio::join!`:

1. **Timer commands** (`TimerCommand<SwimTimer>`) -- sent to `scheduler_tx`
   - `SetSchedule { seq, timer }` -- register a new timer
   - `CancelSchedule { seq }` -- cancel an existing timer
2. **Outbound packets** (`OutboundPacket`) -- sent to `transport_tx`
   - Each packet has a `target: SocketAddr` and a `SwimPacket` payload

The flush also runs once at startup (before entering the event loop) to drain any commands produced during `Swim::new()` initialization.

## Key Dependencies

| Dependency | Role |
|---|---|
| `Swim` (swim.rs) | Pure state machine. All protocol logic. No async, no I/O. |
| `GossipBuffer` | Piggybacks membership updates onto outbound packets with O(log N) dissemination. |
| `LiveNodeTracker` | Round-robin selection of probe targets, excluding dead/suspect nodes. |
| `Topology` | Consistent hash ring updated on membership changes. |
| `Bootstrapper` / `JoinAttempt` (peer_discovery.rs) | Seed-node join with exponential backoff. |
| `SwimTimer` (messages.rs) | Timer definitions with tick counts for each probe phase. |

## Invariants

1. **Every event must be followed by `flush_outbound_commands`.** The `Swim` state machine buffers all side effects internally. If you skip the flush, packets and timer commands are silently lost. The `run()` loop enforces this -- there is no code path that processes an event without flushing.

2. **The actor is the sole driver of the `Swim` state machine.** No other code calls `step()`, `handle_timeout()`, or `handle_query()` on a `Swim` instance in production. This ensures single-threaded access without locks.

3. **`Swim` is fully synchronous.** It never does I/O, never awaits, never touches channels. This separation makes the protocol logic deterministically testable.

4. **Timer commands flow through the scheduler, not the actor.** The actor converts `TimerCommand` into `TickerCommand` (via `.into()`) and sends it to the ticker actor. The ticker fires `SwimTimeOutCallback` back into the actor's mailbox when timers expire.

5. **Sequence numbers (`seq`) correlate probes to responses.** Each probe gets a unique `seq`. Ack messages carry the same `seq` to cancel the corresponding timer. This is also how proxy pings are correlated.
