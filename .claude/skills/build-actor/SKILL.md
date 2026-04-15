---
name: build-actor
description: >
  How to build a new actor following EastGuard's architecture pattern. Use this skill whenever
  creating a new component, service, or subsystem that needs to process events asynchronously.
  Also use when the task involves designing a new state machine, wiring up channels between
  components, integrating with the timer system, or adding a new actor to the startup sequence.
  Even if the user just says "add a new component for X" or "we need a service that does Y",
  this skill has the full construction workflow. Covers the sync-first design, actor boundary,
  flush protocol, ticker integration, and startup wiring.
---

# Building an Actor

Strict pattern: **pure sync state machine + async actor wrapper**. State machine holds all logic, trivially testable. Actor handles channels, timers, I/O. Every existing component (Swim, Raft, Ticker) follows this. New components must too.

## Why This Pattern Matters

Sync/async split gives:
- **Deterministic testing** -- call methods, check return values, no tokio runtime needed
- **No accidental I/O** -- state machine can't send packet or touch network
- **Clean lifecycle** -- actor owns async boundary; kill task, everything stops
- **Simulation** -- turmoil controls network without touching protocol logic

## Step 1: Design the State Machine First

Start with struct. No async, no channels, no I/O. Receives events via method calls, buffers side effects for actor to drain.

```rust
pub struct YourStateMachine {
    // Your domain state
    // ...

    // Output buffers -- the actor drains these after every event
    pending_outbound: Vec<YourOutboundPacket>,
    pending_timer_commands: Vec<TimerCommand<YourTimer>>,
}

impl YourStateMachine {
    /// Process an inbound event. Buffers any side effects.
    pub fn step(&mut self, event: YourEvent) {
        // Logic here. Push to pending_outbound / pending_timer_commands.
    }

    /// Process a timer expiry.
    pub fn handle_timeout(&mut self, callback: YourTimeoutCallback) {
        // ...
    }

    /// Drain outbound packets. Returns ownership to the caller.
    pub fn take_outbound(&mut self) -> Vec<YourOutboundPacket> {
        std::mem::take(&mut self.pending_outbound)
    }

    /// Drain timer commands.
    pub fn take_timer_commands(&mut self) -> Vec<TimerCommand<YourTimer>> {
        std::mem::take(&mut self.pending_timer_commands)
    }
}
```

**Key constraints:**
- No `async fn`. No `.await`. No `mpsc::Sender`. No `tokio::` anything.
- All side effects go into `pending_*` vecs.
- State machine doesn't know who consumes its output.


## Step 2: Define Message Types

Create `messages.rs` (or add to existing one):

### Actor command enum (what actor receives)

```rust
pub enum YourActorCommand {
    PacketReceived { src: SocketAddr, data: YourPacket },
    Timeout(YourTimeoutCallback),
    Query(YourQueryCommand),
}
```

Convention: `PacketReceived` for network input, `Timeout` for timer expiry, `Query` for read-only questions answered via oneshot.

### Outbound packet (what state machine produces)

```rust
pub struct YourOutboundPacket {
    pub target: SocketAddr,  // or NodeId if transport resolves addresses
    pub payload: YourPayload,
}
```

### Query commands (if component answers external questions)

```rust
pub enum YourQueryCommand {
    GetSomething { reply: oneshot::Sender<SomeResult> },
}
```

## Step 3: Implement TTimer (If You Need Timers)

If component uses timers, implement `TTimer` trait from `src/schedulers/timer.rs`. Reuses existing ticker infrastructure.

```rust
use crate::schedulers::timer::TTimer;

pub struct YourTimer {
    kind: YourTimerKind,
    ticks_remaining: u32,
}

#[derive(Default)]
pub enum YourTimeoutCallback {
    #[default]
    ProtocolPeriodElapsed,  // The Default variant fires every PROBE_INTERVAL_TICKS (10 ticks)
    YourSpecificTimeout { /* ... */ },
}

impl TTimer for YourTimer {
    type Callback = YourTimeoutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, seq: u32) -> YourTimeoutCallback {
        // Convert the expired timer into a callback
        YourTimeoutCallback::YourSpecificTimeout { /* ... */ }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None // or Some(id) if applicable
    }
}
```

`Default` callback on `Callback` matters -- ticker emits it every protocol period (10 ticks = 1 second) regardless of registered timers. If component doesn't need periodic events, make default variant no-op that actor ignores.

**Sharing ticker with existing actors**: timer callbacks arrive through same channel. Actor command type needs `From<YourTimeoutCallback>`, scheduling actor's sender must be compatible.

**Own ticker needed**: spawn separate `run_scheduling_actor` instance with own mailbox.

## Step 4: Build the Actor

Actor = async wrapper that:
1. Receives events from mailbox
2. Feeds into sync state machine
3. Drains output buffers (flush)
4. Sends results to other actors

No struct needed for simple actors — pass all channels directly to `run()`. Actor is a unit-like struct used only as a namespace for the `run` and `flush` associated functions.

```rust
pub struct YourActor;

impl YourActor {
    pub async fn run(
        mut mailbox: mpsc::Receiver<YourActorCommand>,
        mut state: YourStateMachine,
        transport_tx: mpsc::Sender<YourOutboundPacket>,
        scheduler_tx: mpsc::Sender<TickerCommand<YourTimer>>,
    ) {
        // Flush any side effects from initialization
        Self::flush(&mut state, &transport_tx, &scheduler_tx).await;

        while let Some(event) = mailbox.recv().await {
            match event {
                YourActorCommand::PacketReceived { src, data } => {
                    state.step(/* ... */);
                }
                YourActorCommand::Timeout(callback) => {
                    state.handle_timeout(callback);
                }
                YourActorCommand::Query(cmd) => {
                    state.handle_query(cmd);
                }
            }

            // THIS MUST HAPPEN AFTER EVERY EVENT -- no exceptions
            Self::flush(&mut state, &transport_tx, &scheduler_tx).await;
        }
    }

    async fn flush(
        state: &mut YourStateMachine,
        transport_tx: &mpsc::Sender<YourOutboundPacket>,
        scheduler_tx: &mpsc::Sender<TickerCommand<YourTimer>>,
    ) {
        let timer_commands = state.take_timer_commands();
        let outbound_packets = state.take_outbound();

        // Send concurrently -- neither depends on the other
        tokio::join!(
            async {
                for cmd in timer_commands {
                    let _ = scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    let _ = transport_tx.send(pkt).await;
                }
            }
        );
    }
}
```

**Critical: flush must happen after EVERY event.** Existing actors enforce this with single flush call at bottom of match, no early returns or continues before it. Branch that skips flush = side effects silently lost.

### Multiplexing pattern (like RaftActor)

If actor manages multiple state machine instances (e.g., one per shard), same unit-like struct pattern. Domain state declared as local variables inside `run()`:

```rust
pub struct MultiplexActor;

impl MultiplexActor {
    pub async fn run(
        node_id: NodeId,
        mut mailbox: mpsc::Receiver<YourActorCommand>,
        transport_tx: mpsc::Sender<YourOutboundPacket>,
        scheduler_tx: mpsc::Sender<TickerCommand<YourTimer>>,
    ) {
        let mut groups: HashMap<GroupId, YourStateMachine> = HashMap::new();
        let mut seq_counter: u32 = 0;
        let mut shard_tokens: HashMap<ShardToken, u32> = HashMap::new();

        while let Some(cmd) = mailbox.recv().await {
            // match cmd, mutate groups, call Self::flush(...)
        }
    }
}
```

Each instance emits local seq values for timers. Actor must namespace them to avoid collisions in shared ticker. See `RaftActor.flush()` in `src/raft/actor.rs` for translation pattern: `get_or_alloc_seq(shard_group_id.token(local_seq))` maps each `(group_id, local_seq)` pair to unique global seq.

When removing group, cancel all its timers by iterating over known local seqs and removing their global mappings.

## Step 5: Wire Into Startup (lib.rs)

Add channel creation and actor spawning in `StartUp::run()`:

```rust
pub async fn run(self) -> Result<()> {
    // ... existing channels ...

    // Your channels
    let (your_sender, your_mailbox) = mpsc::channel(100);
    let (your_outbound_tx, your_outbound_rx) = mpsc::channel(100);

    // Your state machine initialization
    let your_state = YourStateMachine::new(/* ... */);

    // Spawn (after ticker, before client listener)
    tokio::spawn(YourActor::run(your_mailbox, your_state, your_outbound_tx, ticker_cmd_tx.clone()));

    // If you have a transport, spawn it too
    // tokio::spawn(your_transport.run());

    // ... existing client listener ...
}
```

**Spawn order matters:**
1. Ticker first (other actors send timer commands immediately)
2. Transport actors (start listening for network events)
3. Protocol actors (flush initial state on startup, producing timer commands)
4. Client listener last

## Step 6: Test the State Machine

Sync tests, no tokio needed:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_behavior() {
        let mut sm = YourStateMachine::new(/* ... */);

        sm.step(some_event);

        let packets = sm.take_outbound();
        assert_eq!(packets.len(), 1);
        // assert on contents...

        let timers = sm.take_timer_commands();
        assert_eq!(timers.len(), 1);
        // assert on timer type, tick count...
    }
}
```

Biggest advantage of sync-first design: test protocol logic without async machinery.

## Checklist

- [ ] State machine is `pub struct` with no async, no channels, no I/O
- [ ] Side effects buffered in `pending_outbound` and `pending_timer_commands`
- [ ] `take_outbound()` and `take_timer_commands()` drain via `std::mem::take`
- [ ] Actor calls flush after every event (no early returns before flush)
- [ ] TTimer implemented if using timers (with `Default` callback)
- [ ] Channels created and actor spawned in `lib.rs` startup
- [ ] Sync unit tests exercise state machine directly
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes