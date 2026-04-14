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

Uses a strict pattern: **pure sync state machine + async actor wrapper**. The state
machine holds all logic and is trivially testable. The actor handles channels, timers, and I/O.
Every existing component (Swim, Raft, Ticker) follows this, and new components should too.

## Why This Pattern Matters

The sync/async split isn't cosmetic. It gives you:
- **Deterministic testing** -- call methods, check return values, no tokio runtime needed
- **No accidental I/O** -- the state machine can't send a packet or touch the network
- **Clean lifecycle** -- the actor owns the async boundary; kill the task, everything stops
- **Simulation** -- turmoil can control the network without touching protocol logic

## Step 1: Design the State Machine First

Start with a struct that has no async, no channels, no I/O. It receives events via method calls
and buffers its side effects for the actor to drain.

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
- All side effects go into the `pending_*` vecs.
- The state machine doesn't know who will consume its output.


## Step 2: Define Message Types

Create a `messages.rs` (or add to an existing one) with:

### The actor command enum (what the actor receives)

```rust
pub enum YourActorCommand {
    PacketReceived { src: SocketAddr, data: YourPacket },
    Timeout(YourTimeoutCallback),
    Query(YourQueryCommand),
}
```

Follow the convention: `PacketReceived` for network input, `Timeout` for timer expiry,
`Query` for read-only questions answered via oneshot.

### The outbound packet (what the state machine produces)

```rust
pub struct YourOutboundPacket {
    pub target: SocketAddr,  // or NodeId if transport resolves addresses
    pub payload: YourPayload,
}
```

### Query commands (if your component answers external questions)

```rust
pub enum YourQueryCommand {
    GetSomething { reply: oneshot::Sender<SomeResult> },
}
```

## Step 3: Implement TTimer (If You Need Timers)

If your component uses timers, implement the `TTimer` trait from `src/schedulers/timer.rs`.
This lets you reuse the existing ticker infrastructure.

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

The `Default` callback on `Callback` is important -- it's what the ticker emits every protocol
period (10 ticks = 1 second) regardless of any registered timers. If your component doesn't
need periodic events, you can make the default variant a no-op that the actor ignores.

**If you share the ticker with existing actors**, your timer callbacks arrive through the same
channel. You'll need to make the actor command type accept `From<YourTimeoutCallback>`, and the
scheduling actor's sender must be compatible.

**If your component needs its own ticker**, spawn a separate `run_scheduling_actor` instance
with its own mailbox.

## Step 4: Build the Actor

The actor is an async wrapper that:
1. Receives events from a mailbox
2. Feeds them into the sync state machine
3. Drains output buffers (the "flush")
4. Sends results to other actors

```rust
pub struct YourActor {
    mailbox: mpsc::Receiver<YourActorCommand>,
    transport_tx: mpsc::Sender<YourOutboundPacket>,
    scheduler_tx: mpsc::Sender<TickerCommand<YourTimer>>,
}

impl YourActor {
    pub fn new(
        mailbox: mpsc::Receiver<YourActorCommand>,
        transport_tx: mpsc::Sender<YourOutboundPacket>,
        scheduler_tx: mpsc::Sender<TickerCommand<YourTimer>>,
    ) -> Self {
        Self { mailbox, transport_tx, scheduler_tx }
    }

    pub async fn run(mut self, mut state: YourStateMachine) {
        // Flush any side effects from initialization
        self.flush(&mut state).await;

        while let Some(event) = self.mailbox.recv().await {
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
            self.flush(&mut state).await;
        }
    }

    async fn flush(&mut self, state: &mut YourStateMachine) {
        let timer_commands = state.take_timer_commands();
        let outbound_packets = state.take_outbound();

        // Send concurrently -- neither depends on the other
        tokio::join!(
            async {
                for cmd in timer_commands {
                    let _ = self.scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    let _ = self.transport_tx.send(pkt).await;
                }
            }
        );
    }
}
```

**Critical: the flush must happen after EVERY event.** The existing actors enforce this by
having a single flush call at the bottom of the match, with no early returns or continues
before it. If you add a branch that skips the flush, side effects are silently lost.

### Multiplexing pattern (like RaftActor)

If your actor manages multiple state machine instances (e.g., one per shard):

```rust
pub struct MultiplexActor {
    groups: HashMap<GroupId, YourStateMachine>,
    // ... channels ...
    seq_counter: u32,
    shard_tokens: HashMap<ShardToken, u32>,
}
```

Each instance emits local seq values for timers. The actor must namespace them to avoid
collisions in the shared ticker. See `RaftActor.flush()` in `src/raft/actor.rs` for the
translation pattern: `get_or_alloc_seq(shard_group_id.token(local_seq))` maps each
`(group_id, local_seq)` pair to a unique global seq.

When removing a group, cancel all its timers by iterating over known local seqs and
removing their global mappings.

## Step 5: Wire Into Startup (lib.rs)

Add channel creation and actor spawning in `StartUp::run()`:

```rust
pub async fn run(self) -> Result<()> {
    // ... existing channels ...

    // Your channels
    let (your_sender, your_mailbox) = mpsc::channel(100);
    let (your_outbound_tx, your_outbound_rx) = mpsc::channel(100);

    // Your actor
    let your_actor = YourActor::new(your_mailbox, your_outbound_tx, ticker_cmd_tx.clone());

    // Your state machine initialization
    let your_state = YourStateMachine::new(/* ... */);

    // Spawn (after ticker, before client listener)
    tokio::spawn(your_actor.run(your_state));

    // If you have a transport, spawn it too
    // tokio::spawn(your_transport.run());

    // ... existing client listener ...
}
```

**Spawn order matters:**
1. Ticker first (other actors send timer commands to it immediately)
2. Transport actors (they start listening for network events)
3. Protocol actors (they flush initial state on startup, producing timer commands)
4. Client listener last

## Step 6: Test the State Machine

Write sync tests that don't need tokio:

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

This is the biggest advantage of the sync-first design: you test protocol logic without
any async machinery.

## Checklist

- [ ] State machine is `pub struct` with no async, no channels, no I/O
- [ ] Side effects buffered in `pending_outbound` and `pending_timer_commands`
- [ ] `take_outbound()` and `take_timer_commands()` drain via `std::mem::take`
- [ ] Actor calls flush after every event (no early returns before flush)
- [ ] TTimer implemented if using timers (with `Default` callback)
- [ ] Channels created and actor spawned in `lib.rs` startup
- [ ] Sync unit tests exercise the state machine directly
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes
