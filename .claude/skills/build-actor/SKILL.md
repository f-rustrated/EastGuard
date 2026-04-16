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

Start with struct. No async, no channels, no I/O. Receives events via method calls, buffers all side effects into a single `pending_events: Vec<YourEvent>` for the actor to drain.

```rust
/// Unified side-effect type. Actor drains and routes by variant.
pub enum YourEvent {
    Packet(YourOutboundPacket),
    Timer(TimerCommand<YourTimer>),
}

pub struct YourStateMachine {
    // Your domain state
    // ...

    // Single output buffer -- actor drains after every event batch
    pending_events: Vec<YourEvent>,
}

impl YourStateMachine {
    /// Process an inbound event. Buffers side effects into pending_events.
    pub fn step(&mut self, event: YourInbound) {
        // Logic here. Push YourEvent variants.
        self.pending_events.push(YourEvent::Packet(/* ... */));
        self.pending_events.push(YourEvent::Timer(/* ... */));
    }

    /// Process a timer expiry.
    pub fn handle_timeout(&mut self, callback: YourTimeoutCallback) {
        // ...
    }

    /// Drain all buffered events. Returns ownership to the caller.
    pub(crate) fn take_events(&mut self) -> Vec<YourEvent> {
        std::mem::take(&mut self.pending_events)
    }

    /// Top-level command dispatch. Called by the actor's drain loop.
    /// Lives on the state machine because it's pure sync — no I/O.
    pub fn process(&mut self, event: YourActorCommand) {
        match event {
            YourActorCommand::PacketReceived { src, data } => self.step(/* ... */),
            YourActorCommand::Timeout(callback) => self.handle_timeout(callback),
            YourActorCommand::Query(cmd) => self.handle_query(cmd),
        }
    }
}
```

**Key constraints:**
- No `async fn`. No `.await`. No `mpsc::Sender`. No `tokio::` anything.
- All side effects go into `pending_events` as enum variants.
- Single buffer, single drain method. No separate `take_outbound()` / `take_timer_commands()`.
- State machine doesn't know who consumes its output.

**Reference implementations:**
- `Swim` in `src/clusters/swims/swim.rs` — emits `SwimEvent { Packet, Timer, Membership }`
- `Raft` in `src/clusters/raft/state.rs` — emits `RaftEvent { OutboundRaftPacket, Timer, LeaderChange }`


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

### Event enum (what state machine produces)

```rust
pub enum YourEvent {
    Packet(YourOutboundPacket),
    Timer(TimerCommand<YourTimer>),
    // Add more variants as the component grows
}
```

One variant per output channel the actor routes to. Actor matches on variants and sends to the appropriate channel.

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
3. Drains event buffer
4. Routes each event variant to the appropriate channel

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

        // Drain-then-flush: recv_many batches up to `limit` messages,
        // then flush once. Under load N messages = 1 flush instead of N.
        let mut buf = Vec::with_capacity(64);
        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for event in buf.drain(..) {
                state.process(event);
            }
            Self::flush(&mut state, &transport_tx, &scheduler_tx).await;
        }
    }

    async fn flush(
        state: &mut YourStateMachine,
        transport_tx: &mpsc::Sender<YourOutboundPacket>,
        scheduler_tx: &mpsc::Sender<TickerCommand<YourTimer>>,
    ) {
        for event in state.take_events() {
            match event {
                YourEvent::Packet(pkt) => {
                    let _ = transport_tx.send(pkt).await;
                }
                YourEvent::Timer(cmd) => {
                    let _ = scheduler_tx.send(cmd.into()).await;
                }
            }
        }
    }
}
```

**Flush routes directly** — no intermediate Vecs, no `tokio::join!`. Iterate events, match variant, send to channel. mpsc sends are non-blocking unless buffer full, so sequential dispatch is equivalent to concurrent.

**Critical: flush must happen after draining all pending events.** Output buffers accumulate across multiple `process()` calls, so one flush handles the entire batch. `process()` lives on the state machine when dispatch is pure sync (no I/O). If command handling requires async I/O (e.g. sending transport commands), keep it as an actor associated fn instead. Branch that skips flush = side effects silently lost.

**Translation in flush**: If events need transformation before routing (e.g. membership events → raft commands, timer seq namespacing), extract a helper method rather than inlining complex logic in the match arm. See `SwimActor::to_raft_command()` and `RaftGroups::translate_timer_seq()`.

### Multiplexing pattern (like RaftActor)

If actor manages multiple state machine instances (e.g., one per shard), extract a `Groups` struct to encapsulate group lifecycle, dirty tracking, and timer seq namespacing. Actor stays thin — just the mailbox loop.

```rust
/// Encapsulates multiplexed state machines and their bookkeeping.
struct YourGroups {
    node_id: NodeId,
    groups: HashMap<GroupId, YourStateMachine>,
    seq_counter: u32,
    shard_tokens: HashMap<ShardToken, u32>,
    dirty: HashSet<GroupId>,
}

impl YourGroups {
    async fn process_command(&mut self, cmd: YourActorCommand, ...) {
        // match cmd, mutate groups, insert into self.dirty
    }

    async fn flush_dirty(&mut self, transport_tx: ..., scheduler_tx: ...) {
        let to_flush = std::mem::take(&mut self.dirty);
        for group_id in to_flush {
            let Some(sm) = self.groups.get_mut(&group_id) else { continue };
            for event in sm.take_events() {
                match event {
                    YourEvent::Packet(pkt) => { /* aggregate or send */ }
                    YourEvent::Timer(cmd) => {
                        if let Some(cmd) = self.translate_timer_seq(group_id, cmd) {
                            let _ = scheduler_tx.send(cmd.into()).await;
                        }
                    }
                }
            }
        }
    }
}
```

See `RaftGroups` in `src/clusters/raft/actor.rs` for the full implementation. Key details:

- `process_command` is async when some commands need I/O (e.g. `DisconnectPeer`, `CancelSchedule`). Channel senders passed as params, not stored in the struct.
- Outbound packets aggregated by target NodeId before sending — batches N shard groups into fewer channel sends.


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

Sync tests, no tokio needed. Use helper functions to filter events by variant:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Extract only Packet events from take_events().
    fn packets(sm: &mut YourStateMachine) -> Vec<YourOutboundPacket> {
        sm.take_events()
            .into_iter()
            .filter_map(|e| match e {
                YourEvent::Packet(p) => Some(p),
                _ => None,
            })
            .collect()
    }

    /// Drain all events (discard).
    fn drain(sm: &mut YourStateMachine) {
        sm.take_events();
    }

    #[test]
    fn basic_behavior() {
        let mut sm = YourStateMachine::new(/* ... */);

        sm.step(some_event);

        let pkts = packets(&mut sm);
        assert_eq!(pkts.len(), 1);
        // assert on contents...
    }
}
```

Biggest advantage of sync-first design: test protocol logic without async machinery.

## Checklist

- [ ] State machine is `pub struct` with no async, no channels, no I/O
- [ ] Side effects buffered in single `pending_events: Vec<YourEvent>`
- [ ] `YourEvent` enum has one variant per output channel
- [ ] Single `take_events()` drain via `std::mem::take`
- [ ] Actor flush iterates events and routes by variant — no intermediate Vecs
- [ ] Actor drains pending messages via `recv_many` then flushes once (no early returns before flush)
- [ ] TTimer implemented if using timers (with `Default` callback)
- [ ] Channels created and actor spawned in `lib.rs` startup
- [ ] Sync unit tests with helper functions filtering events by variant
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes
