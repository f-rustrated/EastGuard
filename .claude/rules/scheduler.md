# Scheduling Actor

## Purpose

`run_scheduling_actor` — generic tick-based timer actor driving `Ticker<T>` state machine at fixed real-time intervals. Owns no protocol logic — pure timer service firing callbacks on timer expiry and emitting periodic "protocol period elapsed" events.

## Architecture

```
run_scheduling_actor<T: TTimer>(
    sender:  mpsc::Sender<impl From<T::Callback>>,  // outbound callback channel
    mailbox: mpsc::Receiver<TickerCommand<T>>,       // inbound command channel
)
```

Generic over any `TTimer` implementation. Current concrete type: `SwimTimer`, but actor decoupled from SWIM specifics. Creates `tokio::time::interval` at `TICK_PERIOD_MS` and fresh `Ticker<T>`, enters infinite `tokio::select!` loop.

## Tick Model
Every real tick (100 ms), `Ticker` decrements all active timers and advances protocol-period counter. When counter reaches `PROBE_INTERVAL_TICKS` — resets to zero, emits `Default::default()` callback ("protocol period elapsed" event).

## Event Loop

```
loop {
    tokio::select! {
        biased;                          // tick branch always checked first
        _ = interval.tick() => { ... }   // 1. advance clock, fire callbacks
        Some(cmd) = mailbox.recv() => {  // 2. apply timer commands
            ...
        }
    }
}
```

**On tick:** calls `ticker.advance_clock()` — decrements every in-flight timer, collects expired-timer callbacks, optionally emits protocol-period event. Each callback sent through `sender`.

**On mailbox:** receives `TickerCommand<T>`, either applies `TimerCommand` (`SetSchedule` / `CancelSchedule`) or, in test builds only, handles `ForceTick`.

## Timer Lifecycle

1. **SetSchedule { seq, timer }** — inserts timer into `Ticker`'s `HashMap<u32, T>` keyed by sequence number.
2. **Each tick** — `timer.tick()` called, decrementing timer's internal counter, returning remaining ticks.
3. **Expiry (remaining == 0)** — `to_timeout_callback(seq)` called, callback collected, timer removed from map.
4. **CancelSchedule { seq }** — removes timer early, preventing future callback.

## Testing

`TickerCommand::ForceTick` (gated behind `#[cfg(test)]`) lets tests advance clock by one tick deterministically without waiting for real time. Calls same `advance_clock()` path as interval branch.

## Generics

Actor parameterized over `TTimer`, trait requiring:

- `tick(&mut self) -> u32` — decrement and return remaining ticks.
- `to_timeout_callback(self, id: u32) -> Self::Callback` — produce callback on expiry.
- `Callback: Default` — default value serves as protocol-period-elapsed event.

Any type satisfying `TTimer` can plug in. `SwimTimer` only current implementation.

## Invariants

- **Biased select** — interval branch always checked before mailbox branch. Guarantees ticks never starved by command burst.
- **Pure timer** — actor contains zero protocol logic. All domain behavior lives in `TTimer` implementation and callback channel consumer.
- **Infallible loop** — send failures on callback channel silently ignored (`let _ = sender.send(...).await`), dropped receiver not crash actor.