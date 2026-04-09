# Scheduling Actor

## Purpose

`run_scheduling_actor` is a generic, tick-based timer actor that drives a `Ticker<T>` state machine at fixed real-time intervals. It owns no protocol logic -- it is a pure timer service that fires callbacks when timers expire and emits periodic "protocol period elapsed" events.

## Architecture

```
run_scheduling_actor<T: TTimer>(
    sender:  mpsc::Sender<impl From<T::Callback>>,  // outbound callback channel
    mailbox: mpsc::Receiver<TickerCommand<T>>,       // inbound command channel
)
```

The function is generic over any `TTimer` implementation. The current concrete type is `SwimTimer`, but the actor is decoupled from SWIM specifics. It creates a `tokio::time::interval` at `TICK_PERIOD_MS` and a fresh `Ticker<T>`, then enters an infinite `tokio::select!` loop.

## Tick Model
Every real tick (100 ms), the `Ticker` decrements all active timers and advances a protocol-period counter. When the counter reaches `PROBE_INTERVAL_TICKS` it resets to zero and emits a `Default::default()` callback (the "protocol period elapsed" event).

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

**On tick:** calls `ticker.advance_clock()`, which decrements every in-flight timer, collects expired-timer callbacks, and optionally emits the protocol-period event. Each callback is sent through `sender`.

**On mailbox:** receives a `TickerCommand<T>` and either applies a `TimerCommand` (`SetSchedule` / `CancelSchedule`) or, in test builds only, handles `ForceTick`.

## Timer Lifecycle

1. **SetSchedule { seq, timer }** -- inserts a timer into the `Ticker`'s `HashMap<u32, T>` keyed by sequence number.
2. **Each tick** -- `timer.tick()` is called, decrementing the timer's internal counter and returning the remaining ticks.
3. **Expiry (remaining == 0)** -- `to_timeout_callback(seq)` is called, the callback is collected, and the timer is removed from the map.
4. **CancelSchedule { seq }** -- removes the timer early, preventing any future callback.

## Testing

`TickerCommand::ForceTick` (gated behind `#[cfg(test)]`) lets tests advance the clock by one tick deterministically, without waiting for real time. It calls the same `advance_clock()` path as the interval branch.

## Generics

The actor is parameterized over `TTimer`, a trait requiring:

- `tick(&mut self) -> u32` -- decrement and return remaining ticks.
- `to_timeout_callback(self, id: u32) -> Self::Callback` -- produce a callback on expiry.
- `Callback: Default` -- the default value serves as the protocol-period-elapsed event.

Any type satisfying `TTimer` can be plugged in. `SwimTimer` is the only current implementation.

## Invariants

- **Biased select** -- the interval branch is always checked before the mailbox branch. This guarantees ticks are never starved by a burst of commands.
- **Pure timer** -- the actor contains zero protocol logic. All domain behavior lives in the `TTimer` implementation and the consumer of the callback channel.
- **Infallible loop** -- send failures on the callback channel are silently ignored (`let _ = sender.send(...).await`), so a dropped receiver does not crash the actor.
