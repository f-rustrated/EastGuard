# Code Convention

## Encapsulation

Functions taking `&self`, `&mut self`, or a reference to a struct as their first argument must be methods on that struct. Free functions with `&SomeStruct` as first arg are never acceptable — add them as `impl SomeStruct` methods instead.

## Result over Option for observability

Prefer `Result<T, E>` over `Option<T>` when `None` represents a failure condition that an operator or developer would want to diagnose. Silent `None` returns hide *why* something didn't happen — use `Result` so the error is observable via logging or propagation.

Legitimate `Option` uses: simple lookups where "not found" is the only possible reason (e.g., `HashMap::get`, `Vec::first`).

## Propagate errors, don't swallow them

- Use `?` to propagate errors up the call chain. Don't match on `Result` just to convert `Err` to `None` or `Ok(())`.
- When `.ok()` is necessary at a boundary (e.g., best-effort forwarding), log the error at `debug!` or `warn!` level before discarding it.
- Never use `Result<Option<T>>` when the `None` case can be expressed as an error variant. The double-unwrap ceremony at call sites is a sign the API is wrong.

## Avoid boolean flags as function parameters

Don't pass `bool` parameters to control method behavior. Instead, have the caller check the condition and only call the method when appropriate. Boolean parameters hide branching from the call site and make APIs harder to read.

## Compile-time exhaustiveness over runtime defaults

Never use wildcard `_ => {}` or `_ => unreachable!()` in match arms for enum dispatch. List all variants explicitly so the compiler catches new variants at compile time. When splitting a match across sub-functions, the "already handled" arms should be listed explicitly (e.g., `VariantA { .. } | VariantB { .. } => {}`).

## Batch channel messages with Box\<[T]\>

Cross-actor channels should send `Box<[T]>` rather than individual `T` messages. The sender accumulates into a `Vec<T>` during its event loop iteration, then calls `.into_boxed_slice()` before sending — sheds unused capacity so only exact-size allocation crosses the channel. Benefits: fewer send operations (less channel contention), lower probability of a bounded channel being full, no wasted memory on the receiver side, and natural alignment with the flush-based actor pattern where side effects are drained after each event.

For bounded channels using `try_send`, batching is especially important — one send per batch instead of per item means fewer opportunities for a full-channel drop.

## Temporal coupling in error propagation

When a method performs mutation followed by a fallible read-only operation, consider that propagating the read-only error with `?` implies the entire operation failed — but the mutation already happened.

In such cases:
- Log the read-only failure instead of propagating it, if the mutation was the primary intent and succeeded.
- Or restructure so that the fallible read-only check runs *before* the mutation.
- Never let a post-mutation diagnostic failure abort the operation that already committed.
