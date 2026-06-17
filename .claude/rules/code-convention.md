# Code Convention

## Encapsulation

Functions taking `&self`, `&mut self`, or a reference to a struct as their first argument must be methods on that struct. Free functions with `&SomeStruct` as first arg are never acceptable — add them as `impl SomeStruct` methods instead.

## Enum struct pattern

Enum variants that carry data must use the tuple-variant + named-struct pattern. Never use inline fields on enum variants. See `/.claude/skills/enum-struct-pattern/SKILL.md` for the full pattern including `impl_from_variant!` usage.

## Result over Option for observability

Prefer `Result<T, E>` over `Option<T>` when `None` represents a failure condition that an operator or developer would want to diagnose. Silent `None` returns hide *why* something didn't happen — use `Result` so the error is observable via logging or propagation.

Legitimate `Option` uses: simple lookups where "not found" is the only possible reason (e.g., `HashMap::get`, `Vec::first`).

## Propagate errors, don't swallow them

- Use `?` to propagate errors up the call chain. Don't match on `Result` just to convert `Err` to `None` or `Ok(())`.
- When `.ok()` is necessary at a boundary (e.g., best-effort forwarding), log the error at `debug!` or `warn!` level before discarding it.
- Never use `Result<Option<T>>` when the `None` case can be expressed as an error variant. The double-unwrap ceremony at call sites is a sign the API is wrong.

## Avoid boolean flags as function parameters

Don't pass `bool` parameters to control method behavior. Instead, have the caller check the condition and only call the method when appropriate. Boolean parameters hide branching from the call site and make APIs harder to read.

## `Box<[T]>` over `Vec<T>`
* Use `Box<[T]>` when the collection has been built and its size will never change again.
* Use `Vec<T>` when it is mutable state.

## Compile-time exhaustiveness over runtime defaults

Never use wildcard `_ => {}` or `_ => unreachable!()` in match arms for enum dispatch. List all variants explicitly so the compiler catches new variants at compile time. When splitting a match across sub-functions, the "already handled" arms should be listed explicitly (e.g., `VariantA { .. } | VariantB { .. } => {}`).

## Batch channel messages with Box\<[T]\>

Cross-actor channels should send `Box<[T]>` rather than individual `T` messages. The sender accumulates into a `Vec<T>` during its event loop iteration, then calls `.into_boxed_slice()` before sending — sheds unused capacity so only exact-size allocation crosses the channel. Benefits: fewer send operations (less channel contention), lower probability of a bounded channel being full, no wasted memory on the receiver side, and natural alignment with the flush-based actor pattern where side effects are drained after each event.

For bounded channels using `try_send`, batching is especially important — one send per batch instead of per item means fewer opportunities for a full-channel drop.

## Place methods on the struct that owns the data

When adding a method, put it on the struct whose fields it reads or computes over — not on a parent that happens to hold a reference. If `MetadataStateMachine::topic_stats()` just iterates `self.topics` and calls per-topic logic, that per-topic logic belongs on `TopicMeta`, not inlined in the iterator. This keeps methods close to the fields they touch, makes them independently testable, and prevents parent structs from accumulating pass-through logic.

## Temporal coupling in error propagation

When a method performs mutation followed by a fallible read-only operation, consider that propagating the read-only error with `?` implies the entire operation failed — but the mutation already happened.

In such cases:
- Log the read-only failure instead of propagating it, if the mutation was the primary intent and succeeded.
- Or restructure so that the fallible read-only check runs *before* the mutation.
- Never let a post-mutation diagnostic failure abort the operation that already committed.

## Prefer a declarative model over delta changes

When a coordinator/leader drives a change across other components, publish the
**desired end state** and let each receiver idempotently reconcile its local
reality against it — don't compute an imperative diff (what changed, who-does-what)
and dispatch tailored per-receiver instructions.

Why: the declarative form is idempotent by construction (re-publishing the same
desired state is a no-op for already-converged receivers), so it self-heals across
re-drives and lost messages; and it puts decisions (e.g. which peer to copy from)
on the receiver, which has the freshest local context and can retry. Imperative
diffs re-derive relationships already known at construction, are fragile to
recompute, and can't recover from a dropped message.

When you must still reject stale, duplicate, or out-of-order application, reach for
**versioning** before bespoke diff logic: tag the state with a monotonic version and
accept an operation only if its version is strictly newer. With a single writer (the
Raft leader, the segment leader), a lower-version operation is *naturally* a no-op —
the same mechanism the topology shard-leader map (term-monotonic, `topology.md` #6)
and segment seals already rely on.
