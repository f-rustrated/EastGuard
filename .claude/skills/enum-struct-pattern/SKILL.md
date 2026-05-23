---
name: enum-struct-pattern
description: How to define Rust enum command/event types using the tuple-variant + named-struct pattern with `impl_from_variant!`. Use this skill whenever defining new enums for commands, events, messages, or any dispatch-style enum where variants carry structured data. Also use when refactoring existing inline-field enum variants into this pattern.
---

# Enum Struct Pattern

This codebase uses a consistent pattern for defining command and event enums: each variant wraps a named struct, and `impl_from_variant!` generates `From` impls so callers can use `.into()` for ergonomic construction.

## Why this pattern

Inline-field enum variants (`Variant { field1, field2 }`) force destructuring at every dispatch and construction site. As variants grow, this creates noisy boilerplate. Named structs give each variant a type identity — handlers accept the struct directly, construction uses `.into()`, and the enum becomes a thin routing layer.

## Structure

```rust
// 1. Define structs for each variant
pub struct CreateTopic {
    pub name: String,
    pub replica_set: Vec<NodeId>,
}

pub struct DeleteTopic {
    pub name: String,
}

// 2. Define the enum with tuple variants wrapping the structs
pub enum MetadataCommand {
    CreateTopic(CreateTopic),
    DeleteTopic(DeleteTopic),
}

// 3. Generate From impls with the macro
impl_from_variant!(
    MetadataCommand,
    CreateTopic,
    DeleteTopic,
);
```

The macro lives in `src/macros/mod.rs` and expands to:
```rust
impl From<CreateTopic> for MetadataCommand {
    fn from(val: CreateTopic) -> Self {
        MetadataCommand::CreateTopic(val)
    }
}
// ... for each variant
```

Import the macro with `use crate::impl_from_variant;`.

## Construction

Callers construct variants using `.into()` — no need to name the enum:

```rust
// Direct — when the target type is known from context
let cmd: MetadataCommand = CreateTopic { name, replica_set }.into();

// Via a helper that accepts `impl Into<EnumType>`
fn send(message: impl Into<MetadataCommand>) { ... }
send(CreateTopic { name, replica_set }); // .into() happens implicitly
```

For raising events internally, define a helper method:
```rust
fn raise_event(&mut self, event: impl Into<DataPlaneEvent>) {
    self.pending_events.push(event.into());
}

// Usage — no .into() needed at call site
self.raise_event(event::ReplicationTimedOut {
    segment_key,
    committed_end_offset,
});
```

## Dispatch

Handlers accept the struct directly. The dispatch match becomes one-liners:

```rust
fn process(&mut self, cmd: MetadataCommand) {
    use MetadataCommand as C;
    match cmd {
        C::CreateTopic(cmd) => self.handle_create_topic(cmd),
        C::DeleteTopic(cmd) => self.handle_delete_topic(cmd),
    }
}

fn handle_create_topic(&mut self, cmd: CreateTopic) {
    // Access fields via cmd.name, cmd.replica_set
}
```

For match arms with inline logic (no dedicated handler), use `evt.field` access:
```rust
DataPlaneEvent::ReplicaAckReceived(evt) => {
    tracker.process_ack(evt.segment_key, evt.from);
}
```

## When to use inline fields instead

Not every variant needs a struct. Keep inline fields for:
- Single-field wrappers: `Timer(TimerCommand<T>)`, `SubmitCheckpoint(CheckpointJob)`
- Variants that are simple forwarding without a dedicated handler

The pattern is for variants with 2+ fields that have dedicated handling logic.

## Existing examples

- `DataPlaneInterNodeCommand` in `src/data_plane/messages/command.rs` — wire format commands
- `DataPlaneEvent` in `src/data_plane/messages/event.rs` — internal state machine events
- `DataTransportCommand::send()` in `src/data_plane/transport/command.rs` — `impl Into` helper
