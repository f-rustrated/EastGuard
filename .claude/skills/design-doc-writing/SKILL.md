---
name: design-doc-writing
description: >-
  Write or edit EastGuard design and architecture documentation — roadmaps, phase
  design docs (like D1–D6 in docs/), mental-model explainers, subsystem
  walkthroughs — in the project's concept-focused house style. Use this skill
  whenever creating, updating, or reviewing any design / architecture / conceptual
  doc for this project, even when the user just says "write the D5 doc", "document
  this subsystem", "update the roadmap", "explain how X works in a doc", or "make a
  diagram doc for Y". The style: describe concepts and the *why*, grounded in the
  real implementation, without leaking exact code identifiers (struct / enum /
  method / field / variable names) unless the name is shared domain vocabulary that
  aids understanding. This does NOT apply to .claude/rules/ invariant specs, which
  intentionally name exact types and methods because they are code contracts.
---

# Writing EastGuard Design Docs

EastGuard's design docs (the `docs/` roadmaps and phase docs, mental-model
explainers, subsystem walkthroughs) exist to make a reader *understand the system*
— the shapes, the flows, the tradeoffs, and above all the **why**. They are read
by people onboarding, by future contributors deciding how to extend a subsystem,
and by you in a later session reconstructing intent. They long-outlive any
particular function name.

That goal drives every choice below.

## Scope: which docs this applies to

Apply this style to **design / architecture / conceptual docs** — anything whose
job is to explain how and why the system works. Roadmaps, phase design docs,
mental models, "how does X work" explainers. Wherever they live (not only under
`docs/`).

**Do NOT apply it to `.claude/rules/` invariant files.** Those are code contracts
checked by `assert_invariants()` at runtime; they deliberately name exact types,
methods, and fields so a contributor can find and verify the check in code. Naming
is the point there. If you're writing or editing a rules file, keep the exact
identifiers.

When in doubt, ask: *is this doc explaining the system to a human, or pinning a
contract to specific code?* The former gets the concept-focused style; the latter
stays exact.

## The core principle: concepts over identifiers

Code gets refactored and renamed constantly; concepts are stable. A design doc
written around `pending_seal_keys` and `apply_roll_segment()` rots the moment
someone renames them — and worse, it quietly trains the reader to think in terms
of *where the code is* rather than *what the system does*. Write so the doc stays
true and useful even after a big refactor.

**The test for any name:** would it still make sense if someone renamed the
underlying Rust symbol tomorrow?

- **Domain vocabulary survives renames — keep it.** These are the words the system
  uses to talk about itself, shared across every doc: *seal request, roll, segment,
  range, replica set, keyspace, coordinator, leader / follower, commit boundary,
  shard group, lineage, SWIM, Raft, vnode.* Dropping these for vague paraphrases
  ("the seal message", "the roll thing") makes the doc *harder* to understand and
  impossible to cross-reference. Keep them.

- **Code identifiers don't survive renames — cut them or describe the role.**
  Struct / enum / method / field / variable / module names that exist only because
  that's what the symbol is called: `coordinator_tx`, `RaftEvent::MetadataApplied`,
  `ShardLeaderEntry`, `apply_roll_segment()`, `pending_rolls`. Replace each with
  the concept it represents.

- **Drop Rust type-definition code blocks.** Don't paste `struct`/`enum`
  definitions with field lists and signatures into a design doc. Describe the
  *information carried* and *why it's there* in prose or a table instead.

**Exception that proves the rule:** occasionally a literal identifier *is* the
clearest way to convey a concept — a wire constant, a well-known timer name, a
config key the reader will grep for. Use it when the name genuinely aids
understanding, not out of habit.

### Before / after

The shift is from "narrating the code" to "explaining the design."

> **Identifier-heavy (avoid):** On `CoordinatorCommand::SealRequest`, the actor
> checks `pending_seal_keys`; if absent it builds a `RollSegment`, calls
> `raft.propose()`, and on `Ok(log_index)` inserts into `pending_rolls:
> HashMap<(ShardGroupId, u64), PendingRollContext>`.

> **Concept-focused (prefer):** When a seal request arrives, the coordinator skips
> it if an identical seal is already in flight (dedup); otherwise it builds a roll,
> proposes it through Raft, and — if accepted — records pending context keyed by
> the proposal's log position, so it can answer the requester once the entry
> commits.

Same information, but the second version stays correct after a refactor and reads
as a *design*, not a code tour.

## Ground every claim in the real implementation

A design doc that describes aspirational or stale behavior is worse than no doc —
it misleads with authority. Before writing, verify against the actual code (grep,
read the modules, or delegate a search). When the code and an existing doc
disagree, **the code wins** — fix the doc.

Watch specifically for stale facts when editing: behavior that was renamed,
return values that changed (e.g. an operation that now returns a no-op instead of
an error), responsibilities that moved between components, or invented names that
never matched the code. Correct them to match reality, and note real gaps and
tradeoffs honestly ("temporarily violates the offset-continuity invariant; holds
again after correction") rather than papering over them.

## Structure and voice

Match the shape of the existing docs (read one or two siblings first — e.g. the
D-series in `docs/data-plane/` — to calibrate tone and density). The
recurring shape:

- **Title**, then a one-paragraph `**Goal:**` (what this accomplishes and why) and
  a `**Depends on:**` line for prerequisites.
- `---` separators between major sections.
- Sections that *build understanding in order*: the architecture / data shapes,
  then the central mechanism / flow, then edge cases and failure handling.
- **ASCII diagrams** for message sequences, data flows, and layouts. They carry a
  lot of meaning compactly and are worth the effort — most of these docs lean on
  them heavily.
- **Tables** for catalogs: wire-message directories, command → effect, trigger
  matrices, coverage matrices.
- **Examples**: concrete end-to-end walkthroughs (often a sequence diagram of a
  real scenario).
- **Invariants**: a numbered list near the end. Each item leads with a **bold
  one-line statement**, then explains *why it must hold* and what breaks without
  it. This "why" is the signature of these docs — never list a rule without its
  rationale.
- **Cross-references**: point to sibling docs and relevant `.claude/rules/` files
  by filename and section, so the reader can follow the thread.

Throughout, **explain the why**, not just the what. The reader can eventually find
*what* happens by reading code; what they can't recover is *why it was designed
that way* — the tradeoff that was chosen, the failure it prevents, the simpler
alternative that was rejected. That reasoning is the most valuable thing the doc
carries.

## Watch for invariants

Design-doc work often surfaces or changes system invariants. Per this project's
`CLAUDE.md`, if your writing introduces a **new** invariant or modifies an
existing one, stop and ask the user before proceeding — regardless of mode.
Invariants are system-level contracts that affect every test and every future
contributor; documenting one is a real decision, not a phrasing choice. (Also
keep invariant *cross-references* accurate — verify the number/section you cite
still matches the rules file.)

## Workflow

1. **Calibrate** — read one or two sibling design docs to match structure, density,
   and voice.
2. **Verify** — confirm the behavior you're about to describe against the actual
   code; note any drift in docs you're editing.
3. **Draft** — write in the structure above, leading with goal and why, leaning on
   diagrams and tables, keeping domain vocabulary and dropping code identifiers.
4. **Reread with fresh eyes** — hunt for leaked code identifiers and replace each
   with its concept; confirm every mechanism explains its *why*; check the
   invariants section leads with statements and follows with rationale; verify
   cross-references resolve.
5. **For edits to existing docs** — additionally fix stale facts to match current
   code and soften any leaked identifiers already present, but preserve the doc's
   existing structure and the conceptual vocabulary it already uses.
