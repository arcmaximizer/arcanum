# Scheduler vs Docs Alignment Analysis

## Overview

Comparing `src/scheduler.rs` against `docs/event_loop.md` and `docs/mental_model.md`.

---

## 1. Promise Storage Mismatch

**Docs** (`mental_model.md:35-36`): Promise is stored globally.
> "append a promise to the set of pending promises"

**Code** (`scheduler.rs:31,35`): Promise lives on Proposal, not globally.
```rust
pub struct Proposal {
    pub promise: Option<Promise>,  // <-- promise is per-proposal, not global
}
```

**Impact**: Medium. The docs describe a global "set of pending promises" but the code nests them in proposals. Promise resolution in `satisfy_proposal` (lines 175-194) also uses the root chunk's proposal, which seems correct for call/response flow, but there's no global tracking.

---

## 2. Event Status Transitions Missing

**Docs** (`event_loop.md:82-88`): Explicit state machine.
```
Pending -> Running (on dequeue)
Running -> Completed (on receipt without promise)
Running -> Running (on receipt with promise -> new event)
Running -> Failed (on error)
```

**Code** (`scheduler.rs`): No `EventStatus` type, no transitions. The scheduler only has `event__counter` (line 58) which just tracks sequence numbers — there's no concept of Pending/Running/Completed/Failed states.

**Impact**: High. The event loop docs describe a full state machine that the scheduler doesn't implement.

---

## 3. Promise Resolution Logic Gap

**Docs** (`event_loop.md:32-36`): Promise creates new proposal and event.
```
if effects contain a promise:
   - route the return value to promise target
   - create new proposal for target process
   - pass return value as input to new event
   - continue from step 1 (loop)
```

**Code** (`scheduler.rs:175-194`): Resolution creates a new `Proposal` for the source event's process using the root chunk's returns as inputs. This is close but:
- Line 190: `promise: None` is set on the new proposal — correct
- However, there's no mechanism to actually enqueue this new proposal into the target process's schedule

**Impact**: Medium. The code creates the proposal but never adds it to any schedule.

---

## 4. `ensure_event_running` Not Implemented

**Docs** (`event_loop.md:17-19`): Event must be ensured Running before execution.
```
2. event = ensure_even_running(proposal.event)
   - if event doesn't exist, create it with status = Running
   - this step is atomic with step 1
```

**Code** (`scheduler.rs`): `satisfy_proposal` creates events lazily via `event_counter` (line 78) but never checks/updates status.

---

## 5. Proposal Source/Process Naming Inconsistency

**Docs** (`mental_model.md:26`): Proposal has a **source**.
> "Proposal: a chunk requested to be executed in the future, with inputs, **source** and other data"

**Code** (`scheduler.rs:27-32`): Proposal has `process`, not `source`.
```rust
pub struct Proposal {
    pub process: ProcessId,   // <-- called "process" not "source"
    pub event: Option<EventId>,
    pub inputs: Vec<String>,
    pub promise: Option<Promise>,
}
```

**Impact**: Low. The terms are semantically similar (both reference a process), but the docs use "source" while code uses "process".

---

## 6. `promise.target` Semantics

**Code** (`scheduler.rs:37`): `Promise.target` is an `EventId`.
```rust
pub struct Promise {
    pub id: u64,
    pub target: EventId,
}
```

**Docs** (`event_loop.md:112-117`): Promise routes to a process/event pair.
```
- When event A calls event B, a promise is created
- When event B completes, its result routes back to event A
```

**Impact**: Low. The code stores target as EventId (includes process+proc+seq), which is more specific than needed but not wrong.

---

## Summary

| Issue | Severity | Docs Location | Code Location |
|-------|----------|---------------|--------------|
| Global promise set missing | Medium | mental_model.md:35-36 | scheduler.rs:31,35 |
| Event status state machine | High | event_loop.md:82-88 | Not implemented |
| New proposal not enqueued | Medium | event_loop.md:32-36 | scheduler.rs:186-194 |
| `ensure_event_running` | High | event_loop.md:17-19 | Not implemented |
| "source" vs "process" | Low | mental_model.md:26 | scheduler.rs:27 |

---

## Priority Fixes

1. **Event status** — needs `EventStatus` enum (Pending/Running/Completed/Failed) and transitions in `satisfy_proposal`
2. **Ensure event running** — add status check/create before executing
3. **Promise global tracking** — either add a `promises: HashMap<PromiseId, Promise>` field or update docs to clarify promises live on proposals
4. **Enqueue resolved proposal** — line 190 creates a new proposal but never adds it to the schedule