# Side Effects

Side effects are a part of any program. However, to maintain the event log's
consistency, **Arcanum only ever performs side effects at the end of a
transaction.**

A side effect is an output effect that runs after an event is committed to the
mainline. Side effects are _not_ part of the event's computation — they are
executed after the event tree has been committed, and their success or failure
does not affect the commit itself.

## Why defer side effects?

Event execution must be deterministic and replayable. If an event performed I/O
(e.g., sending an HTTP request) during execution, replaying the event would
produce a different result. By deferring side effects to after commit, Arcanum
guarantees that:

- Event execution is always replayable against the same state
- The event tree commits atomically — either all events succeed or all are
  abandoned
- Side effects do not block the commit pipeline

## How side effects work

When an event completes, it may produce side effects alongside its return value.
These effects are collected and executed after the entire transaction tree is
committed.

From the runner's perspective:

1. An event returns a result with optional side effects (see
   [Runner](./runner.md))
2. The runner waits for all children in the transaction tree to complete
3. Once all events are done, the transaction is committed to the event tree
4. Side effects are then executed in order

If a side effect fails, it does not roll back the commit. The event and its
state changes are already persisted.

## Examples of side effects

- Sending a notification to an external service
- Triggering a webhook
- Writing to a log
- Emitting a message to a runtime extension

## Side effects vs derived events

A derived event (`ctx.call()`) is _not_ a side effect. Derived events are part
of the transaction tree — they execute during the event, their results are
available to the caller, and they are committed together with the parent event.

A side effect runs _after_ commit and has no return value.
