# Runner

Every app runs in a persistent Web Worker. The runner is responsible for:

- Sending events to workers for execution
- Receiving call and result messages back from workers
- Tracking in-flight events and releasing resources when they complete or
  timeout
- Ensuring that if a worker never responds, the system does not leak memory

The runner and worker communicate using a bidirectional IPC layer built on
`postMessage`. Both sides can call methods on each other and await responses.

## Terms

- **Runner**: The host-side module on the main thread. Manages workers, events,
  contentions, and timeouts.
- **Worker**: A persistent Web Worker. One per app (identified by `to`). Handles
  multiple in-flight events, sometimes concurrently, over its lifetime.
- **Glue code**: The runtime layer inside the Worker that bridges `postMessage`
  IPC to userspace function calls. Injects deterministic primitives and abort
  checking.
- **Event**: A unit of computation. Sent from the runner to a worker, executed
  in userspace, and a result returned to the runner.
- **Derived event**: An event spawned from within another event's execution.
  Derived events are created via `ctx.call()` (part of the parent's
  transaction).
- **Transaction**: A tree of events rooted at a single event. Includes the root
  event and all derived events spawned via `ctx.call()` during its execution.
- **Contention**: A reference-counted marker on an event's base state. Prevents
  the cache from evicting state while an event is using it.
- **Pending event**: An event that has been sent to a worker but has not yet
  returned a result.

## IPC Protocol

The runner and worker communicate via the IPC library (`lib/ipc/`). Both sides
use the same interface:

```
ipc.call(method, body)   → sends a request, returns a promise
ipc.on(method, handler)  → registers a handler for incoming calls
ipc.off(method)          → unregisters a handler
ipc.terminate()          → tears down the IPC, rejects all pending calls
```

### Message Types

**Runner → Worker:**

| Method    | Body     | Description                         |
| --------- | -------- | ----------------------------------- |
| `execute` | proposal | Execute an event in userspace       |
| `abort`   | eventId  | Signal that this event is abandoned |

**Worker → Runner:**

| Method     | Body                | Description                            |
| ---------- | ------------------- | -------------------------------------- |
| `call`     | proposal            | Request to execute a derived event     |
| `getState` | key                 | Read state at the current event's base |
| `result`   | output, sideEffects | Event completed                        |
| `error`    | error string        | Event failed                           |

### Call/Return Flow

The IPC library provides bidirectional RPC. When the worker calls
`ipc.call("call", proposal)`, the runner's `on("call", handler)` fires, executes
the derived event, and returns the result through the IPC promise. The worker's
`call()` awaits this promise, then continues its userspace execution.

The runner and worker can call each other freely. There is a single message
handler on each side — no listener explosion. Request IDs correlate calls with
responses.

## Event Lifecycle

### 1. Proposal

The runner receives an event proposal. It generates a unique `eventId` and
determines the `rootId` for the transaction (either from the proposal metadata
or the event's own ID if it is the root).

### 2. Contention

The runner adds a contention on the event's `base` — the state snapshot this
event will run against. While the contention is active, the cache will not evict
the materialized state for that base.

### 3. Dispatch

The runner sends an `execute` message to the worker for the event's `to` app. If
no worker exists for that app, one is spawned. The worker receives the message,
sets up an `AsyncLocalStorage` context for the event, and calls the userspace
function.

### 4. Execution

The userspace function runs. It can:

- Call deterministic primitives (`ctx.random()`, `ctx.time()`, `ctx.uuid()`)
- Read state via `ctx.getState(key)`
- Call other apps via `ctx.call(app, input)` — awaits a response, part of the
  transaction tree
- Write to local state (tracked as diffs)
- Return a result

### 5. Response

The userspace function returns. The worker posts back a `result` message with
the output and any side effects. The runner stores the result but does not
resolve the parent's promise yet — it waits for all children in the transaction
tree to complete (see [Derived Events](#derived-events)). Once every event in
the tree has returned, the runner resolves all promises and commits.

### 6. Timeout

If the event does not return within the configured timeout:

1. The runner fires the timeout for the event.
2. The runner calls `abandon(rootId)` to cancel the entire transaction tree.
3. All pending events under that `rootId` are removed and their contentions
   released.
4. An `abort` message is sent to the relevant worker(s).
5. The promise is rejected with a timeout error.
6. If the worker later sends a result for any event in the abandoned tree, it is
   silently discarded.

The worker is **not terminated**. It continues to accept and process new events.
Only the timed-out event's work is abandoned.

## Contention Management

Contentions are reference-counted markers on event base states. The cache
eviction policy is:

- `addContention(eventId)`: Increment the ref count. State for this event is
  pinned.
- `removeContention(eventId)`: Decrement the ref count. If ref count reaches
  zero, the state is evicted from the cache.

Contentions are released when:

- The event completes normally (runner receives the result)
- The event times out (runner abandons the transaction tree)

If an event's contention is never released (e.g., a bug in the runner), the
cached state will never be evicted, causing a memory leak. The timeout mechanism
exists primarily to prevent this.

## Derived Events

From the userspace perspective, a derived event looks like a regular function
call to another app:

```typescript
// Awaited — block until the child event completes, get the result
const result = await ctx.call("other-app", "query");

// Not awaited — I don't need the result, but the event still runs
// as part of my transaction
ctx.call("other-app", "increment");
```

### `ctx.call(app, input)`

Sends an event to `app`. This creates a derived event tracked under the parent's
`rootId` in the transaction tree. The runner waits for all children before
resolving the parent's promise — even if the userspace function didn't `await`
the call.

The glue code sends a `call` message to the runner. The runner:

1. Generates a new `eventId` for the derived event.
2. Tracks it under the parent's `rootId` in the transaction map.
3. Calls `execute()` recursively — adding contention, dispatching to a worker,
   and waiting for the result.
4. Sends the result back to the originating worker.

Whether the user `await`s the call or not, the child event is part of the same
transaction. The distinction is only about whether the user needs the return
value.

### Transaction Tree

All events spawned via `ctx.call()` during a root event's execution share a
`rootId`. The runner maintains a `transactions` map:
`Map<rootId,
Set<eventId>>`. As derived events are created, they are added to
their root's set.

When any event in the tree times out, `abandon(rootId)` cancels all events in
the set. This ensures atomicity — either the entire event tree succeeds or
everything is abandoned.

```
rootId: "evt-A"
  ├── evt-A          (root, processed first)
  ├── evt-B          (spawned by A via ctx.call())
  └── evt-C          (spawned by A via ctx.call())
      └── evt-D      (spawned by C via ctx.call())
```

If `evt-A` times out, all four events are abandoned. If `evt-D` times out,
`abandon("evt-A")` is called, and the entire tree is cancelled.

#### Completion semantics

The runner does not resolve a parent's promise until all children in the tree
have completed. This means:

```typescript
// This starts a derived event. The user ignores the result,
// but the runner still waits for it before committing the transaction.
ctx.call("other-app", "do-work");

// The return value of this function is the parent's output.
// The transaction is not committed until all children finish,
// including the ctx.call above.
return "done";
```

The parent's function returns, but the parent's promise stays pending until the
last child in the tree sends its `result`. Only then does the runner resolve the
parent, release contentions, and commit.

## Determinism

Worker code runs inside an `AsyncLocalStorage` context. Each in-flight event has
its own context, keyed by `eventId`. Impure operations are routed through the
context.

The glue code monkey-patches built-in globals (`Math.random`, `Date.now`) at
Worker startup so that userspace code works without modification. The patches
read from `AsyncLocalStorage` to get the current event's context.

This ensures that event execution is replayable — running the same event against
the same state produces identical results.

## Abort & Cancellation

When a timeout fires, the runner sends an `abort` message to the worker. The
worker adds the event ID to its `_aborting` set.

Every context call in the userspace function includes a `checkAbort()` check. If
the event is in the aborting set, the call throws an `AbortError`, which is
caught by the glue code and sent back as an error.

The only unabortable case is a tight synchronous loop that never touches any
context call (e.g., `while (true) {}`). These are pathological and considered
user error. The timeout still gives the main thread back — it stops waiting and
releases resources — but the worker slot remains occupied until the worker
naturally finishes or is restarted.

## Worker Lifecycle

Workers are persistent. They are not terminated on event completion or timeout.
A worker is created once per app (identified by `to`) and reused across multiple
events.

The runner tracks active workers in a `workers` map: `Map<to, IPC>`. When a new
event targets an app that does not yet have a worker, one is created via the
`WorkerFactory` and its IPC is established.

When the IPC is terminated (`ipc.terminate()`), all pending calls on that
channel are rejected with an error.
