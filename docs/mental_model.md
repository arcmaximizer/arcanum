# Mental Model

This should hopefully help you get a mental model of what exactly Arcanum is and
how to write code for it.

Arcanum is a Deno program which maintains many append‑only, git‑like event
histories with safe branching and compaction. Every app has its own event tree,
and it is always possible to linearize an event history starting from the
current head.

## Terms

- Event: a unit of computation representing one invocation of a worker
- Proposed event: an event not yet added to a line
- Committed event: an event added to a line
- Head: a potential head in the current mainline
- Potential head: an event with no children
- Child: an event which is ordered after another event in a line
- Parent: an event that is ordered before another event in a line
- Event tree: a tree of events, has a root and a head
- Root: the event with no parent in a given event tree
- Mainline: linear list of events from the head to the root
- Line: linear list of events from a potential head to the root
- Derived event: event which is triggered by an origin event during execution
- Origin event: event which triggers a derived event during execution
- Side effect: an output effect run after an event is committed to the mainline

## Anatomy of an Event

An event goes through multiple stages before being appended to an app's event
tree.

1. Proposal
2. Execution
3. Commitment

When an event is proposed, it looks something like this (pseudodata):

```
type: event
id: <uuid>
from: dev/app
to: dev2/app2
data: <serializable data>
```

After it is executed, it looks like this (pseudodata):

```
type: committed_event
id: <uuid>
parent: <uuid>
from: dev/app
to: dev2/app2
index: 10
data: <any serializable data>
returns: <any serializable data>

reads: key, key2, key3, ...
writes: key: value, key2: value2, key3: value3, ...
derived: <uuid>, <uuid>, ...
effects: <effect>, <effect>, ...
```

- `id` is a unique identifier for the event. It is not based on a cryptographic
  hash of the event data, but rather generated randomly.
- `from` and `to` are unique identifiers for the app and versions involved
- `data` is the data sent to the app in the event
- `returns` is the data that the app returns
- `parent` is the identifier of the previous event in the tree.
- `index` is an incrementing counter equal to the number of ancestors
- `reads` are the list of state keys read by this event, `writes` are the
  key-value pairs written by the event
- `derived` are the IDs of events in other trees that were directly created by
  this event
- `effects` are the side effects of the event, executed after a commit

We call events that were created by a given event _derived events_, and the
events which create other ones are called _origin events_.

During first execution, an event runs against the current state. The execution
of an event and all of its derived events is treated as an atomic unit, so that
if one event execution is found to have conflicting state (see below), all the
changes (including derived events) will be rolled back and the event will be
re-executed against the latest state.

An event is capable of getting input from the outside such as randomness, HTTP
GET requests, or as responses from events to other runtime extensions, _however_
all these instances are tracked within Arcanum such that one can always replay
an event execution against the state.

### Checkpoint Creation Sequence

_This section was written primarily by OpenCode._

The store implementation uses a specific sequence for checkpoint creation to
ensure correctness:

1. **Insert the node** with `checkpoint_id: undefined` (since it will have
   writes)
2. **Insert the event's writes** into `kv_writes` with a temporary checkpoint ID
3. **Create the checkpoint** which materializes the state from the lineage
4. **Update the node** with the new checkpoint ID
5. **Update the writes** with the correct checkpoint ID

This sequence ensures that:

- The checkpoint includes the event's own writes (not just ancestor writes)
- The checkpoint state is materialized correctly
- No race conditions occur during concurrent reads

The implementation differs from a naive approach where writes are inserted after
checkpoint creation, which would result in checkpoints missing the event's own
writes.

## Derived Events

A derived event is another event created by the execution of an event. In layman
terms, it's when you call app A, and app A sends an event to app B.

The app is able to receive the event and then either return a response
(`returns`) or not. The response will then be copied to the origin event as an
input.

## Conflict Resolution

Each app is capable of specifying a conflict resolution policy over state. For
instance, we may want to use OCC (default), or even naive writes.

When an event is first executed, it runs against the state of the entire system
(the current state of all other event trees touched at that moment). If there is
a conflict due to a variable being read incorrectly, it will not commit any of
the derived events as well.

## Code Upgrades

A code upgrade is not strictly an event, but it is a node in the event tree of
an app. It has a completely different structure from an event.

The structure of a code upgrade is somewhat like this:

```
type: upgrade
id: <uuid>
parent: <uuid>
index: 11
code: <uuid>
```

The `code` field is a UUID pointing to code inside the cache.

## Runtime extensions

Runtime extensions are special apps that do not have persistent state tracked by
the system. They are used as glue code for conducting I/O.

A runtime extension should be as minimal as possible.

## Database Schema Implementation

_This section was written primarily by OpenCode._

The store implementation uses SQLite with Kysely to persist the event tree.
Here's how the mental model maps to the actual database schema:

### Nodes Table

- `id`: Unique identifier for the event (matches mental model)
- `parent`: Parent event ID (matches mental model)
- `base`: The base event used for state reconstruction (defaults to parent or
  own ID for root nodes)
- `checkpoint_id`: ID of the checkpoint materializing this event's state
- `from`: App identifier where the event originated (matches mental model)
- `to`: App identifier where the event is sent (matches mental model)
- `index`: Incrementing counter (matches mental model)
- `data`: JSON-serialized input data sent to the app (matches mental model)
- `returns`: JSON-serialized output data returned by the app (matches mental
  model)

### Key-Value Tables

- `kv_writes`: Stores key-value writes per event, linked to a checkpoint
- `kv_reads`: Stores keys read per event
- `checkpoint_state`: Materialized state at checkpoint events

### Derived Events & Effects

- `derived_events`: Maps origin events to derived events (many-to-many)
- `effects`: Stores side effects for events

### Checkpoint System

- Checkpoints materialize the full state at a specific event
- Checkpoints form a chain via parent relationships
- The `event_id` in the checkpoints table references the parent checkpoint's
  event ID (not the current event), enabling efficient state reconstruction

### Base Field Clarification

- The `base` field is used for state reconstruction optimization
- For root nodes (no parent), `base` defaults to the node's own ID
- This ensures every event has a defined base for consistent state rebuilding

### Cache Service

- Implements contention-based eviction strategy
- `addContention(eventId)`: Marks that an event is being processed (creates a
  state snapshot)
- `removeContention(eventId)`: Marks that event processing is complete
- **State eviction**: Occurs when contention count reaches zero
- This ensures state is preserved during event processing and evicted when no
  longer needed
