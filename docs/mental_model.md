# Mental Model

This should hopefully help you get a mental model of what exactly Arcanum is and
how to write code for it.

Arcanum is a Deno program which maintains trees of events.

An event that is in the event DAG is called a committed event.

## Anatomy of an Event

An event goes through multiple stages before being appended to the event tree.

1. Proposed
2. Executed
3. Committed

When an event is proposed, it looks something like this:

```
id: <uuid>
from: dev/app
to: dev2/app2
data: <serializable data>
```

After it is executed, it looks like this:

```
id: <uuid>
parent: <uuid>
from: dev/app
to: dev2/app2
data: <any serializable data>
returns: <any serializable data>
index: 10

reads: key, key2, key3, ...
writes: key: value, key2: value2, key3: value3, ...
events: <uuid>, <uuid>, ...
couples: <uuid>, <uuid>, <uuid>, ...
```

- `id` is a unique identifier for the event. It is not based on a cryptographic
  hash of the event data, but rather generated randomly.
- `from` and `to` are unique identifiers for the app and versions involved
- `data` is the data sent to the app in the event
- `returns` is the data that the app returns
- `parent` is the identifier of the previous event in the tree.
- `index` is used to prevent cycles: the event must always have a higher index
  than its parent
- `reads` are the list of state keys read by this event, `writes` are the
  key-value pairs written by the event
- `derived` are the IDs of events in other trees that were created by this one
- `couples` are the IDs of events in other trees that this depends on

We call events that were created by a given event _derived events_, and the
events which create other ones are called _origin events_.

During first execution, an event runs against the current state. The execution
of an event and all of its derived events is treated as an atomic unit, so that
if one event execution is found to have conflicting state (see below), all the
changes (including derived events) will be rolled back and the event will be
re-executed against the latest state.

An event is capable of being nondeterministic, _however_ all these instances are
tracked within Arcanum such that one can always replay an event execution
against the current state.

When an event is rolled back after it has been committed, such as by forking the
tree and then switching to the new branch, the derived events _are not_
reverted. However, coupled events _are_ reverted.

Say we have the tree for program A, which has an event ("a") that is in a couple
with another event ("b") in the tree of program B. When the tree of program A is
reverted to a point before a, program B also forks at the parent of b.

TODO: Maybe use better name than "revert", do something gitlike to emphasize
multiple branches?

## Conflict Resolution

When an event is first executed, it runs against the state of the entire system
(the current state of all other event trees touched at that moment). If there is
a conflict due to a variable being read incorrectly, it will revert all of the
derived events as well.

## Code Upgrades

TODO

## Transactions and Events

!! THIS IS NO LONGER ACCURATE - TRANSACTIONS DO NOT EXIST ANYMORE !!

A transaction is an ordered list of events that is atomically committed and can
only be rolled back or replayed as an entire unit. It contains a "root event",
which is always either from the runtime or a runtime extension.

During a transaction, a state change is treated as if it has already occurred,
while other side effects only execute after a transaction has been committed.
For instance, let's say we have the following app, where `subscribers` is an
array of app IDs like `local/hello-world` and `local/cat-fetcher`.

```js
async function onEvent(event, state, ctx) {
  if (event.name == "read") {
    return await state.counter.get();
  } else if (event.name == "write") {
    const newValue = await state.counter.increment();
    const subscribers = await state.subscribers.get();

    subscribers.forEach((i) => i.send(newValue));

    return `new value written: ${newValue}`;
  }
}
```

The transaction:

```
sys/arcnet (root event)
  - local/my-app
    - local/hello-world
    - local/cat-fetcher
      - sys/http
    - sys/arcnet

state changes:
  - counter (local/my-app): 1 -> 2
  - cat_picture (local/my-app): (image) -> (new image)

external inputs:
  - fetch("https://api.thecatapi.com/v1/images/search?format=src") -> (image)

side effects: N/A
```

Apps are structured as deterministic state transition functions over a logged
execution: they take in an event and, within a transaction, compute state
updates and perform read‑only operations such as HTTP GET. Every external input,
including network responses, is captured in the transaction log such that
replaying the log will drive the app through the same sequence of states and
outputs.

From the point of view of the app, these network requests are ordinary side
effects that occur during execution. At the engine level, however, logging all
external inputs makes the system deterministically replayable: given the same
transaction log, the engine will reconstruct the same state changes and emitted
operations.

## Runtime extensions

Runtime extensions are special apps that do not have persistent state tracked by
the system. They are used as glue code for conducting I/O.

A runtime extension should be as minimal as possible.

## Rollbacks

Due to an Arcanum storing multiple DAGs of history, one history for one app is
independent of another. This means that users will have the ability to alter the
history of one app without altering the other.
