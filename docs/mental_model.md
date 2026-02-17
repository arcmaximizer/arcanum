# Mental Model

This should hopefully help you get a mental model of what exactly Arcanum is and
how to write code for it.

Arcanum is a Deno program which maintains an ACID transaction log and exchanges
messages with apps and runtime extensions running in Web Workers.

## Transactions and Events

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

Runtime extensions are special appsthat do not have persistent state tracked by
the system. They are used as glue code for conducting I/O.

A runtime extension should be as minimal as possible and have nearly all of its
logic happen within an Arcanum-tracked app.
