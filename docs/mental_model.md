# Introduction to Arcanum

Arcanum is an event-sourced app runtime and personal server. It offers zero
downtime deploys, over-the-air updates, node identity, NAT traversal, an HTTP
server, event logging and significant extensibility.

It is designed to ensure that it is easy for developers to program for as well
as easy for users to set up.

## Prior Art

- Erlang/OTP
- Cloudflare Workers
- Urbit
- Houyhnhnm Computing

## Processes

Arcanum follows the actor-model pattern. A **process** is an entity with its own
state, which communicate with others by passing messages. Processes are meant to
be cheap and easy to create, and when unused they should not expend memory.

Unlike projects such as Erlang/OTP, processes in Arcanum should be thought of as
more like Cloudflare's Durable Objects: they represent "logical units of
coordination", and due to JavaScript's own async logic, your app doesn't need a
new process for every single async task.

Examples of good process boundaries include:

- Forum post
- Chess game
- Chatroom

Examples of bad process boundaries include:

- Like
- Chess piece
- Message

## Execution

Process execution is in many ways just like Erlang, and in many others, it has
nothing alike. This area is the most inspired by Cloudflare Durable Objects.

Process execution is broken into "chunks". A chunk is guaranteed to execute
synchronously such that there is no interleaving of operations between chunks
even across normal async boundaries.

In regular JavaScript, when you run a block of synchronous code, it is
guaranteed that there will be no other code running between operations. This is
because your computation is a single 'task' in the event loop. However, when
running Promises, your code is split up into multiple 'tasks' at the promise
boundary, leading to possible interleaving.

```js
async function doWork(name) {
  console.log(name, "step 1");
  await Promise.resolve(); // boundary: yields back to the event loop
  console.log(name, "step 2");
}

doWork("A");
doWork("B");

// Output:
// A step 1
// B step 1
// A step 2
// B step 2
```

Chunks are essentially the same. Arcanum will not create a new chunk when an
existing chunk is running.

Storage operations are always considered to be part of the same chunk as well
as any synchronous operations. However, any other async work such as network
I/O or message passing results in the current chunk ending, allowing other
tasks to run.

**Storage operations are not reverted when events throw!**

```js
class MyProcess extends Process {
  async foo() {
    const counter = await kv.get("counter");
    await kv.set("counter", counter + 1);

    const response = await fetch("https://example.com"); // -- chunk boundary --
    if (!response.ok) {
      throw new Error("Fetching failed")
    }
    const body = await response.text();
    console.log("First 100 chars:\n", body.slice(0, 100));

    const response2 = await ctx.send("^bob/example", "hi"); // -- chunk boundary --
    return response2;
  }
}
```

Standard chunk behavior can be overriden by creating a lock, preventing the
chunk fron ending until it hits `unlock`, in which case normal chunk behavior
will apply afterward, or when execution finishes. Use this sparingly becaues you
could cause slowdowns in your application if all other requests are waiting!

```js
await ctx.lock()
// ...
await ctx.unlock()
```

## Environment

Every app runs within a specific worker thread, which is heavily restricted so
that its only source of I/O is through IPC with the runtime's main thread.
Processes themselves run within a more restricted environment, which replaces
various globals with wrappers which log usage or simply removes them.

Notably, here are some web globals which are replaced. For any given I/O, they
are logged within chunks so they can be replayed or examined later on.
- Math.random, crypto.*
- setInterval, setTimeout, clearInterval
  - In replay, timeouts are ignored as zero
  - Timeouts and intervals do not persist beyond the end of execution
- fetch, XMLHttpRequest, WebSocket
- Date
- performance

Prototype pollution is also banned. Please do not try to do prototype pollution.

## Chunk Trees, Events and State

One major aspect of Arcanum that sets it apart from other systems in this regard
is the fact that it is natively event-logged. The state of any given process can
be acquired by traversing the linearized history from a given head.

Due to interleaving, discontinuous chunks from different calls may be appended
one after the other.

```ts
type ChunkCommit = {
  executionId: string;  // the current execution
  chunkSeq: number;     // position within that execution
  globalSeq: number;    // position across all executions
}
```

Let's say we have our happy little process:

```js
const counter = await kv.get("counter");
await kv.set("counter", counter + 1);

const response2 = await ctx.send("^bob/example", "hi"); // -- chunk boundary --
return response2;
```

### Runtime Flow

1. The previous chunk (`^bob/app/my-process/11/0`) is committed into the chunk
   log like this:

```ts
{
  executionId: 11,
  chunkSeq: 0,
  globalSeq: 24,
  inputs: [
    { type: "getKV", name: "counter", value: 5 }
  ],
  outputs: [
    { type: "setKV", name: "counter", value: 6 }
  ],
  effects: [
    { type: "sendCrossApp", to: "^bob/example", data: "hi" }
  ]
}
```

2. The runtime reads the `effects` list and then sends `^bob/example` a message
   at its entrypoint:

```ts
{
  type: "request",
  from: "^bob/app",
  to: "^bob/example",
  replyTo: "^bob/app/my-process/11/1",
  data: "hi"
}
```

3. `^bob/example` does something and returns a result. The execution is then
   committed.

```ts
{
  type: "response",
  from: "^bob/example",
  to: "^bob/app/my-process/11/1",
  data: "Hello world!"
}
```

Let's say we start our Arcanum up after shutting it down mid-execution. Here's
what the runtime conceptually does:

1. Load all app workers
2. Step through all chunk commits

## Communication
