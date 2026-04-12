# Introduction to Arcanum

Arcanum is an event-sourced app runtime and personal server. It offers zero
downtime deploys, over-the-air updates, node identity, NAT traversal, an HTTP
server, event logging and significant extensibility.

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

When writing code, what you are writing is a **process handler**. This is a
chunk of code that serves sort of like a "template" for a process: you can have
many different processes based on that.

Every app has a special **entrypoint** process. When defining the entrypoint
handler, note that it is guaranteed you will only have one instance of it and
that it will be addressable by a call to the app's name, like `^arc/my-app`.

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
export const processes = {
  myProcess: {
    id: "myProcess",
    handler: async (ctx, msg) => {
      const counter = (await ctx.kv.get("counter")) ?? 0;
      await ctx.kv.set("counter", counter + 1);

      const response = await fetch("https://example.com"); // -- chunk boundary --
      if (!response.ok) {
        throw new Error("Fetching failed");
      }
      const body = await response.text();
      console.log("First 100 chars:\n", body.slice(0, 100));

      const response2 = await ctx.send("^bob/example", "hi"); // -- chunk boundary --
      return response2;
    },
  },
};
```

Standard chunk behavior can be overriden by creating a lock, preventing the
chunk from ending until it hits `unlock`, in which case normal chunk behavior
will apply afterward, or when execution finishes. Use this sparingly because you
could cause slowdowns in your application if all other requests are waiting!

```js
await ctx.lock();
// ...
await ctx.unlock();
```

## Environment

Every process runs within a V8 isolate of its own. Like Cloudflare Workers,
they may be evicted at any time, so there should be no behavior that requires
long-running tasks. Globals common in the Web and other environments are not
guaranteed to be present in the Arcanum environment.

Notably, here are some web globals which are replaced. For any given I/O, they
are logged within chunks so they can be replayed or examined later on.
- Math.random, crypto.*
- Date
- performance
- fetch, XMLHttpRequest, WebSocket
- setInterval, setTimeout, clearInterval
  - In replay, timeouts are ignored as zero
  - Timeouts and intervals do not persist beyond the end of execution

Prototype pollution is also banned. Please do not try to do prototype pollution.

## Chunk Trees, Events and State

One major aspect of Arcanum that sets it apart from other systems in this regard
is the fact that it is natively event-logged. The state of any given process can
be acquired by traversing the linearized history from a given head.

Due to interleaving, discontinuous chunks from different calls may be appended
one after the other.

## WIP

This entire document, like the other docs, is a massive work in progress!
