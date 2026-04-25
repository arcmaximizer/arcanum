# Introduction to Arcanum

Arcanum is an event-sourced app runtime and personal server. It offers zero
downtime deploys, over-the-air updates, node identity, NAT traversal, an HTTP
server, event logging and significant extensibility.

## Prior Art

- Erlang/OTP
- Cloudflare Workers
- Urbit
- Houyhnhnm Computing

## Terminology

- App: a group of **processes**
- Handler: a stateless Lua function
- Process: a logical entity with a **handler, state, identity, schedule and
  history** which executes **chunks** sequentially, not to be confused with an
  OS process
- Chunk: a unit of computation within a given **event**, mapping onto a block of
  work between two yield points, which is either a **proposal, execution or
  receipt**
- Receipt: a **chunk** that has been completed and appended to the database
- Proposal: a **chunk** requested to be executed in the future, with inputs,
  **source** and other data
- Execution: either a **chunk** which is being executed (for lack of a better
  word) or the action of taking a **proposal**, performing the associated
  computation and then producing a **receipt**
- Event: a unit of computation consisting of a list of **chunks**, created by
  some **source**, which maps onto a single Lua coroutine
- Schedule: a list of **proposals** on a **process** which are awaiting
  **execution**
- History: a list of previously executed **chunks**
- Promise: a unique identifier used to route the return values of **derived
  events** back to their **source events**, creating a new **proposal**
- Derived event: 
- Source event: **event** which directly causes a **proposal** to be added to a
  different

## Processes

Arcanum follows the actor-model pattern. A **process** is an entity with its own
state, which communicate with others by passing messages. Processes are meant to
be cheap and easy to create, and when unused they should not expend memory.

Unlike projects such as Erlang/OTP, processes in Arcanum should be thought of as
more like Cloudflare's Durable Objects: they represent "logical units of
coordination", and due to JavaScript's own async logic, your app doesn't need a
new process for every single async task. (TODO: Edit this for Lua)

Examples of good process boundaries include:

- Forum post
- Chess game
- Chatroom

Examples of bad process boundaries include:

- Like
- Chess piece
- Message

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
boundary, leading to possible interleaving. TODO: Edit this for Lua

```lua
local processes = {
  myProcess = {
    id = "myProcess",
    handler = function(ctx, msg)
      local counter = ctx.kv.get("counter") or 0
      ctx.kv.set("counter", counter + 1)

      local response = fetch("https://example.com") -- chunk boundary
      if not response.ok then
        error("Fetching failed")
      end
      local body = response.text()
      print("First 100 chars:\n" .. string.sub(body, 1, 100))

      local response2 = ctx.call("^bob/example", "hi") -- chunk boundary
      return response2
    end,
  },
}
```

Chunks are essentially the same. Arcanum will not create a new chunk when an
existing chunk is running.

Storage operations are always considered to be part of the same chunk as well
as any synchronous operations. However, any other async work such as network
I/O or message passing results in the current chunk ending, allowing other
tasks to run.

**Storage operations are not reverted when events error!**

```lua
local processes = {
  myProcess = {
    id = "myProcess",
    handler = function(ctx, msg)
      local counter = ctx.kv.get("counter") or 0
      ctx.kv.set("counter", counter + 1)

      local response = fetch("https://example.com") -- chunk boundary
      if not response.ok then
        error("Fetching failed")
      end
      local body = response.text()
      print("First 100 chars:\n" .. string.sub(body, 1, 100))

      local response2 = ctx.call("^bob/example", "hi") -- chunk boundary
      return response2
    end,
  },
}
```

When you expect an async response from a given execution, that is called a
promise. For example:

```lua
local response = ctx.call("^bob/example", "hi")
```

## System Calls

A process is essentially a piece of code which may:
- Take in input (as ctx and msg)
- Make syscalls, handing control flow back to the runtime
- Return an output

There are only two main system call types: state operations and IPC. Operations
such as web fetching or other I/O are implemented 'as if' they were really
processes: they can be addressed as apps such as `^sys/http` but most crucially,
**their state is not tracked by Arcanum.**

System calls can either block the process or not block it. A blocking system
call means that Arcanum will not treat this as a chunk boundary and begin to
process another chunk. A non-blocking system call means that Arcanum will end
the 

The following system calls are blocking:
- KVRead: reads a value from a key in the process's KV
- KVWrite: writes a value to a key in the process's KV

The following system calls are non-blocking:
- Call: append a message to the chunk queue of a different process, before then
  appending a promise to the set of pending promises
- Notify: append a message to the chunk queue of a different process

## Chunk Trees, Events and State

One major aspect of Arcanum that sets it apart from other systems in this regard
is the fact that it is natively event-logged. The state of any given process can
be acquired by traversing the linearized history from a given head.

Due to interleaving, discontinuous chunks from different calls may be appended
one after the other.

## WIP

This entire document, like the other docs, is a massive work in progress!
