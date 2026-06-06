# API Surface

Arcanum's API surface covers process-to-process communication, persistent KV
storage, HTTP client/server, networking, and process lifecycle. All user code is
Lua running inside Arcanum's embedded Luau executor.

## Process Model

Every process is addressable by a three-part ID:

```
^namespace/app/process
```

- `namespace` - the node owner, e.g. `arc`, `bob`
- `app` - the application name, e.g. `forum`, `cheeseboard`
- `process` - the process instance name

A process is a specific pair of **(handler, state)** - the handler defines the
behavior, the state is private to that instance. Processes are created by
calling `register(template, name)` which spawns a new instance backed by the
named handler from your app's handler table.

The `entrypoint` process is special - it is created automatically when the
system boots. Other processes must be explicitly registered via `register()`.
If the process part is omitted from an address, it defaults to `entrypoint`:

```
^arc/forum           → ^arc/forum/entrypoint
^arc/forum/board     → ^arc/forum/board   (must be registered first)
```

Events within a process are addressed by a four-part ID:

```
^namespace/app/process/e{seq}
```

## Context

`ctx` is the first parameter passed to every handler.

| Field | Alias | Description |
|-------|-------|-------------|
| `from` | - | The process ID that sent this message |
| `id` | `me`, `self`, `process`, `proc` | The current process ID |
| `handler` | - | The current handler's name within the process |
| `app` | - | The current app ID (e.g. `^arc/cheeseboard`) |

> **Note**: Messages from external sources always arrive from a local system
> process such as `^sys/http-server` (or, in the future, `^sys/net`). Check
> `ctx.from` and follow their API contract instead of treating the remote
> sender as the origin.

## IPC: call & notify

Processes communicate by passing messages. There are two primitives:

```lua
-- Sends a message and waits for a response (returns the callee's return value)
local result = call("^target/app/process", data)
-- or to the entrypoint:
local result = call("^target/app", data)  -- routes to entrypoint

-- Sends a message with no response expected
notify("^target/app/process", data)
```

Under the hood, `call` creates a **promise** - the runtime resumes the caller's
event when the callee finishes. `notify` is fire-and-forget.

### call

```luau
function call(target: string, data: any): any
```

- `target` - the destination process ID (e.g. `"^arc/forum"`)
- `data` - any Lua value (serialized as MessagePack)
- returns - the callee's return value

### notify

```luau
function notify(target: string, data: any): nil
```

- Does not block or return a value
- Messages are queued on the target's schedule and processed FIFO

## KV Storage

Every process has its own private key-value store. Values are strings.

```lua
-- Read a value (returns nil if the key does not exist)
local val = kv.get("my-key")

-- Write a value
kv.set("my-key", "my-value")
```

```luau
function kv.get(key: string): string?
function kv.set(key: string, value: string): nil
```

KV operations do not **complete the proposal** - the Lua thread resumes
immediately after the key-value pair is read or written.

> **Warning**: KV writes are not reverted if the event later errors. Side
> effects persist even on failure.

## HTTP Client

Arcanum ships with a built-in HTTP client. Calls are routed through `^sys/http`.

```lua
-- Options is optional

http.get("https://api.example.com/data", options)
http.post("https://api.example.com/data", options)
http.put("https://api.example.com/data", options)
http.delete("https://api.example.com/data", options)
http.request("https://api.example.com/data", "patch", options)
```

```luau
type HttpMethod =
    "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"

type ResponseType =
    "json" | "text" | "blob" | "arrayBuffer"

type QueryValue = string | number | boolean

type HttpRequestOptions = {
    method: HttpMethod?,
    headers: { [string]: string }?,
    query: { [string]: QueryValue }?,
    body: string | { [string]: any }?,
    timeoutMs: number?,
    responseType: ResponseType?,
}

type HttpResponse = {
    ok: boolean,
    status: number,
    statusText: string,
    headers: { [string]: string },
    content: string?,
}
```

All HTTP calls **complete the proposal** - the Lua thread is suspended while the
HTTP request is in flight. Responses are routed back to the caller via the
promise system.

## Process Lifecycle

### register

Register a new process instance from a template handler:

```lua
local board = register("board", "my-unique-id")
-- board is a reference to the new process
-- e.g. ^arc/cheeseboard/my-unique-id
```

```luau
function register(template: string, name: string): ProcessRef
```

- `template` - the handler name (a key in your app's returned handler table)
- `name` - a unique name for this process instance
- Returns a reference to the new process

If `register` is called twice with the same process instance name and handler,
it is a noop. If called with the same name but a **different** handler, it
errors.

The registered process gets its own isolated state and is spawned when the
first message arrives. The registering process can then interact with it via
`call` or `notify`.

## Arcnet

> **Not yet implemented.** This section describes planned functionality.

Arcnet is the peer-to-peer networking layer between Arcanum nodes. A process
can expose itself over Arcnet by claiming a **port name** - an identifier that
does not need to match the process or app name.

For example, `^arc/chatboard` running on `^bob` can claim the port `board`.
Remote nodes address it as `^bob:board`. This allows multiple implementations
of the same protocol:

```
^lux/my-chat-board    → port: board
^arc.sol/chatplace    → port: board
^bob/forum            → port: forum
```

```lua
-- Send a message and await a response
local response = call("^sys/net", "^target:port", message, options)
-- or via the net helper:
local response2 = net.call("^target:port", message, options)

-- Fire-and-forget
notify("^sys/net", "^target:port", message, options)
net.notify("^target:port", message, options)
```

Incoming Arcnet messages arrive from `^sys/net`:

```lua
function handler(ctx, msg)
    if ctx.from == "^sys/net" then
        local from = msg.from    -- sending node, e.g. ^arc/blog
        local to = msg.to        -- claimed port on this node
        local data = msg.data
        return string.concat("Thanks for calling, ", from, "!")
    end
end
```

```luau
type ArcnetOptions = {
    timeout: number?,
}

type ArcnetMessage = {
    from: string,       -- sending node and app, e.g. ^arc/blog
    to: string,         -- claimed port on the receiving node, e.g. board
    data: any,          -- the payload (serialized as MessagePack)
}
```

## HTTP Server

Arcanum's built-in HTTP server listens on port 6202. Routes are registered by
sending a message to the `^sys/http-server` process:

Messages to `^sys/http-server`:

```luau
type HttpServerAction = "add" | "remove" | "list-uris"

type HttpServerMessage = {
    action: HttpServerAction,
    app: string,   -- e.g. "my-namespace/my-app"
    host: string?, -- required for add/remove
}
```

### Actions

| Action | Description |
|--------|-------------|
| `add` | Register a route — maps `host` to a process. The process defined in `app` will receive HTTP requests whose `Host` header matches. |
| `remove` | Unregister a route. Returns the number of routes removed. |
| `list-uris` | List all registered hosts for the given app. |

### Responses

Responses come back as the return value of `call()`:

```luau
-- add (success)
{ ok: true }

-- add (error — invalid process ID)
{ error: string }

-- remove
{ ok: true, removed: number }

-- list-uris
{ uris: string[] }
```

### Incoming HTTP Requests

When an HTTP request arrives for a registered route, the target process
receives it from `^sys/http-server`:

- `ctx.from` is `^sys/http-server`
- `msg` is the HTTP body. If the body is valid JSON it is parsed as JSON;
  otherwise it arrives as a string.
- The Host header determines routing — the path is not used for routing but
  can be handled by the app.

```lua
function entrypoint(ctx, msg)
    -- msg is the HTTP body (parsed from JSON if applicable)
    return "response data"
end
```

### HTTP Response

The process's return value becomes the HTTP response:

- If the handler returns `nil`: `204 No Content`
- If the handler returns a value: `200 OK` with the handler's return value
  wrapped in `{"data": ...}` as JSON.
- If the handler errors: `500 Internal Server Error` with the error message.
- If the handler takes longer than 30 seconds: `504 Gateway Timeout`.

### Registration from Lua

```lua
-- Register a route
notify("^sys/http-server", {
    action = "add",
    app = "my-namespace/my-app",
    host = "example.com",
})

-- Remove a route
call("^sys/http-server", {
    action = "remove",
    app = "my-namespace/my-app",
    host = "example.com",
})
```

> The interactive shell (`^arc: sys/http-server add ...`) is not yet
> implemented. Route registration from Lua is the current workaround.

## Writing Apps

An app is a Lua module that returns a table mapping handler names to handler
definitions:

```lua
return {
    entrypoint = {
        handler = function(ctx, msg)
            return "Hello from my app!"
        end,
    },
    board = {
        handler = function(ctx, msg)
            -- handle messages for this handler
        end,
    },
}
```

Each key in the returned table is the handler name used with `register()`.
Each handler can be:

- A function: `board = function(ctx, msg) ... end`
- A table with a `handler` field:
  ```lua
  board = {
      handler = function(ctx, msg) ... end,
  }
  ```
- A file path string referencing another file in the package:
  ```lua
  board = {
      handler = "./board.lua",
  }
  ```

The handler function receives `(ctx, msg)` where `msg` is the
MessagePack-deserialized payload. The handler returns a value that becomes the
response for `call()`.

### Handler template files

Handlers can reference external Lua files by path instead of inline functions:

```lua
return {
    board = {
        handler = "./board.lua",
    },
}
```

The referenced file should return a function `(ctx, msg) -> any`.
