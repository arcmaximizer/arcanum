# API Surface

Arcanum's API surface covers process-to-process communication, persistent KV
storage, HTTP client/server, networking, and process lifecycle. All user code is
Lua running inside Arcanum's embedded Luau executor.

## Process Model

Every process is addressable by a three-part ID:

```
^namespace/app/process
```

- `namespace` — the node owner, e.g. `arc`, `bob`
- `app` — the application name, e.g. `forum`, `cheeseboard`
- `process` — the specific handler within the app, e.g. `entrypoint`, `board`

If the process part is omitted, it defaults to `entrypoint`:

```
^arc/forum           → ^arc/forum/entrypoint
^arc/forum/board     → ^arc/forum/board
```

Events within a process are addressed by a four-part ID:

```
^namespace/app/process/e{seq}
```

## Context

`ctx` is the first parameter passed to every handler.

| Field | Alias | Description |
|-------|-------|-------------|
| `from` | — | The process ID that sent this message |
| `id` | `me`, `self`, `process`, `proc` | The current process ID |
| `handler` | — | The current handler's name within the process |
| `app` | — | The current app ID (e.g. `^arc/cheeseboard`) |

> **Warning**: Messages from over the network always arrive from a local system
> process such as `^sys/net` or `^sys/http`. Check `ctx.from` and follow their
> API contract instead of treating the remote sender as the origin.

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

Under the hood, `call` creates a **promise** — the runtime resumes the caller's
event when the callee finishes. `notify` is fire-and-forget.

### call

```luau
function call(target: string, data: any): any
```

- `target` — the destination process ID (e.g. `"^arc/forum"`)
- `data` — any Lua value (serialized as MessagePack)
- returns — the callee's return value

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

KV operations do not **complete the proposal** — the Lua thread resumes
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

All HTTP calls **complete the proposal** — the Lua thread is suspended while the
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

- `template` — the handler ID within the app (defined in the app's process map)
- `name` — a unique name for this process instance
- Returns a reference to the new process

The registered process gets its own isolated state. The registering process can
then interact with it via `call` or `notify`.

## Arcnet

Arcnet is the peer-to-peer networking layer between Arcanum nodes. A process
can expose itself over Arcnet by claiming a **port name** — an identifier that
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

Arcanum's built-in HTTP server listens on port 6202. Users can register URI
bindings via the shell:

```
^arc: sys/http-server add arc/forum forum.example.org
^arc: sys/http-server remove arc/forum forum.example.org
^arc: sys/http-server list-uris arc/forum
```

Processes may also register URI bindings programmatically at runtime.

## Writing Apps

An app is a Lua module that returns a map of process handlers:

```lua
local processes = {
    entrypoint = {
        id = "entrypoint",
        handler = function(ctx, msg)
            return "Hello from my app!"
        end,
    },
    board = {
        id = "board",
        handler = function(ctx, msg)
            -- handle messages for this handler
        end,
    },
}

return processes
```

Each handler receives `(ctx, msg)` where `msg` is the MessagePack-deserialized
payload. The handler returns a value that becomes the response for `call()`.

### Handler template files

Handlers can reference external Lua files by path instead of inline functions:

```lua
board = {
    id = "board",
    handler = "./board.lua",
}
```

The referenced file should return a function `(ctx, msg) -> any`.
