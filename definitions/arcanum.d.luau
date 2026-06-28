-- Arcanum API Type Definitions
-- For use with luau-lsp (https://github.com/JohnnyMorganz/luau-lsp)
-- Load via: --definitions:@arcanum=definitions/arcanum.d.luau
--
-- Or in VSCode settings:
--   "luau-lsp.types.definitionFiles": ["definitions/arcanum.d.luau"]

-- Types ---------------------------------------------------------------------

export type HttpMethod =
    "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"

export type ResponseType = "json" | "text" | "blob" | "arrayBuffer"

export type QueryValue = string | number | boolean

export type HttpRequestOptions = {
    method: HttpMethod?,
    headers: { [string]: string }?,
    query: { [string]: QueryValue }?,
    body: string | { [string]: any }?,
    timeoutMs: number?,
    responseType: ResponseType?,
}

export type HttpResponse = {
    ok: boolean,
    status: number,
    statusText: string,
    headers: { [string]: string },
    content: string?,
}

export type Ctx = {
    --- The process ID that sent this message
    from: string,
    --- The current process ID
    id: string,
    --- Alias for id
    me: string,
    --- Alias for id
    self: string,
    --- Alias for id
    process: string,
    --- Alias for id
    proc: string,
    --- The current handler's name within the process
    handler: string,
    --- The current app ID (e.g. ^arc/cheeseboard)
    app: string,
}

--- A reference to a registered process, returned by register().
export type ProcessRef = {
    --- The process ID string (e.g. ^arc/cheeseboard/my-instance)
    _process: string,
    --- Send a message to the entrypoint or a named handler, await response
    call: ((self: ProcessRef, data: any) -> any)
        & ((self: ProcessRef, handler: string, data: any) -> any),
    --- Fire-and-forget message to the entrypoint or a named handler
    notify: ((self: ProcessRef, data: any) -> nil)
        & ((self: ProcessRef, handler: string, data: any) -> nil),
}

export type HttpServerAction = "add" | "remove" | "list-uris"

export type HttpServerMessage = {
    action: HttpServerAction,
    app: string,
    host: string?,
}

export type HttpServerAddResponse = { ok: true }

export type HttpServerErrorResponse = { error: string }

export type HttpServerRemoveResponse = { ok: true, removed: number }

export type HttpServerListResponse = { uris: {string} }

-- Globals --------------------------------------------------------------------

--- Send a message and wait for a response.
--- Returns the callee's return value.
declare function call(target: string, data: any): any

--- Send a fire-and-forget message. Does not block or return a value.
declare function notify(target: string, data: any): nil

--- Register a new process instance from a template handler.
--- Returns a reference to the new process which can be used with call/notify.
declare function register(template: string, name: string): ProcessRef

--- Per-process private key-value store.
declare kv: {
    --- Read a value. Returns nil if the key does not exist.
    get: (key: string) -> string?,
    --- Write a value. KV writes persist even if the event later errors.
    set: (key: string, value: string) -> nil,
}

--- Per-process SQLite database with parameterized queries.
declare sql: {
    --- Execute a SQL statement. Parameters are positional varargs.
    exec: (statement: string, ...any) -> any,
    --- Query rows. Returns an array of rows. Parameters are positional varargs.
    query: (statement: string, ...any) -> {any},
}

--- Built-in HTTP client. Calls are routed through ^sys/http.
declare http: {
    --- HTTP GET request.
    get: (url: string, options: HttpRequestOptions?) -> HttpResponse,
    --- HTTP POST request.
    post: (url: string, options: HttpRequestOptions?) -> HttpResponse,
    --- HTTP PUT request.
    put: (url: string, options: HttpRequestOptions?) -> HttpResponse,
    --- HTTP DELETE request.
    delete: (url: string, options: HttpRequestOptions?) -> HttpResponse,
    --- Generic HTTP request. `method` is case-insensitive.
    request: (url: string, method: string, options: HttpRequestOptions?) -> HttpResponse,
}
