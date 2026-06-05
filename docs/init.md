# First Setup

Setting up your Arcanum is designed to be easy. Here's how you do it.

## Building

```bash
git clone https://github.com/arcmaximizer/arcanum
cd arcanum
cargo build --release
```

The binary will be at `target/release/arcanum`.

## Creating a Package

A package is a `.tar.gz` containing at least a `main.lua` file and an
`arcanum.toml` manifest.

### main.lua

Your app returns a table mapping handler names to handler functions. The
`entrypoint` handler is special — it runs when the app starts (see
[Writing Apps](api.md#writing-apps) for details).

```lua
return {
    entrypoint = function(ctx, msg)
        return "Hello from my app!"
    end,
    board = function(ctx, msg)
        return "board says: " .. tostring(msg)
    end,
}
```

### arcanum.toml

```toml
name = "^my-namespace/my-app"
```

The `name` field determines how your app is addressed (e.g.
`^my-namespace/my-app/entrypoint`). The `^` prefix is added automatically if
missing.

### Building the tarball

```bash
mkdir -p my-app
cp main.lua arcanum.toml my-app/
cd my-app && tar czf my-app.tar.gz main.lua arcanum.toml
```

## Running Arcanum

Create a data directory and a `store` subdirectory:

```bash
mkdir -p ~/my-arcanum/store
```

Copy your package into the store:

```bash
cp my-app.tar.gz ~/my-arcanum/store/
```

Start the node:

```bash
arcanum --data-dir ~/my-arcanum
```

Arcanum will:
1. Scan `~/my-arcanum/store/` for `.tar.gz` packages
2. Extract each package and register its name from `arcanum.toml`
3. Spawn the `entrypoint` handler for each package
4. Start the HTTP server on port 6202
5. Wait for you to press Ctrl+C to shut down

### CLI Options

```
-d, --data-dir <DIR>       Data directory (default: OS data dir + /arcanum)
-c, --config <FILE>        Config file path
    --port <PORT>          HTTP server port (default: 6202)
    --bind <ADDR>          HTTP server bind address (default: 127.0.0.1)
    --packages-dir <DIR>   Directory of packages to auto-load
    --auto-load-packages   Enable auto-loading from packages dir
```

## Interacting with the Node

Currently there is **no interactive shell** — `arcanum` runs as a daemon and
exits on Ctrl+C.

The HTTP server is running and can route requests to your apps, but routes must
be registered **from within a running process** by sending a message to
`^sys/http-server`:

```lua
notify("^sys/http-server", {
    action = "add",
    app = "my-namespace/my-app",
    host = "example.com",
})
```

A common pattern is to self-register in the entrypoint handler:

```lua
return {
    entrypoint = function(ctx, msg)
        notify("^sys/http-server", {
            action = "add",
            app = "my-namespace/my-app",
            host = "example.com",
        })
        return "registered"
    end,
}
```

After the route is registered, HTTP requests with the matching `Host` header
will be forwarded to your app's `entrypoint` handler. The request body is
passed as the `msg` parameter, and the return value is sent back as the HTTP
response.

```bash
curl -X POST http://127.0.0.1:6202/any-path \
  -H "Host: example.com" \
  -d "hello from curl"
```

## What's Not Implemented

These features are described in the docs but not yet built:

- **Interactive shell** — no `arcanum install`, `sideload`, `update`, or
  in-shell `sys/http-server add` commands
- **Arcnet** — peer-to-peer networking between nodes
- **App registry** — no public store to download packages from
- **Package updates** — no version tracking or zero-downtime update mechanism
- **Identity service** — no keypair registration or node naming
