![Arcanum](./header.png)

Arcanum is an event-sourced app runtime and personal server. Applications are
written in Lua and run inside Arcanum's embedded Lua executor. The runtime
provides KV storage, SQL storage, an HTTP client and server, process-to-process
IPC (call/notify), and dynamic process registration.

## Status

Very early stage. The core works. The interactive shell, package registry,
Arcnet networking, rollbacks and update flows are not yet built. Expect breaking
changes. Not to be used in production.

## Quick Start

```bash
# Build
git clone https://github.com/arcmaximizer/arcanum
cd arcanum
cargo build --release

# Create a package (tar.gz with arcanum.toml + main.lua)
mkdir -p my-app/store
cat > my-app/main.lua << 'EOF'
return {
    entrypoint = function(ctx, msg)
        return "Hello from " .. ctx.app
    end,
}
EOF
cat > my-app/arcanum.toml << 'EOF'
name = "^hello/world"
EOF
cd my-app && tar czf store/pkg.tar.gz main.lua arcanum.toml

# Run
cargo run --data-dir .
```

## Creating Apps

Apps are Lua modules that return a table of handlers. Drop a `.tar.gz`
containing `main.lua` and `arcanum.toml` into the `store/` directory. The
entrypoint handler is spawned automatically on boot.

See [docs/init.md](docs/init.md) and [docs/api.md](docs/api.md) for details.

## License

Arcanum is an explicitly ideological project in the service of Free Software.

Free Software is not a matter of price, but of freedom. It is the idea that the
operator of a given system has the natural right to use, modify and share their
code. In operating systems such as Microsoft Windows, you are restricted by law
from studying the source code: men with guns will come to your home and take you
away for releasing your changes, while they themselves rely on the work of many
thousands of permissive and unpaid maintainers.

Free Software is not a matter of practicality either. Often, we must accept that
much of the software of the modern world is built to restrict our freedom and
thus must reject their usage, making our lives harder in the short run in the
service of personal virtue - though in many cases, there exist Free programs
which perform similarly or even better to proprietary counterparts, and without
creating a culture of dependency on a given maintaining entity.

**All code works under Arcanum are licensed under the [GNU General Public License 3.0](./LICENSE).**
This means that you have a non-revocable, non-exclusive right to use the system,
read and modify the code, share copies and share your modified copies.

To the extent possible under law, [the copyrights and related or neighboring
rights to the documentation and art assets are fully waived](https://creativecommons.org/publicdomain/zero/1.0/).

