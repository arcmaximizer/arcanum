# Runtime Extensions

There are some things you might want to do with your Arcanum that cannot be done
using solely the functions offered by the runtime. You would need some kind of
system _outside_ of the standard Arcanum model to do this.

This is called a runtime extension. Potential uses of runtime extensions
include:

- Discord bots via the Gateway
- Native local GPU/inference API
- DNS servers
- Hardware access

As the Arcanum runtime cannot and will not support every single potential I/O
method, it is up to developers to write this glue code.

Runtime extensions **do not have their state tracked by Arcanum**. Just like I/O
in general, it should be treated as fundamentally unreliable and ephemeral. A
runtime extension will not maintain its state after a reboot as it is stored
within the global scope.

Runtime extensions use the same function signature as regular apps:

```ts
export default async function (from: string, req: unknown, ctx) {
  // from — the ProgramId of the caller
  // req  — the event payload
  // ctx  — the event context
}
```

For example, here is a runtime extension which maintains a persistent WebSocket
connection to an external server:

```ts
let sockets: Map<string, WebSocket> = new Map();

export default async function (from, req, ctx) {
  if (req.type == "open") {
    if (sockets.has(req.url)) return "already exists";
    const ws = new WebSocket(req.url);
    sockets.set(req.url, ws);

    ws.onmessage = (e) => {
      ctx.call(from, e.data);
    };
  }
}
```

When a given runtime extension is killed, the runtime is rebooted or the state
of the runtime extension has otherwise been reset, `sys/events` will send an
event to all apps in arbitrary order. Apps may then use this to recreate any
lost connections or other things.
