# Making Your First App

Prerequisites: Arcanum, Deno

Great, now that you've installed your Arcanum, it's time to create your first
app. Let's initialize our directory using the following command:

```
dx create-arcanum my-app
```

You should get a directory structure that goes a little bit like this:

```
main.ts
arcanum.yaml
README.md
```

Let's take a look inside the `arcanum.yaml` to see what's going on:

```yaml
# arcanum.yaml
id: local/my-app
version: 0.0.0
name: My App
desc: My awesome application
entrypoint: main.ts
```

The `local` developer is a reserved developer ID in the Arcanum namespace and is
meant for quick development. You can install it directly on your Arcanum, but
trying to distribute them to other Arcanum nodes via the Storefront will not
work.

## Writing your handler

Every app exports a default async function with the following signature:

```ts
export default async function (from: string, req: unknown, ctx) {
  // from — the ProgramId of the caller
  // req  — the event payload (arbitrary serializable data)
  // ctx  — the event context
}
```

The context object (`ctx`) provides the following methods:

| Method                 | Description                                  |
| ---------------------- | -------------------------------------------- |
| `ctx.get(key)`         | Read a value from state. Returns `undefined` |
|                        | if the key does not exist.                   |
| `ctx.set(key, value)`  | Write a key-value pair to state.             |
| `ctx.exists(key)`      | Check whether a key has a value. Returns     |
|                        | `true` or `false`.                           |
| `ctx.call(app, input)` | Send a derived event to another app. Returns |
|                        | the other app's response. Part of your       |
|                        | transaction.                                 |

## Example: a simple counter

```ts
export default async function (from, req, ctx) {
  const value = (await ctx.get("value")) ?? 0;

  switch (req.command) {
    case "raise": {
      await ctx.set("value", value + 1);
      return value + 1;
    }
    case "lower": {
      await ctx.set("value", value - 1);
      return value - 1;
    }
    default: {
      throw { type: "INVALID_COMMAND" };
    }
  }
}
```

## Deploying

You can deploy this app on your very own Arcanum node using
`dx arcanum-cli push .`

Once pushed, you should be able to access it at `my-app--local.tryarcanum.org`,
which forwards to localhost. If your Arcanum is running at a different IP, use
`my-app--<nodename>.tryarcanum.org`.
