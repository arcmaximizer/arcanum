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
desc: Callback!
entrypoint: main.ts

capabilities:
  - sys/http:receive
  - sys/arcnet:send,receive
arcnet:
  aliases:
    - #my-app
```

The `local` developer is a reserved developer ID in the Arcanum namespace and is
meant for quick development. You can install it directly on your Arcanum, but
trying to distribute them to other Arcanum nodes via the Storefront will not
work.

```ts
async function onHttp(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;

  const currentValue = await env.MY_KV.get(key);
  const count = currentValue ? currentValue : 0;

  await env.MY_KV.put(key, count + 1);

  // Return response
  return new Response(
    JSON.stringify({
      path: path,
      visits: newCount,
      message:
        `This app has been visited ${newCount} time(s). Message me over Arcnet at #my-app@${env.nodeId} to get a callback!`,
    }),
    {
      headers: { "Content-Type": "application/json" },
    },
  );
}

async function onArcnet(request, env, ctx) {
}

async function onTimer(event, env, ctx) {
}

export { onArcnet, onHttp, onTimer };
```
