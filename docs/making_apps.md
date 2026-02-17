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

capabilities:
  - sys/arcnet:send,receive
  - sys/timer:set
```

The `local` developer is a reserved developer ID in the Arcanum namespace and is
meant for quick development. You can install it directly on your Arcanum, but
trying to distribute them to other Arcanum nodes via the Storefront will not
work.

```ts
async function onEvent(event, env) {
  if (env.process == "timer") {
    env.send(event.to, event.message);
  } else {
    let timerId = await env.send(
      "sys/timer",
      { content: { to: env.process, message: "Ping!" }, delay: 3000 },
    );
    return `I'll send you a request soon! Timer ID: ${timerId}`;
  }
}

export default onEvent;
```

You can deploy this app on your very own Arcanum node using
`dx arcanum-cli push .`

Once pushed, you should be able to access it at `my-app--local.tryarcanum.org`,
which forwards to localhost. If your Arcanum is running at a different IP, use
`my-app--<nodename>.tryarcanum.org`.
