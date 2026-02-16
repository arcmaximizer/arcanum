# The Event Log

The history of your Arcanum is a function of the event log, such that you are
always able to step back through it.

Events are any sources of indeterminism, or in other words, inputs. This
includes, but is not limited to:

- HTTP requests
- Arcnet messages
- Randomness

Events may contain multiple sources of indeterminism on the inside. This is fine
as long as the indeterministic results are stored within the log such that it
can be replayed.

An event triggered by HTTP, alarm or external arcnet message is called a root
event. Root events are only committed to the event log when they are complete.

For example, let's take an app that messages a different server before then
messaging a different app.

```ts
async function onHttp(request, env, ctx) {
  const key = request.body;

  const currentValue = await env.PINGS.get(key);
  const count = currentValue ? currentValue : 0;

  await env.PINGS.put(key, count + 1);

  const res = await ctx.send(key, "ping!");
  console.log("We got a ping response from", key, "-", res);

  // Let's talk to a local app now
  const res2 = await ctx.send("sys/echo", "ping!");

  // Return response
  return new Response(
    JSON.stringify({
      pings: newCount,
      message: `Thanks, just pinged the app you sent.`,
    }),
    {
      headers: { "Content-Type": "application/json" },
    },
  );
}
```

The event log looks a bit like this:

```
root event: (sys/http) receives HTTP request
 |---> receive reply from other Arcanum
 '---> reply & local execution of sys/echo
        '---> any side effects in the sys/echo program (we have none)
```

The event metadata looks a bit like this:

```
root event: receive HTTP request

state reads: PINGS[key]

external calls:
  pinging other Arcanum
```
