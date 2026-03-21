# Footguns

This is a list of things to please not do in your apps, both because you may not
only hurt yourself but also hurt the user experience.

## Using setTimeout and setInterval

The worker is not guaranteed to last. If you want to schedule tasks for the
future, call the timer app via `ctx.call()`.

Bad usage:

```ts
export default async function (from, req, ctx) {
  setTimeout(() => console.log("Ping!"), 3_000);
  return "I'll send you a request soon!";
}
```

Good usage:

```ts
export default async function (from, req, ctx) {
  const timerId = await ctx.call("sys/timer", {
    target: from,
    message: "Ping!",
    delay: 3_000,
  });
  return `I'll send you a request soon! Timer ID: ${timerId}`;
}
```

## Using filesystem APIs

This will not work. Use `ctx.get()` and `ctx.set()` for state.

## Using global-scoped state

**You will break determinism!** Don't do this unless doing so for performance
reasons - but be extremely careful with it as well.

## Using randomness in the global scope

This will not work. Use `ctx.random()` or call a randomness extension.

## Blocking the event loop

Don't do this. Offload blocking computations to a dedicated app or runtime
extension via `ctx.call()`.
