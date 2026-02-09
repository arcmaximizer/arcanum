# Footguns

This is a list of things to please not do in your apps, both because you may not
only hurt yourself but also hurt the user experience.

## Using setTimeout and setInterval

The isolate is not guaranteed to last. If you want to schedule tasks for the
future, please use the timer object passed in `ctx` to your functions.

Bad usage:

```js
function onArcnet(req, env, ctx) {
  setTimeout(() => env.reply("Ping!"), 3_000);
  return "I'll send you a request soon!";
}
```

Good usage:

```js
function onArcnet(req, env, ctx) {
  let timerId = ctx.addTimer({ sender: req.sender, message: "Ping!" }, 3_000);
  return `I'll send you a request soon! Timer ID: ${timerId}`;
}

function onTimer(event, env, ctx) {
  env.send(event.sender, event.message);
}
```

## Using filesystem APIs

This will not work. Use the state provided.

## Using global-scoped state

**You will break determinism!** Don't do this unless doing so for performance
reasons - but be extremely careful with it as well.

## Using randomness in the global scope

This will not work.
