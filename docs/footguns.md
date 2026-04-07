# Footguns

## Always yield system calls

A system call is a call to the runtime, such as:
```ts
yield ctx.send("^bob/example", "data")
```

Under the hood, all that `ctx.send` does is return a JavaScript object, which is
then serialized and sent to the main runtime process. As such, simply calling it
on its own will do **nothing**.
