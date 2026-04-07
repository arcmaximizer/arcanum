# Flows

## Cross-App Communication

Let's say we have two apps, `^bob/app` and `^bob/example`. The code for them
looks like the following:

```ts
// ^bob/app/my-process

const counter = yield kv.get("counter");
await kv.set("counter", counter + 1);

const response2 = yield ctx.send("^bob/example", "hi"); // -- chunk boundary --
return response2;
```

```ts
// ^bob/example
return "Hello world"
```

1. The previous chunk (`^bob/app/my-process/11/0`) is committed into the chunk
   log like this:

```ts
{
  executionId: 11,
  chunkSeq: 0,
  globalSeq: 24,
  inputs: [
    { type: "getKV", name: "counter", value: 5 }
  ],
  outputs: [
    { type: "setKV", name: "counter", value: 6 }
  ],
  effects: [
    { type: "sendApp", to: "^bob/example", data: "hi", key: "ea49a71963b7bb1c954db6c7e5d2929f"}
  ],
  end: false
}
```

2. The runtime reads the `effects` list and then sends a message to the inbox of
   `^bob/example`:

```ts
{
  type: "request",
  from: "^bob/app",
  to: "^bob/example",
  replyTo: "^bob/app/my-process/11",
  data: "hi",
  key: "ea49a71963b7bb1c954db6c7e5d2929f"
}
```

3. The execution of `^bob/example` finishes and is committed:

```ts
{
  executionId: 5,
  chunkSeq: 0,
  globalSeq: 5,
  cause: {
    type: "request",
    from: "^bob/app",
    to: "^bob/example",
    replyTo: "^bob/app/my-process/11",
    data: "hi",
    key: "ea49a71963b7bb1c954db6c7e5d2929f"
  },
  inputs: [/* ... */],
  outputs: [/* ... */],
  effects: [/* ... */],
  returns: "Hello world",
  end: true
}
```

4. `^bob/example`'s response is then routed by the runtime to the specific
   execution of `^bob/app/my-process`:

```ts
{
  type: "response",
  from: "^bob/example",
  to: "^bob/app/my-process/11",
  data: "Hello world!",
  key: "ea49a71963b7bb1c954db6c7e5d2929f"
}
```

5. Control flow resumes at `^bob/app/my-process`, creating a new chunk precommit
   `^bob/app/my-process/11/1`:

```ts
{
  executionId: 11,
  chunkSeq: 1,
  globalSeq: 25,
  inputs: [
    {
      type: "response",
      from: "^bob/example",
      to: "^bob/app/my-process/11/1",
      data: "Hello world!"
    }
  ],
  outputs: [/* ... */],
  effects: [/* ... */],
  returns: "Hello world",
  end: true
}
```

6. The precommit is then committed and the execution of `^bob/app` ends.

## Interrupted Cross-App Communication

We have the same thing that we did before! However, let's say the runtime shut
down before it sent the message and now we have to pick up from where we left
off.

1. The previous chunk (`^bob/app/my-process/11/0`) is committed into the chunk
   log like this:

```ts
{
  executionId: 11,
  chunkSeq: 0,
  globalSeq: 24,
  inputs: [
    { type: "getKV", name: "counter", value: 5 }
  ],
  outputs: [
    { type: "setKV", name: "counter", value: 6 }
  ],
  effects: [
    { type: "sendApp", to: "^bob/example", data: "hi" }
  ],
  end: false
}
```

2. The runtime restarts after this is committed but before the effect runs. It
   loads all app code again then assembles a list of in-flight executions:

```ts
[
  {
    executionId: "^bob/app/my-process/11",
    chunks: [
      {
        chunkSeq: 0,
        globalSeq: 24,
        inputs: [
          { type: "getKV", name: "counter", value: 5 }
        ],
        outputs: [
          { type: "setKV", name: "counter", value: 6 }
        ],
        effects: [
          { type: "sendApp", to: "^bob/example", data: "hi", key: "ea49a71963b7bb1c954db6c7e5d2929f" }
        ],
        end: false
      }
    ]
  }
]
```

3. The runtime compiles a list of all `sendApp` and `sendProcess` events without
   a corresponding response:

```ts
[
  { type: "sendApp", to: "^bob/example", data: "hi", key: "ea49a71963b7bb1c954db6c7e5d2929f" }
]
```

3. The runtime reads the `effects` list and then sends a message to the inbox of
   `^bob/example`:

```ts
{
  type: "request",
  from: "^bob/app",
  to: "^bob/example",
  replyTo: "^bob/app/my-process/11/1",
  data: "hi"
}
```

3. The execution of `^bob/example` finishes and is committed:

```ts
{
  executionId: 5,
  chunkSeq: 0,
  globalSeq: 5,
  inputs: [/* ... */],
  outputs: [/* ... */],
  effects: [/* ... */],
  returns: "Hello world",
  end: true
}
```

4. `^bob/example`'s response is then routed by the runtime to the specific
   execution of `^bob/app/my-process`:

```ts
{
  type: "response",
  from: "^bob/example",
  to: "^bob/app/my-process/11/1",
  data: "Hello world!"
}
```

5. Control flow resumes at `^bob/app/my-process`, creating a new chunk precommit
   `^bob/app/my-process/11/1`:

```ts
{
  executionId: 11,
  chunkSeq: 1,
  globalSeq: 25,
  inputs: [
    {
      type: "response",
      from: "^bob/example",
      to: "^bob/app/my-process/11/1",
      data: "Hello world!"
    }
  ],
  outputs: [/* ... */],
  effects: [/* ... */],
  returns: "Hello world",
  end: true
}
```

6. The precommit is then committed and the execution of `^bob/example` ends.
