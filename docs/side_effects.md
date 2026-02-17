# Side Effects

Side effects are a part of any program. However, to maintain the event log's
consistency, **Arcanum only ever performs side effects at the end of a
transaction.**

For example, let's take the following code:

```ts
```
