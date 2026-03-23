import { assertEquals, assertRejects } from "@std/assert";
import { Database } from "@db/sqlite";
import { Kysely } from "kysely";
import { createTreeTables, SqliteTreeStore } from "../svc/store.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import type { TreeDatabase } from "../svc/store.ts";
import Scheduler, { OCCConflictError } from "../svc/scheduler.ts";

async function makeStore(): Promise<SqliteTreeStore> {
  const db = new Database(":memory:");
  const kysely = new Kysely<TreeDatabase>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
  await createTreeTables(kysely);
  return new SqliteTreeStore(kysely);
}

function makeScheduler(store: SqliteTreeStore): Scheduler {
  const root = new URL("..", import.meta.url).pathname;
  return new Scheduler(store, {} as never, {
    timeout: 10_000,
  });
}

// Override the runner's worker factory after construction
function withWorkerFactory(scheduler: Scheduler): void {
  const root = new URL("..", import.meta.url).pathname;
  (scheduler as any).runner.workerFactory = async (id: string) => {
    const code = await Deno.readFile(
      new URL(`./workers/${id}.ts`, import.meta.url),
    );
    const worker = new Worker(
      new URL("../lib/runner/glue.ts", import.meta.url).href,
      {
        type: "module",
        deno: {
          permissions: {
            read: [`${root}lib/ipc/`, `${root}lib/types.ts`],
            net: false,
            write: false,
            run: false,
          },
        },
      } as WorkerOptions,
    );
    worker.postMessage({ type: "init", entrypoint: code.buffer });
    return worker;
  };
}

async function seed(store: SqliteTreeStore) {
  await store.addNode(
    "root-1",
    undefined,
    new Map([["greeting", JSON.stringify("world")]]),
  );
  await store.setHead("root-1");
}

Deno.test("scheduler: happy path — executes and commits", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  const result = await scheduler.execute({
    from: "app/a" as any,
    to: "reader" as any,
    input: { key: "greeting" },
    metadata: null,
  });

  assertEquals(result.head, result.precommit.id);
  assertEquals(result.precommit.output, {
    key: "greeting",
    value: "world",
  });

  const node = await store.getNodeDetails(result.precommit.id);
  assertEquals(node !== null, true);
  assertEquals(node!.parent, "root-1");
  assertEquals(node!.from, "app/a");
  assertEquals(node!.to, "reader");
});

Deno.test("scheduler: OCC conflict — rejects when read key modified between base and head", async () => {
  const store = await makeStore();
  // root-1: greeting = "world"
  await store.addNode(
    "root-1",
    undefined,
    new Map([["greeting", JSON.stringify("world")]]),
  );
  await store.setHead("root-1");

  // Start event reading "greeting" at base = root-1
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  // Meanwhile, another event writes "greeting" = "changed"
  await store.addNode(
    "writer-1",
    undefined,
    new Map([["greeting", JSON.stringify("changed")]]),
  );
  await store.setHead("writer-1");

  // Reader ran at root-1 but head is now writer-1
  await assertRejects(
    () =>
      scheduler.execute({
        from: "app/a" as any,
        to: "reader" as any,
        input: { key: "greeting" },
        metadata: null,
        base: "root-1",
      }),
    OCCConflictError,
  );
});

Deno.test("scheduler: no conflict when read key unchanged between base and head", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  // Writer writes to "count" (doesn't touch "greeting")
  const r1 = await scheduler.execute({
    from: "app/a" as any,
    to: "writer" as any,
    input: { key: "count", value: 42 },
    metadata: null,
  });

  // Reader reads "greeting" — no conflict since "greeting" wasn't changed
  const r2 = await scheduler.execute({
    from: "app/a" as any,
    to: "reader" as any,
    input: { key: "greeting" },
    metadata: null,
  });

  assertEquals(r2.precommit.output, { key: "greeting", value: "world" });
});

Deno.test("scheduler: sequential writes chain correctly", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  const r1 = await scheduler.execute({
    from: "app/a" as any,
    to: "writer" as any,
    input: { key: "x", value: 1 },
    metadata: null,
  });

  const r2 = await scheduler.execute({
    from: "app/a" as any,
    to: "writer" as any,
    input: { key: "x", value: 2 },
    metadata: null,
  });

  // Second write's parent is the first write
  const node = await store.getNodeDetails(r2.precommit.id);
  assertEquals(node!.parent, r1.precommit.id);

  // Head is the last committed event
  assertEquals(await store.getHead(), r2.precommit.id);
});

Deno.test("scheduler: derived events included in precommit children", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  const result = await scheduler.execute({
    from: "app/a" as any,
    to: "caller" as any,
    input: { target: "hello", input: { ping: true } },
    metadata: null,
  });

  assertEquals(result.precommit.children.length, 1);
  const child = result.precommit.children[0]!;
  assertEquals(child.output, { message: "hello", from: "app/a" });
  // Derived event's base is the parent's base (the committed state it reads from),
  // not the parent event's ID (which isn't committed yet)
  assertEquals(child.base, result.precommit.base);
});

Deno.test("scheduler: derived events committed to store", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  const result = await scheduler.execute({
    from: "app/a" as any,
    to: "caller" as any,
    input: { target: "hello", input: { ping: true } },
    metadata: null,
  });

  // Both root and derived event are in the store
  const rootNode = await store.getNodeDetails(result.precommit.id);
  assertEquals(rootNode !== null, true);

  const child = result.precommit.children[0]!;
  const childNode = await store.getNodeDetails(child.id);
  assertEquals(childNode !== null, true);
  assertEquals(childNode!.parent, result.precommit.id);
});

Deno.test("scheduler: writes persisted in store state", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  await scheduler.execute({
    from: "app/a" as any,
    to: "writer" as any,
    input: { key: "mykey", value: "myval" },
    metadata: null,
  });

  // The written value should be retrievable from the store
  const stored = await store.get("mykey");
  assertEquals(stored, JSON.stringify("myval"));
});

Deno.test("scheduler: returns output and diffs", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  const result = await scheduler.execute({
    from: "app/a" as any,
    to: "writer" as any,
    input: { key: "count", value: 1 },
    metadata: null,
  });

  assertEquals(result.precommit.diffs.get("count"), "1");
  assertEquals(result.precommit.output, { written: "count" });
});

Deno.test("scheduler: worker error propagates", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  await assertRejects(
    () =>
      scheduler.execute({
        from: "app/a" as any,
        to: "failer" as any,
        input: null,
        metadata: null,
      }),
    Error,
    "intentional failure",
  );

  // Head should not have changed
  assertEquals(await store.getHead(), "root-1");
});

Deno.test("scheduler: OCC rejects when key is deleted between base and head", async () => {
  const store = await makeStore();
  // root-1: greeting = "world"
  await store.addNode(
    "root-1",
    undefined,
    new Map([["greeting", JSON.stringify("world")]]),
  );
  await store.setHead("root-1");

  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  // Delete "greeting" via a new event
  await store.addNode(
    "deleter-1",
    undefined,
    new Map([["greeting", null]]),
  );
  await store.setHead("deleter-1");

  // Reader at root-1 saw greeting="world", but head deleted it
  await assertRejects(
    () =>
      scheduler.execute({
        from: "app/a" as any,
        to: "reader" as any,
        input: { key: "greeting" },
        metadata: null,
        base: "root-1",
      }),
    OCCConflictError,
  );
});

Deno.test("scheduler: OCC detects TOCTOU conflict — head changes during execution", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  // Start a slow reader that takes 100ms to read "greeting".
  // While it runs, we'll commit a conflicting write underneath it.
  const promise = scheduler.execute({
    from: "app/a" as any,
    to: "slow_reader" as any,
    input: { key: "greeting", delay: 100 },
    metadata: null,
  });

  // Wait 10ms to ensure the runner has started (dispatched the worker),
  // then commit a write to the same key — simulating another scheduler
  // execution that completes before our slow_reader finishes.
  await new Promise((r) => setTimeout(r, 10));
  await store.addNode(
    "writer-1",
    "root-1",
    new Map([["greeting", JSON.stringify("changed")]]),
  );
  await store.setHead("writer-1");

  // The slow_reader's conflict check should detect that "greeting"
  // changed between its base (root-1) and the current head (writer-1).
  await assertRejects(() => promise, OCCConflictError);
});

Deno.test("scheduler: no TOCTOU conflict when concurrent write doesn't touch read set", async () => {
  const store = await makeStore();
  await seed(store);
  const scheduler = makeScheduler(store);
  withWorkerFactory(scheduler);

  // Start a slow reader that reads "greeting" after a 100ms delay.
  const promise = scheduler.execute({
    from: "app/a" as any,
    to: "slow_reader" as any,
    input: { key: "greeting", delay: 100 },
    metadata: null,
  });

  // While it runs, commit a write to a DIFFERENT key ("count").
  // This should NOT conflict since "greeting" is unchanged.
  await new Promise((r) => setTimeout(r, 10));
  await store.addNode(
    "writer-1",
    "root-1",
    new Map([["count", JSON.stringify(42)]]),
  );
  await store.setHead("writer-1");

  // Should succeed — no conflict on the read set.
  const result = await promise;
  assertEquals(result.precommit.output, { key: "greeting", value: "world" });
});
