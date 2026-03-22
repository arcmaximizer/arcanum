import { assertEquals, assertRejects } from "@std/assert";
import { Database } from "@db/sqlite";
import { Kysely } from "kysely";
import { createTreeTables, SqliteTreeStore } from "../svc/store.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import type { TreeDatabase } from "../svc/store.ts";
import Runner from "../svc/runner.ts";

async function makeStore(): Promise<SqliteTreeStore> {
  const db = new Database(":memory:");
  const kysely = new Kysely<TreeDatabase>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
  await createTreeTables(kysely);
  return new SqliteTreeStore(kysely);
}

async function seed(store: SqliteTreeStore) {
  await store.addNode(
    "root-1",
    undefined,
    new Map([["greeting", JSON.stringify("world")]]),
  );
  await store.setHead("root-1");
}

async function makeRunner(
  store: SqliteTreeStore,
  timeout?: number,
): Promise<Runner> {
  return new Runner(store, {} as never, async (id) => {
    const root = new URL("..", import.meta.url).pathname;
    const code = await Deno.readFile(
      new URL(`./workers/${id}.ts`, import.meta.url),
    );
    const worker = new Worker(
      new URL("../lib/runner/glue.ts", import.meta.url).href,
      {
        type: "module",
        deno: {
          permissions: {
            read: [
              `${root}lib/ipc/`,
              `${root}lib/types.ts`,
            ],
            net: false,
            write: false,
            run: false,
          },
        },
      } as WorkerOptions,
    );
    worker.postMessage({ type: "init", entrypoint: code.buffer });
    return worker;
  }, {
    timeout,
  });
}

Deno.test("runner: simple event returns output", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "hello" as any,
    input: { greeting: "hi" },
    metadata: null,
  });

  assertEquals(result.output, { message: "hello", from: "app/a" });
  assertEquals(typeof result.id, "string");
  assertEquals(result.id.length, 36); // UUID format
  assertEquals(result.base, "root-1");
});

Deno.test("runner: explicit base reads state from base, not head", async () => {
  const store = await makeStore();
  // root-1 is the head, has no state
  await store.addNode("root-1");
  await store.setHead("root-1");
  // snap-1 has different state (addNode sets head to snap-1, but the explicit
  // base means the runner uses snap-1 regardless of head)
  await store.addNode(
    "snap-1",
    undefined,
    new Map([["color", JSON.stringify("blue")]]),
  );

  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "reader" as any,
    input: { key: "color" },
    metadata: null,
    base: "snap-1",
  });

  assertEquals(result.base, "snap-1");
  // If it read from head (root-1), color would be undefined
  assertEquals(result.output, { key: "color", value: "blue" });
});

Deno.test("runner: base is captured before execution, not affected by head change", async () => {
  const store = await makeStore();
  // root-1 has state: name = "old"
  await store.addNode(
    "root-1",
    undefined,
    new Map([["name", JSON.stringify("old")]]),
  );
  // root-2 has state: name = "new"
  await store.addNode(
    "root-2",
    undefined,
    new Map([["name", JSON.stringify("new")]]),
  );
  // Set head to root-1 (addNode sets head to the new node, so we override)
  await store.setHead("root-1");
  assertEquals(await store.getHead(), "root-1");

  const runner = await makeRunner(store, 500);

  // Start execution — base is resolved to "root-1" (current head) immediately
  const p = runner.execute({
    from: "app/a" as any,
    to: "slow_reader" as any,
    input: { key: "name", delay: 100 },
    metadata: null,
  });

  // Change head while the worker is sleeping
  await store.setHead("root-2");

  // The worker's ctx.get should still read from root-1's state, not root-2's
  const result = await p;
  assertEquals(result.base, "root-1");
  assertEquals(result.output, { key: "name", value: "old" });
});

Deno.test("runner: worker reads undefined for missing key", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "reader" as any,
    input: { key: "nonexistent" },
    metadata: null,
  });

  assertEquals(result.output, { key: "nonexistent", value: undefined });
});

Deno.test("runner: ctx.exists returns true for existing key", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "exister" as any,
    input: { key: "greeting" },
    metadata: null,
  });

  assertEquals(result.output, { key: "greeting", exists: true });
});

Deno.test("runner: ctx.exists returns false for missing key", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "exister" as any,
    input: { key: "nonexistent" },
    metadata: null,
  });

  assertEquals(result.output, { key: "nonexistent", exists: false });
});

Deno.test("runner: worker error propagates", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  await assertRejects(
    () =>
      runner.execute({
        from: "app/a" as any,
        to: "failer" as any,
        input: null,
        metadata: null,
      }),
    Error,
    "intentional failure",
  );
});

Deno.test("runner: timeout releases contention and rejects", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store, 200);

  await assertRejects(
    () =>
      runner.execute({
        from: "app/a" as any,
        to: "hanger" as any,
        input: null,
        metadata: null,
      }),
    Error,
    "timed out",
  );

  // Contention must be released even on timeout
  assertEquals((store.getCache() as any).contentionCache.size, 0);
});

Deno.test("runner: derived event via ctx.call", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "caller" as any,
    input: { target: "hello", input: { ping: true } },
    metadata: null,
  });

  assertEquals(result.output, {
    called: "hello",
    result: { message: "hello", from: "app/a" },
  });
});

Deno.test("runner: worker writes are tracked in diffs", async () => {
  const store = await makeStore();
  await seed(store);
  const runner = await makeRunner(store);

  const result = await runner.execute({
    from: "app/a" as any,
    to: "writer" as any,
    input: { key: "count", value: 1 },
    metadata: null,
  });

  assertEquals(result.diffs.get("count"), "1");
  assertEquals(result.output, { written: "count" });
});
