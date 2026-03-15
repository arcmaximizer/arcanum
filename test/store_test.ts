import { Database } from "@db/sqlite";
import { Kysely, sql } from "kysely";
import { assertEquals, assertExists, assertRejects } from "@std/assert";
import {
  createTreeTables,
  dropTreeTables,
  SqliteTreeStore,
  TreeStore,
} from "../svc/store.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import type { TreeDatabase, CacheService } from "../svc/store.ts";
import { InMemoryCacheService } from "../svc/store.ts";

function createTestDb(): Kysely<TreeDatabase> {
  const db = new Database(":memory:");
  return new Kysely<TreeDatabase>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
}

Deno.test("createTreeTables creates nodes, checkpoints, kv_writes, and heads tables", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  const nodesTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'nodes'
  `.execute(db);
  const checkpointsTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'checkpoints'
  `.execute(db);
  const checkpointStateTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'checkpoint_state'
  `.execute(db);
  const kvWritesTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'kv_writes'
  `.execute(db);
  const headsTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'heads'
  `.execute(db);

  assertEquals(nodesTable.rows.length, 1);
  assertEquals(checkpointsTable.rows.length, 1);
  assertEquals(checkpointStateTable.rows.length, 1);
  assertEquals(kvWritesTable.rows.length, 1);
  assertEquals(headsTable.rows.length, 1);

  await db.destroy();
});

Deno.test("dropTreeTables removes tables", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  await dropTreeTables(db);

  const tables = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('nodes', 'checkpoints', 'checkpoint_state', 'kv_writes', 'heads')
  `.execute(db);

  assertEquals(tables.rows.length, 0);
  await db.destroy();
});

Deno.test("addNode inserts node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");

  const exists = await store.getNode("a");
  assertEquals(exists, true);

  await db.destroy();
});

Deno.test("addNode is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.addNode("a");

  const allNodes = await store.nodes();
  assertEquals(allNodes, ["a"]);

  await db.destroy();
});

Deno.test("getNode returns false for non-existent node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  const exists = await store.getNode("nonexistent");
  assertEquals(exists, false);

  await db.destroy();
});

Deno.test("setHead sets head", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");

  const head = await store.getHead("main");
  assertEquals(head, "a");

  const allNodes = await store.nodes();
  assertEquals(allNodes, ["a"]);

  await db.destroy();
});

Deno.test("setHead is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.setHead("a", "main");
  await store.setHead("a", "main");

  const head = await store.getHead("main");
  assertEquals(head, "a");

  await db.destroy();
});

Deno.test("addChild creates nodes and edge", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");

  const allNodes = await store.nodes();
  assertEquals(allNodes.sort(), ["a", "b"]);

  const parent = await store.getParent("b");
  assertEquals(parent, "a");

  await db.destroy();
});

Deno.test("addChild is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "b");

  const sorted = await store.topologicalSort();
  assertEquals(sorted, ["a", "b"]);

  await db.destroy();
});

Deno.test("addChild rejects adding second parent to child", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");

  await assertRejects(
    () => store.addChild("c", "b"),
    Error,
    "already has a parent",
  );

  const parent = await store.getParent("b");
  assertEquals(parent, "a");

  await db.destroy();
});

Deno.test("nodes returns all node ids", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addNode("b");
  await store.addNode("c");

  const allNodes = await store.nodes();
  assertEquals(allNodes.sort(), ["a", "b", "c"]);

  await db.destroy();
});

Deno.test("heads returns the head node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");
  await store.addChild("b", "d");

  const headId = await store.getHead("main");
  assertEquals(headId, "a");

  await db.destroy();
});

Deno.test("topologicalSort returns dependencies first", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");
  await store.addChild("b", "d");

  const sorted = await store.topologicalSort();

  assertEquals(sorted.indexOf("a") < sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("b") < sorted.indexOf("d"), true);

  await db.destroy();
});

Deno.test("traverse visits nodes depth-first", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");
  await store.addChild("b", "d");

  const visited: string[] = [];
  await store.traverse((id: string) => visited.push(id), undefined);

  assertEquals(visited[0], "a");
  assertEquals(visited.includes("b"), true);
  assertEquals(visited.includes("c"), true);
  assertEquals(visited.includes("d"), true);

  await db.destroy();
});

Deno.test("traverse provides correct state", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");

  const states: { id: string; parent: string | null; depth: number }[] = [];
  await store.traverse(
    (id: string, state: { parent: string | null; depth: number }) => {
      states.push({ id, parent: state.parent, depth: state.depth });
    },
    undefined,
  );

  const aState = states.find((s) => s.id === "a");
  assertExists(aState);
  assertEquals(aState!.parent, null);
  assertEquals(aState!.depth, 0);

  const bState = states.find((s) => s.id === "b");
  assertExists(bState);
  assertEquals(bState!.parent, "a");
  assertEquals(bState!.depth, 1);

  await db.destroy();
});

Deno.test("traverse tracks index and total correctly", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");

  const states: { id: string; index: number; total: number }[] = [];
  await store.traverse(
    (id: string, state: { index: number; total: number }) => {
      states.push({ id, index: state.index, total: state.total });
    },
    undefined,
  );

  const bState = states.find((s) => s.id === "b");
  const cState = states.find((s) => s.id === "c");

  assertExists(bState);
  assertExists(cState);
  assertEquals(bState!.index + cState!.index, 1);
  assertEquals(bState!.total, 2);
  assertEquals(cState!.total, 2);

  await db.destroy();
});

Deno.test("handles tree with branching", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");
  await store.addChild("b", "d");
  await store.addChild("c", "e");

  const sorted = await store.topologicalSort();

  assertEquals(sorted.indexOf("a") < sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("b") < sorted.indexOf("d"), true);
  assertEquals(sorted.indexOf("c") < sorted.indexOf("e"), true);

  const visited: string[] = [];
  await store.traverse((id: string) => visited.push(id), undefined);

  assertEquals(visited.length, 5);
  assertEquals(visited.filter((id) => id === "d").length, 1);

  await db.destroy();
});

Deno.test("traverseFrom visits descendants of specific node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");
  await store.addChild("b", "d");
  await store.addChild("c", "e");

  const visited: string[] = [];
  await store.traverseFrom("a", (id: string) => visited.push(id), undefined);

  assertEquals(visited.includes("a"), true);
  assertEquals(visited.includes("b"), true);
  assertEquals(visited.includes("c"), true);
  assertEquals(visited.includes("d"), true);
  assertEquals(visited.includes("e"), true);

  await db.destroy();
});

Deno.test("traverseFrom with context", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");

  const depths: number[] = [];
  await store.traverseFrom("a", (_id: string, state: { depth: number }) => {
    depths.push(state.depth);
  }, undefined);

  assertEquals(depths, [0, 1]);

  await db.destroy();
});

Deno.test("topologicalSort returns empty when no root nodes", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  const sorted = await store.topologicalSort();
  assertEquals(sorted, []);

  await db.destroy();
});

Deno.test("traverse returns early when no head", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  const visited: string[] = [];
  await store.traverse((id: string) => visited.push(id), undefined);
  assertEquals(visited, []);

  await db.destroy();
});

async function heads(
  trx: Kysely<TreeDatabase>,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("heads")
    .select("id")
    .execute();
  return rows.map((r) => r.id);
}

Deno.test("addNode with kvDiffs stores writes", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));

  const head = await store.getHead("main");
  assertEquals(head, "e1");

  const value = await store.get("foo");
  assertEquals(value, "bar");

  await db.destroy();
});

Deno.test("addNode with kvDiffs updates head", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  await store.addNode("e2", "e1", new Map([["b", "2"]]));

  const head = await store.getHead("main");
  assertEquals(head, "e2");

  const a = await store.get("a");
  assertEquals(a, "1");

  const b = await store.get("b");
  assertEquals(b, "2");

  await db.destroy();
});

Deno.test("get returns null for non-existent key", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));

  const value = await store.get("nonexistent");
  assertEquals(value, null);

  await db.destroy();
});

Deno.test("get reads at specific event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.addNode("e2", "e1", new Map([["foo", "baz"]]));
  await store.addNode("e3", "e2", new Map([["foo", "qux"]]));

  const v1 = await store.get("foo", "e1");
  assertEquals(v1, "bar");

  const v2 = await store.get("foo", "e2");
  assertEquals(v2, "baz");

  const v3 = await store.get("foo", "e3");
  assertEquals(v3, "qux");

  await db.destroy();
});

Deno.test("get reads current state when no event specified", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.addNode("e2", "e1", new Map([["foo", "baz"]]));

  const value = await store.get("foo");
  assertEquals(value, "baz");

  await db.destroy();
});

Deno.test("getMany returns multiple keys", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode(
    "e1",
    undefined,
    new Map([
      ["foo", "bar"],
      ["baz", "qux"],
    ]),
  );

  const result = await store.getMany(["foo", "baz", "missing"]);
  assertEquals(result.get("foo"), "bar");
  assertEquals(result.get("baz"), "qux");
  assertEquals(result.get("missing"), null);

  await db.destroy();
});

Deno.test("createCheckpoint creates checkpoint and updates node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.addNode("e2", "e1", new Map([["foo", "baz"]]));

  const checkpointId = await store.createCheckpoint("e2");

  const checkpointRows = await db
    .selectFrom("checkpoint_state")
    .selectAll()
    .where("checkpoint_id", "=", checkpointId)
    .execute();

  const value = await store.get("foo", "e2");
  assertEquals(value, "baz");
  assertEquals(checkpointRows.length, 1);
  assertEquals(checkpointRows[0]?.key, "foo");
  assertEquals(checkpointRows[0]?.value, "baz");

  await db.destroy();
});

Deno.test("checkpoint optimization uses materialized checkpoint state", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"], ["b", "2"]]));
  await store.addNode("e2", "e1", new Map([["b", "3"], ["c", "4"]]));
  await store.addNode("e3", "e2", new Map([["d", "5"]]));

  const checkpointId = await store.createCheckpoint("e2");

  store.resetStateBuildStats();
  const value = await store.get("d", "e3");
  const stats = store.getStateBuildStats();

  const checkpointRows = await db
    .selectFrom("checkpoint_state")
    .selectAll()
    .where("checkpoint_id", "=", checkpointId)
    .execute();

  assertEquals(value, "5");
  assertEquals(checkpointRows.length, 3);
  assertEquals(stats.checkpointHits, 1);
  assertEquals(stats.fullRebuilds, 0);
  assertEquals(stats.lineageEventsApplied, 1);

  await db.destroy();
});

Deno.test("cache instrumentation reports cache hit after warm read", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  store.resetStateBuildStats();
  assertEquals(await store.get("a", "e1"), "1");
  const coldStats = store.getStateBuildStats();

  store.resetStateBuildStats();
  assertEquals(await store.get("a", "e1"), "1");
  const warmStats = store.getStateBuildStats();

  assertEquals(coldStats.cachedStateHits, 0);
  assertEquals(warmStats.cachedStateHits, 1);

  await db.destroy();
});

Deno.test("createCheckpoint on event with no prior checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));

  const checkpointId = await store.createCheckpoint("e1");
  assertExists(checkpointId);

  const value = await store.get("foo", "e1");
  assertEquals(value, "bar");

  await db.destroy();
});

Deno.test("get reads value from checkpoint + diffs", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode(
    "e1",
    undefined,
    new Map([
      ["a", "1"],
      ["b", "2"],
    ]),
  );

  await store.createCheckpoint("e1");

  await store.addNode("e2", "e1", new Map([["b", "3"]]));
  await store.addNode("e3", "e2", new Map([["c", "4"]]));

  const a = await store.get("a", "e3");
  assertEquals(a, "1");

  const b = await store.get("b", "e3");
  assertEquals(b, "3");

  const c = await store.get("c", "e3");
  assertEquals(c, "4");

  await db.destroy();
});

Deno.test("traverseState yields all state at event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode(
    "e1",
    undefined,
    new Map([
      ["foo", "bar"],
      ["baz", "qux"],
    ]),
  );

  await store.createCheckpoint("e1");

  await store.addNode(
    "e2",
    "e1",
    new Map([
      ["foo", "updated"],
      ["new", "key"],
    ]),
  );

  const state: Array<[string, string | null]> = [];
  for await (const [key, value] of store.traverseState("e2")) {
    state.push([key, value]);
  }

  const stateMap = new Map(state);
  assertEquals(stateMap.get("foo"), "updated");
  assertEquals(stateMap.get("baz"), "qux");
  assertEquals(stateMap.get("new"), "key");

  await db.destroy();
});

Deno.test("KV writes at different branches are isolated", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.createCheckpoint("e1");

  await store.addNode("e2", "e1", new Map([["foo", "branch1"]]));
  await store.addNode("e3", "e1", new Map([["foo", "branch2"]]));

  const v2 = await store.get("foo", "e2");
  assertEquals(v2, "branch1");

  const v3 = await store.get("foo", "e3");
  assertEquals(v3, "branch2");

  await db.destroy();
});

Deno.test("delete key with null value", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.addNode("e2", "e1", new Map([["foo", null]]));

  const v1 = await store.get("foo", "e1");
  assertEquals(v1, "bar");

  const v2 = await store.get("foo", "e2");
  assertEquals(v2, null);

  await db.destroy();
});

Deno.test("get returns null when no events exist", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  const value = await store.get("foo");
  assertEquals(value, null);

  await db.destroy();
});

Deno.test("getHeads returns all heads", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "tree1");
  await store.addNode("b");
  await store.setHead("b", "tree2");

  const heads = await store.getHeads();
  assertEquals(heads.get("tree1"), "a");
  assertEquals(heads.get("tree2"), "b");
  assertEquals(heads.get("main"), "b");

  await db.destroy();
});

Deno.test("getChildren returns direct children", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a");
  await store.setHead("a", "main");
  await store.addChild("a", "b");
  await store.addChild("a", "c");

  const children = await store.getChildren("a");
  assertEquals(children.sort(), ["b", "c"]);

  const leafChildren = await store.getChildren("b");
  assertEquals(leafChildren, []);

  await db.destroy();
});

Deno.test("addNode with base parameter", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("a", undefined, new Map([["foo", "bar"]]), undefined, "base-a");

  const exists = await store.getNode("a");
  assertEquals(exists, true);

  await db.destroy();
});

Deno.test("createCheckpoint returns existing checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  const checkpoint1 = await store.createCheckpoint("e1");
  const checkpoint2 = await store.createCheckpoint("e1");

  assertEquals(checkpoint1, checkpoint2);

  await db.destroy();
});

Deno.test("traverseState without eventId uses current head", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.createCheckpoint("e1");
  await store.addNode("e2", "e1", new Map([["foo", "baz"]]));

  const state: Array<[string, string | null]> = [];
  for await (const [key, value] of store.traverseState()) {
    state.push([key, value]);
  }

  const stateMap = new Map(state);
  assertEquals(stateMap.get("foo"), "baz");

  await db.destroy();
});

Deno.test("cache service - addContention and removeContention", () => {
  const cache: CacheService = new InMemoryCacheService();

  cache.addContention("event1");
  cache.addContention("event1");
  cache.removeContention("event1");
  cache.removeContention("event1");
});

Deno.test("cache service - cacheState and getCachedState", () => {
  const cache: CacheService = new InMemoryCacheService();

  const state = new Map([["foo", "bar"]]);
  cache.cacheState("event1", state);

  const cached = cache.getCachedState("event1");
  assertEquals(cached?.get("foo"), "bar");

  const notCached = cache.getCachedState("event2");
  assertEquals(notCached, undefined);
});

Deno.test("cache service - ref counting and eviction", () => {
  const cache: CacheService = new InMemoryCacheService();

  const state = new Map([["foo", "bar"]]);
  cache.cacheState("event1", state);
  cache.incrementRefCount("event1");
  cache.incrementRefCount("event1");
  cache.decrementRefCount("event1");
  cache.decrementRefCount("event1");

  const cached = cache.getCachedState("event1");
  assertEquals(cached, undefined);
});

Deno.test("cache service - clear", () => {
  const cache: CacheService = new InMemoryCacheService();

  cache.cacheState("event1", new Map([["foo", "bar"]]));
  cache.addContention("event1");
  cache.incrementRefCount("event1");

  cache.clear();

  assertEquals(cache.getCachedState("event1"), undefined);
});
