import { Database } from "@db/sqlite";
import { Kysely, sql } from "kysely";
import { assertEquals, assertExists, assertRejects } from "@std/assert";
import {
  addChild,
  addNode,
  createCheckpoint,
  createTreeTables,
  dropTreeTables,
  get,
  getHead,
  getMany,
  getNode,
  getParent,
  nodes,
  setHead,
  topologicalSort,
  traverse,
  traverseFrom,
  traverseState,
} from "../svc/store.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import type { TreeDatabase } from "../svc/store.ts";

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
  const kvWritesTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'kv_writes'
  `.execute(db);
  const headsTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'heads'
  `.execute(db);

  assertEquals(nodesTable.rows.length, 1);
  assertEquals(checkpointsTable.rows.length, 1);
  assertEquals(kvWritesTable.rows.length, 1);
  assertEquals(headsTable.rows.length, 1);

  await db.destroy();
});

Deno.test("dropTreeTables removes tables", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  await dropTreeTables(db);

  const tables = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('nodes', 'checkpoints', 'kv_writes', 'heads')
  `.execute(db);

  assertEquals(tables.rows.length, 0);
  await db.destroy();
});

Deno.test("addNode inserts node", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");

  const exists = await getNode(db, "a");
  assertEquals(exists, true);

  await db.destroy();
});

Deno.test("addNode is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await addNode(db, "a");

  const allNodes = await nodes(db);
  assertEquals(allNodes, ["a"]);

  await db.destroy();
});

Deno.test("getNode returns false for non-existent node", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  const exists = await getNode(db, "nonexistent");
  assertEquals(exists, false);

  await db.destroy();
});

Deno.test("setHead sets head", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");

  const head = await getHead(db);
  assertEquals(head, "a");

  const allNodes = await nodes(db);
  assertEquals(allNodes, ["a"]);

  await db.destroy();
});

Deno.test("setHead is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await setHead(db, "a");
  await setHead(db, "a");

  const head = await getHead(db);
  assertEquals(head, "a");

  await db.destroy();
});

Deno.test("addChild creates nodes and edge", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");

  const allNodes = await nodes(db);
  assertEquals(allNodes.sort(), ["a", "b"]);

  const parent = await getParent(db, "b");
  assertEquals(parent, "a");

  await db.destroy();
});

Deno.test("addChild is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "b");

  const sorted = await topologicalSort(db);
  assertEquals(sorted, ["a", "b"]);

  await db.destroy();
});

Deno.test("addChild rejects adding second parent to child", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");

  await assertRejects(
    () => addChild(db, "c", "b"),
    Error,
    "already has a parent",
  );

  const parent = await getParent(db, "b");
  assertEquals(parent, "a");

  await db.destroy();
});

Deno.test("nodes returns all node ids", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addNode(db, "b");
  await addNode(db, "c");

  const allNodes = await nodes(db);
  assertEquals(allNodes.sort(), ["a", "b", "c"]);

  await db.destroy();
});

Deno.test("heads returns the head node", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");
  await addChild(db, "b", "d");

  const headId = await getHead(db);
  assertEquals(headId, "a");

  await db.destroy();
});

Deno.test("topologicalSort returns dependencies first", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");
  await addChild(db, "b", "d");

  const sorted = await topologicalSort(db);

  assertEquals(sorted.indexOf("a") < sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("b") < sorted.indexOf("d"), true);

  await db.destroy();
});

Deno.test("traverse visits nodes depth-first", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");
  await addChild(db, "b", "d");

  const visited: string[] = [];
  await traverse(db, (id: string) => visited.push(id), undefined);

  assertEquals(visited[0], "a");
  assertEquals(visited.includes("b"), true);
  assertEquals(visited.includes("c"), true);
  assertEquals(visited.includes("d"), true);

  await db.destroy();
});

Deno.test("traverse provides correct state", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");

  const states: { id: string; parent: string | null; depth: number }[] = [];
  await traverse(
    db,
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

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");

  const states: { id: string; index: number; total: number }[] = [];
  await traverse(db, (id: string, state: { index: number; total: number }) => {
    states.push({ id, index: state.index, total: state.total });
  }, undefined);

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

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");
  await addChild(db, "b", "d");
  await addChild(db, "c", "e");

  const sorted = await topologicalSort(db);

  assertEquals(sorted.indexOf("a") < sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("b") < sorted.indexOf("d"), true);
  assertEquals(sorted.indexOf("c") < sorted.indexOf("e"), true);

  const visited: string[] = [];
  await traverse(db, (id: string) => visited.push(id), undefined);

  assertEquals(visited.length, 5);
  assertEquals(visited.filter((id) => id === "d").length, 1);

  await db.destroy();
});

Deno.test("traverseFrom visits descendants of specific node", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");
  await addChild(db, "a", "c");
  await addChild(db, "b", "d");
  await addChild(db, "c", "e");

  const visited: string[] = [];
  await traverseFrom(db, "a", (id: string) => visited.push(id), undefined);

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

  await addNode(db, "a");
  await setHead(db, "a");
  await addChild(db, "a", "b");

  const depths: number[] = [];
  await traverseFrom(db, "a", (_id: string, state: { depth: number }) => {
    depths.push(state.depth);
  }, undefined);

  assertEquals(depths, [0, 1]);

  await db.destroy();
});

Deno.test("topologicalSort returns empty when no root nodes", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  const sorted = await topologicalSort(db);
  assertEquals(sorted, []);

  await db.destroy();
});

Deno.test("traverse returns early when no head", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  const visited: string[] = [];
  await traverse(db, (id: string) => visited.push(id), undefined);
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

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));

  const head = await getHead(db);
  assertEquals(head, "e1");

  const value = await get(db, "foo");
  assertEquals(value, "bar");

  await db.destroy();
});

Deno.test("addNode with kvDiffs updates head", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["a", "1"]]));
  await addNode(db, "e2", "e1", new Map([["b", "2"]]));

  const head = await getHead(db);
  assertEquals(head, "e2");

  const a = await get(db, "a");
  assertEquals(a, "1");

  const b = await get(db, "b");
  assertEquals(b, "2");

  await db.destroy();
});

Deno.test("get returns null for non-existent key", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));

  const value = await get(db, "nonexistent");
  assertEquals(value, null);

  await db.destroy();
});

Deno.test("get reads at specific event", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));
  await addNode(db, "e2", "e1", new Map([["foo", "baz"]]));
  await addNode(db, "e3", "e2", new Map([["foo", "qux"]]));

  const v1 = await get(db, "foo", "e1");
  assertEquals(v1, "bar");

  const v2 = await get(db, "foo", "e2");
  assertEquals(v2, "baz");

  const v3 = await get(db, "foo", "e3");
  assertEquals(v3, "qux");

  await db.destroy();
});

Deno.test("get reads current state when no event specified", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));
  await addNode(db, "e2", "e1", new Map([["foo", "baz"]]));

  const value = await get(db, "foo");
  assertEquals(value, "baz");

  await db.destroy();
});

Deno.test("getMany returns multiple keys", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(
    db,
    "e1",
    undefined,
    new Map([
      ["foo", "bar"],
      ["baz", "qux"],
    ]),
  );

  const result = await getMany(db, ["foo", "baz", "missing"]);
  assertEquals(result.get("foo"), "bar");
  assertEquals(result.get("baz"), "qux");
  assertEquals(result.get("missing"), null);

  await db.destroy();
});

Deno.test("createCheckpoint creates checkpoint and updates node", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));
  await addNode(db, "e2", "e1", new Map([["foo", "baz"]]));

  const checkpointId = await createCheckpoint(db, "e2");

  const value = await get(db, "foo", "e2");
  assertEquals(value, "baz");

  await db.destroy();
});

Deno.test("createCheckpoint on event with no prior checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));

  const checkpointId = await createCheckpoint(db, "e1");
  assertExists(checkpointId);

  const value = await get(db, "foo", "e1");
  assertEquals(value, "bar");

  await db.destroy();
});

Deno.test("get reads value from checkpoint + diffs", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(
    db,
    "e1",
    undefined,
    new Map([
      ["a", "1"],
      ["b", "2"],
    ]),
  );

  await createCheckpoint(db, "e1");

  await addNode(db, "e2", "e1", new Map([["b", "3"]]));
  await addNode(db, "e3", "e2", new Map([["c", "4"]]));

  const a = await get(db, "a", "e3");
  assertEquals(a, "1");

  const b = await get(db, "b", "e3");
  assertEquals(b, "3");

  const c = await get(db, "c", "e3");
  assertEquals(c, "4");

  await db.destroy();
});

Deno.test("traverseState yields all state at event", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(
    db,
    "e1",
    undefined,
    new Map([
      ["foo", "bar"],
      ["baz", "qux"],
    ]),
  );

  await createCheckpoint(db, "e1");

  await addNode(
    db,
    "e2",
    "e1",
    new Map([
      ["foo", "updated"],
      ["new", "key"],
    ]),
  );

  const state: Array<[string, string | null]> = [];
  for await (const [key, value] of traverseState(db, "e2")) {
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

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));
  await createCheckpoint(db, "e1");

  await addNode(db, "e2", "e1", new Map([["foo", "branch1"]]));
  await addNode(db, "e3", "e1", new Map([["foo", "branch2"]]));

  const v2 = await get(db, "foo", "e2");
  assertEquals(v2, "branch1");

  const v3 = await get(db, "foo", "e3");
  assertEquals(v3, "branch2");

  await db.destroy();
});

Deno.test("delete key with null value", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  await addNode(db, "e1", undefined, new Map([["foo", "bar"]]));
  await addNode(db, "e2", "e1", new Map([["foo", null]]));

  const v1 = await get(db, "foo", "e1");
  assertEquals(v1, "bar");

  const v2 = await get(db, "foo", "e2");
  assertEquals(v2, null);

  await db.destroy();
});

Deno.test("get returns null when no events exist", async () => {
  const db = createTestDb();
  await createTreeTables(db);

  const value = await get(db, "foo");
  assertEquals(value, null);

  await db.destroy();
});
