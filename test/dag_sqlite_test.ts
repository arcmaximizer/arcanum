import { Database } from "@db/sqlite";
import { Kysely, sql } from "kysely";
import { assertEquals, assertExists, assertRejects } from "@std/assert";
import {
  addEdge,
  addNode,
  createDAGTables,
  dropDAGTables,
  getNode,
  nodes,
  roots,
  topologicalSort,
  traverse,
  traverseFrom,
} from "../lib/dag_sqlite.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import type { DAGDatabase } from "../lib/dag_sqlite.ts";

function createTestDb(): Kysely<DAGDatabase> {
  const db = new Database(":memory:");
  return new Kysely<DAGDatabase>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
}

Deno.test("createDAGTables creates nodes and edges tables", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  const nodesTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'nodes'
  `.execute(db);
  const edgesTable = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'edges'
  `.execute(db);

  assertEquals(nodesTable.rows.length, 1);
  assertEquals(edgesTable.rows.length, 1);

  await db.destroy();
});

Deno.test("dropDAGTables removes tables", async () => {
  const db = createTestDb();
  await createDAGTables(db);
  await dropDAGTables(db);

  const tables = await sql<{ name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('nodes', 'edges')
  `.execute(db);

  assertEquals(tables.rows.length, 0);
  await db.destroy();
});

Deno.test("addNode inserts node", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addNode(db, "a");

  const exists = await getNode(db, "a");
  assertEquals(exists, true);

  await db.destroy();
});

Deno.test("addNode is idempotent", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addNode(db, "a");
  await addNode(db, "a");

  const allNodes = await nodes(db);
  assertEquals(allNodes, ["a"]);

  await db.destroy();
});

Deno.test("getNode returns false for non-existent node", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  const exists = await getNode(db, "nonexistent");
  assertEquals(exists, false);

  await db.destroy();
});

Deno.test("addEdge creates nodes and edge", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");

  const allNodes = await nodes(db);
  assertEquals(allNodes.sort(), ["a", "b"]);

  const rootIds = await roots(db);
  assertEquals(rootIds, ["a"]);

  await db.destroy();
});

Deno.test("addEdge is idempotent", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "b");

  const sorted = await topologicalSort(db);
  assertEquals(sorted.includes("a"), true);
  assertEquals(sorted.includes("b"), true);
  assertEquals(sorted.indexOf("a") > sorted.indexOf("b"), true);

  await db.destroy();
});

Deno.test("addEdge rejects self-loop", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await assertRejects(
    () => addEdge(db, "a", "a"),
    Error,
    "would create a cycle",
  );

  await db.destroy();
});

Deno.test("nodes returns all node ids", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addNode(db, "a");
  await addNode(db, "b");
  await addNode(db, "c");

  const allNodes = await nodes(db);
  assertEquals(allNodes.sort(), ["a", "b", "c"]);

  await db.destroy();
});

Deno.test("roots returns nodes with no incoming edges", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");
  await addEdge(db, "b", "d");
  await addEdge(db, "c", "d");

  const rootIds = await roots(db);
  assertEquals(rootIds, ["a"]);

  await db.destroy();
});

Deno.test("roots returns all nodes if no edges", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addNode(db, "a");
  await addNode(db, "b");

  const rootIds = await roots(db);
  assertEquals(rootIds.sort(), ["a", "b"]);

  await db.destroy();
});

Deno.test("topologicalSort returns dependencies first", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");
  await addEdge(db, "b", "d");
  await addEdge(db, "c", "d");

  const sorted = await topologicalSort(db);

  assertEquals(sorted.indexOf("a") > sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("a") > sorted.indexOf("c"), true);
  assertEquals(sorted.indexOf("b") > sorted.indexOf("d"), true);
  assertEquals(sorted.indexOf("c") > sorted.indexOf("d"), true);

  await db.destroy();
});

Deno.test("traverse visits nodes depth-first", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");
  await addEdge(db, "b", "d");
  await addEdge(db, "c", "d");

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
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");

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
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");

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

Deno.test("handles diamond dependency graph", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");
  await addEdge(db, "b", "d");
  await addEdge(db, "c", "d");

  const sorted = await topologicalSort(db);

  assertEquals(sorted.indexOf("a") > sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("a") > sorted.indexOf("c"), true);
  assertEquals(sorted.indexOf("b") > sorted.indexOf("d"), true);
  assertEquals(sorted.indexOf("c") > sorted.indexOf("d"), true);

  const visited: string[] = [];
  await traverse(db, (id: string) => visited.push(id), undefined);
  const dCount = visited.filter((id) => id === "d").length;
  assertEquals(dCount, 2);

  await db.destroy();
});

Deno.test("handles disconnected subgraphs", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "c", "d");

  const rootIds = await roots(db);
  assertEquals(rootIds.sort(), ["a", "c"]);

  const sorted = await topologicalSort(db);
  assertEquals(sorted.length, 4);

  await db.destroy();
});

Deno.test("traverseFrom visits descendants of specific node", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");
  await addEdge(db, "a", "c");
  await addEdge(db, "b", "d");
  await addEdge(db, "c", "d");

  const visited: string[] = [];
  await traverseFrom(db, "a", (id: string) => visited.push(id), undefined);

  assertEquals(visited.includes("a"), true);
  assertEquals(visited.includes("b"), true);
  assertEquals(visited.includes("c"), true);
  assertEquals(visited.includes("d"), true);

  await db.destroy();
});

Deno.test("traverseFrom with context", async () => {
  const db = createTestDb();
  await createDAGTables(db);

  await addEdge(db, "a", "b");

  const depths: number[] = [];
  await traverseFrom(db, "a", (_id: string, state: { depth: number }) => {
    depths.push(state.depth);
  }, undefined);

  assertEquals(depths, [0, 1]);

  await db.destroy();
});
