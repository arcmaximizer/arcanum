import { Database } from "@db/sqlite";
import { Kysely } from "kysely";
import { assertEquals, assertExists, assertRejects } from "@std/assert";
import {
  createDAG,
  createDAGTables,
  dropDAGTables,
} from "../lib/dag_sqlite.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";

function createTestDb(): Kysely<any> {
  const db = new Database(":memory:");
  return new Kysely<any>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
}

Deno.test("createDAGTables creates nodes and edges tables", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");

  const nodes = await db.selectFrom("test_nodes").selectAll().execute();
  const edges = await db.selectFrom("test_edges").selectAll().execute();

  assertEquals(nodes, []);
  assertEquals(edges, []);

  await db.destroy();
});

Deno.test("dropDAGTables removes tables", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  await dropDAGTables(db, "test");

  const tables = await db.selectFrom("sqlite_master")
    .select("name")
    .where("type", "=", "table")
    .where("name", "like", "test_%")
    .execute();

  assertEquals(tables.length, 0);
  await db.destroy();
});

Deno.test("addNode inserts node", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addNode("a");

  const exists = await dag.getNode("a");
  assertEquals(exists, true);

  await db.destroy();
});

Deno.test("addNode is idempotent", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addNode("a");
  await dag.addNode("a"); // should not throw

  const nodes = await dag.nodes();
  assertEquals(nodes, ["a"]);

  await db.destroy();
});

Deno.test("getNode returns false for non-existent node", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  const exists = await dag.getNode("nonexistent");
  assertEquals(exists, false);

  await db.destroy();
});

Deno.test("addEdge creates nodes and edge", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b"); // a -> b (b depends on a)

  const nodes = await dag.nodes();
  assertEquals(nodes.sort(), ["a", "b"]);

  const roots = await dag.roots();
  assertEquals(roots, ["a"]);

  await db.destroy();
});

Deno.test("addEdge is idempotent", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("a", "b"); // should not throw

  const sorted = await dag.topologicalSort();
  // Post-order DFS: b comes after a
  assertEquals(sorted.includes("a"), true);
  assertEquals(sorted.includes("b"), true);
  assertEquals(sorted.indexOf("a") > sorted.indexOf("b"), true);

  await db.destroy();
});

Deno.test("addEdge rejects cycle", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("b", "c");

  await assertRejects(
    () => dag.addEdge("c", "a"),
    Error,
    "would create a cycle",
  );

  await db.destroy();
});

Deno.test("addEdge rejects self-loop", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await assertRejects(
    () => dag.addEdge("a", "a"),
    Error,
    "would create a cycle",
  );

  await db.destroy();
});

Deno.test("nodes returns all node ids", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addNode("a");
  await dag.addNode("b");
  await dag.addNode("c");

  const nodes = await dag.nodes();
  assertEquals(nodes.sort(), ["a", "b", "c"]);

  await db.destroy();
});

Deno.test("roots returns nodes with no incoming edges", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("a", "c");
  await dag.addEdge("b", "d");
  await dag.addEdge("c", "d");

  const roots = await dag.roots();
  assertEquals(roots, ["a"]);

  await db.destroy();
});

Deno.test("roots returns all nodes if no edges", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addNode("a");
  await dag.addNode("b");

  const roots = await dag.roots();
  assertEquals(roots.sort(), ["a", "b"]);

  await db.destroy();
});

Deno.test("topologicalSort returns dependencies first", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("a", "c");
  await dag.addEdge("b", "d");
  await dag.addEdge("c", "d");

  const sorted = await dag.topologicalSort();

  // Post-order DFS: children come before parents
  // b and c come before a, d comes before b and c
  assertEquals(sorted.indexOf("a") > sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("a") > sorted.indexOf("c"), true);
  assertEquals(sorted.indexOf("b") > sorted.indexOf("d"), true);
  assertEquals(sorted.indexOf("c") > sorted.indexOf("d"), true);

  await db.destroy();
});

Deno.test("traverse visits nodes depth-first", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("a", "c");
  await dag.addEdge("b", "d");
  await dag.addEdge("c", "d");

  const visited: string[] = [];
  await dag.traverse((id) => visited.push(id), undefined);

  // Should traverse depth-first: a -> b -> d -> c -> d (d visited twice via two paths)
  assertEquals(visited[0], "a");
  assertEquals(visited.includes("b"), true);
  assertEquals(visited.includes("c"), true);
  assertEquals(visited.includes("d"), true);

  await db.destroy();
});

Deno.test("traverse provides correct state", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("a", "c");

  const states: { id: string; parent: string | null; depth: number }[] = [];
  await dag.traverse((id, state) => {
    states.push({ id, parent: state.parent, depth: state.depth });
  }, undefined);

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
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  await dag.addEdge("a", "b");
  await dag.addEdge("a", "c");

  const states: { id: string; index: number; total: number }[] = [];
  await dag.traverse((id, state) => {
    states.push({ id, index: state.index, total: state.total });
  }, undefined);

  const bState = states.find((s) => s.id === "b");
  const cState = states.find((s) => s.id === "c");

  assertExists(bState);
  assertExists(cState);
  assertEquals(bState!.index + cState!.index, 1); // one is 0, one is 1
  assertEquals(bState!.total, 2);
  assertEquals(cState!.total, 2);

  await db.destroy();
});

Deno.test("handles diamond dependency graph", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  //     a
  //    / \
  //   b   c
  //    \ /
  //     d
  await dag.addEdge("a", "b");
  await dag.addEdge("a", "c");
  await dag.addEdge("b", "d");
  await dag.addEdge("c", "d");

  const sorted = await dag.topologicalSort();

  // Post-order DFS: children before parents
  assertEquals(sorted.indexOf("a") > sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("a") > sorted.indexOf("c"), true);
  assertEquals(sorted.indexOf("b") > sorted.indexOf("d"), true);
  assertEquals(sorted.indexOf("c") > sorted.indexOf("d"), true);

  // d should appear twice in traversal (once per path)
  const visited: string[] = [];
  await dag.traverse((id) => visited.push(id), undefined);
  const dCount = visited.filter((id) => id === "d").length;
  assertEquals(dCount, 2);

  await db.destroy();
});

Deno.test("handles disconnected subgraphs", async () => {
  const db = createTestDb();
  await createDAGTables(db, "test");
  const dag = createDAG(db, "test");

  // Graph 1: a -> b
  // Graph 2: c -> d
  await dag.addEdge("a", "b");
  await dag.addEdge("c", "d");

  const roots = await dag.roots();
  assertEquals(roots.sort(), ["a", "c"]);

  const sorted = await dag.topologicalSort();
  assertEquals(sorted.length, 4);

  await db.destroy();
});
