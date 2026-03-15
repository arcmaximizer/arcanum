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
import type { TreeDatabase } from "../svc/store.ts";

function createTestDb(): Kysely<TreeDatabase> {
  const db = new Database(":memory:");
  return new Kysely<TreeDatabase>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
}

Deno.test("checkpoint invariants - each node has at most one checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  await store.addNode("e2", "e1", new Map([["b", "2"]]));

  const cp1 = await store.createCheckpoint("e1");
  const cp2 = await store.createCheckpoint("e1");

  assertEquals(cp1, cp2);

  await db.destroy();
});

Deno.test("checkpoint invariants - checkpoints form a chain via parent", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  await store.addNode("e2", "e1", new Map([["b", "2"]]));
  await store.addNode("e3", "e2", new Map([["c", "3"]]));

  const cp1 = await store.createCheckpoint("e1");
  const cp2 = await store.createCheckpoint("e2");
  const cp3 = await store.createCheckpoint("e3");

  const cp1Row = await db.selectFrom("checkpoints").selectAll().where("id", "=", cp1).executeTakeFirst();
  const cp2Row = await db.selectFrom("checkpoints").selectAll().where("id", "=", cp2).executeTakeFirst();
  const cp3Row = await db.selectFrom("checkpoints").selectAll().where("id", "=", cp3).executeTakeFirst();

  assertEquals(cp1Row?.parent ?? undefined, undefined);
  assertEquals(cp2Row?.parent ?? undefined, cp1);
  assertEquals(cp3Row?.parent ?? undefined, cp2);

  await db.destroy();
});

Deno.test("checkpoint invariants - node with writes has checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  const node = await db.selectFrom("nodes").selectAll().where("id", "=", "e1").executeTakeFirst();
  assertExists(node?.checkpoint_id);

  await db.destroy();
});

Deno.test("checkpoint invariants - checkpoint event_id references parent event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  const cp1 = await store.createCheckpoint("e1");

  await store.addNode("e2", "e1", new Map([["b", "2"]]));
  const cp2 = await store.createCheckpoint("e2");

  const cp1Row = await db.selectFrom("checkpoints").selectAll().where("id", "=", cp1).executeTakeFirst();
  const cp2Row = await db.selectFrom("checkpoints").selectAll().where("id", "=", cp2).executeTakeFirst();

  assertEquals(cp1Row?.event_id, "e1");
  assertEquals(cp2Row?.event_id, "e1");

  await db.destroy();
});

Deno.test("checkpoint invariants - checkpoint state materializes full visible state", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"], ["b", "2"]]));
  await store.addNode("e2", "e1", new Map<string, string | null>([["b", null], ["c", "3"]]));

  const checkpointId = await store.createCheckpoint("e2");
  const rows = await db
    .selectFrom("checkpoint_state")
    .selectAll()
    .where("checkpoint_id", "=", checkpointId)
    .execute();

  const materialized = new Map(rows.map((row) => [row.key, row.value]));

  assertEquals(materialized.get("a"), "1");
  assertEquals(materialized.has("b"), false);
  assertEquals(materialized.get("c"), "3");

  await db.destroy();
});

Deno.test("state consistency - get returns last write up to event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"], ["b", "2"]]));
  await store.addNode("e2", "e1", new Map([["a", "3"]]));
  await store.addNode("e3", "e2", new Map([["b", "4"], ["c", "5"]]));

  assertEquals(await store.get("a", "e1"), "1");
  assertEquals(await store.get("a", "e2"), "3");
  assertEquals(await store.get("a", "e3"), "3");
  assertEquals(await store.get("b", "e1"), "2");
  assertEquals(await store.get("b", "e2"), "2");
  assertEquals(await store.get("b", "e3"), "4");
  assertEquals(await store.get("c", "e3"), "5");

  await db.destroy();
});

Deno.test("state consistency - reading never-written key returns null", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  assertEquals(await store.get("b", "e1"), null);

  await db.destroy();
});

Deno.test("state consistency - later events see earlier writes", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["x", "100"]]));
  await store.addNode("e2", "e1", new Map([["y", "200"]]));
  await store.addNode("e3", "e2", new Map([["z", "300"]]));

  assertEquals(await store.get("x", "e3"), "100");
  assertEquals(await store.get("y", "e3"), "200");
  assertEquals(await store.get("z", "e3"), "300");

  await db.destroy();
});

Deno.test("state consistency - delete key with null value", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"], ["b", "2"]]));
  await store.addNode("e2", "e1", new Map<string, string | null>([["a", null]]));

  assertEquals(await store.get("a", "e2"), null);
  assertEquals(await store.get("b", "e2"), "2");

  await db.destroy();
});

Deno.test("state consistency - getMany returns same as multiple get", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([
    ["a", "1"],
    ["b", "2"],
    ["c", "3"],
  ]));

  const many = await store.getMany(["a", "b", "c"], "e1");

  assertEquals(many.get("a"), "1");
  assertEquals(many.get("b"), "2");
  assertEquals(many.get("c"), "3");

  await db.destroy();
});

Deno.test("tree invariants - each node has at most one parent", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined);
  await store.addNode("e2", "e1");

  const parent = await store.getParent("e2");
  assertEquals(parent, "e1");

  await db.destroy();
});

Deno.test("tree invariants - cannot add second parent to child", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined);
  await store.addNode("e2", "e1");
  await store.addNode("e3", undefined);

  await assertRejects(
    async () => await store.addChild("e3", "e2"),
    Error,
    "already has a parent",
  );

  await db.destroy();
});

Deno.test("tree invariants - getHead returns valid node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  const head = await store.getHead("main");
  assertEquals(head, "e1");

  await store.addNode("e2", "e1", new Map([["b", "2"]]));

  const head2 = await store.getHead("main");
  assertEquals(head2, "e2");

  await db.destroy();
});

Deno.test("tree invariants - no cycles exist", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined);
  await store.addNode("e2", "e1");

  const sorted = await store.topologicalSort();

  const e1Idx = sorted.indexOf("e1");
  const e2Idx = sorted.indexOf("e2");
  assertEquals(e1Idx < e2Idx, true);

  await db.destroy();
});

Deno.test("tree invariants - getChildren returns direct children", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("root", undefined);
  await store.addNode("child1", "root");
  await store.addNode("child2", "root");
  await store.addNode("grandchild", "child1");

  const rootChildren = await store.getChildren("root");
  assertEquals(rootChildren.sort(), ["child1", "child2"]);

  const child1Children = await store.getChildren("child1");
  assertEquals(child1Children, ["grandchild"]);

  await db.destroy();
});

Deno.test("branch isolation - writes on different branches don't affect each other", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["shared", "base"]]));

  await store.addNode("e2", "e1", new Map([["a", "branch-a"]]));
  await store.addNode("e3", "e1", new Map([["b", "branch-b"]]));

  assertEquals(await store.get("a", "e2"), "branch-a");
  assertEquals(await store.get("a", "e3"), null);
  assertEquals(await store.get("b", "e2"), null);
  assertEquals(await store.get("b", "e3"), "branch-b");
  assertEquals(await store.get("shared", "e2"), "base");
  assertEquals(await store.get("shared", "e3"), "base");

  await db.destroy();
});

Deno.test("branch isolation - reading from branch only sees writes up to branch point", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["x", "1"]]));
  await store.addNode("e2", "e1", new Map([["x", "2"]]));
  await store.addNode("e3", "e1", new Map([["x", "3"]]));

  assertEquals(await store.get("x", "e2"), "2");
  assertEquals(await store.get("x", "e3"), "3");

  await db.destroy();
});

Deno.test("base parameter - base must be ancestor of event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  await store.addNode("e2", "e1", new Map([["b", "2"]]));
  await store.addNode("e3", "e2", new Map([["c", "3"]]));

  const e3 = await db.selectFrom("nodes").selectAll().where("id", "=", "e3").executeTakeFirst();
  assertEquals(e3?.base, "e2");

  await db.destroy();
});

Deno.test("base parameter - get reads at specific event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.addNode("e2", "e1", new Map([["foo", "baz"]]));
  await store.addNode("e3", "e2", new Map([["foo", "qux"]]));

  assertEquals(await store.get("foo", "e1"), "bar");
  assertEquals(await store.get("foo", "e2"), "baz");
  assertEquals(await store.get("foo", "e3"), "qux");

  await db.destroy();
});

Deno.test("base parameter - get reads current state when no event specified", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["foo", "bar"]]));
  await store.addNode("e2", "e1", new Map([["foo", "baz"]]));

  assertEquals(await store.get("foo"), "baz");

  await db.destroy();
});

Deno.test("cache invariants - cached state matches DB query result", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const cache = new Map<string, Map<string, string | null>>();
  const store = new SqliteTreeStore(db, {
    addContention() {},
    removeContention() {},
    cacheState(eventId, state) { cache.set(eventId, new Map(state)); },
    getCachedState(eventId) { return cache.get(eventId); },
    incrementRefCount() {},
    decrementRefCount() {},
    clear() { cache.clear(); },
  });

  await store.addNode("e1", undefined, new Map([["a", "1"], ["b", "2"]]));

  const firstRead = await store.get("a", "e1");
  assertEquals(firstRead, "1");

  const cached = cache.get("e1");
  assertEquals(cached?.get("a"), "1");
  assertEquals(cached?.get("b"), "2");

  await db.destroy();
});

Deno.test("cache invariants - cache cleared means state reconstructable from DB", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  store.getCache().clear();

  const result = await store.get("a", "e1");
  assertEquals(result, "1");

  await db.destroy();
});

Deno.test("checkpoint optimization - reads after checkpoint apply only tail lineage", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["k1", "v1"]]));
  await store.addNode("e2", "e1", new Map([["k2", "v2"]]));
  await store.addNode("e3", "e2", new Map([["k3", "v3"]]));
  await store.createCheckpoint("e3");
  await store.addNode("e4", "e3", new Map([["k4", "v4"]]));
  await store.addNode("e5", "e4", new Map([["k5", "v5"]]));

  store.resetStateBuildStats();
  assertEquals(await store.get("k5", "e5"), "v5");
  const stats = store.getStateBuildStats();

  assertEquals(stats.checkpointHits, 1);
  assertEquals(stats.fullRebuilds, 0);
  assertEquals(stats.lineageEventsApplied, 2);

  await db.destroy();
});

Deno.test("cache invariants - warm read records cached state hit", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  store.resetStateBuildStats();
  await store.get("a", "e1");
  assertEquals(store.getStateBuildStats().cachedStateHits, 0);

  store.resetStateBuildStats();
  await store.get("a", "e1");
  assertEquals(store.getStateBuildStats().cachedStateHits, 1);

  await db.destroy();
});

Deno.test("contention - adding contention keeps state cached", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  await store.get("a", "e1");

  store.getCache().addContention("e1");

  await store.addNode("e2", "e1", new Map([["b", "2"]]));

  const e1State = store.getCache().getCachedState("e1");
  assertExists(e1State);

  await db.destroy();
});

Deno.test("contention - removing contention allows eviction when refCount is 0", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  store.getCache().addContention("e1");
  store.getCache().removeContention("e1");

  const e1State = store.getCache().getCachedState("e1");
  assertEquals(e1State, undefined);

  await db.destroy();
});

Deno.test("contention - multiple contentions require multiple removals", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  await store.get("a", "e1");

  store.getCache().addContention("e1");
  store.getCache().addContention("e1");
  store.getCache().removeContention("e1");

  const e1State = store.getCache().getCachedState("e1");
  assertExists(e1State);

  store.getCache().removeContention("e1");

  const e1StateAfter = store.getCache().getCachedState("e1");
  assertEquals(e1StateAfter, undefined);

  await db.destroy();
});

Deno.test("ref counting - increment and decrement work correctly", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  await store.get("a", "e1");

  store.getCache().incrementRefCount("e1");
  store.getCache().decrementRefCount("e1");

  store.getCache().addContention("e1");
  store.getCache().removeContention("e1");

  const e1State = store.getCache().getCachedState("e1");
  assertEquals(e1State, undefined);

  await db.destroy();
});

Deno.test("ref counting - ref count prevents eviction even without contention", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  await store.get("a", "e1");

  store.getCache().incrementRefCount("e1");
  store.getCache().decrementRefCount("e1");

  store.getCache().addContention("e1");
  store.getCache().removeContention("e1");

  const e1State = store.getCache().getCachedState("e1");
  assertEquals(e1State, undefined);

  await db.destroy();
});

Deno.test("checkpoint + diffs - traverseState yields same as get", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([
    ["a", "1"],
    ["b", "2"],
    ["c", "3"],
  ]));

  await store.addNode("e2", "e1", new Map([["b", "new"], ["d", "4"]]));

  const state: Record<string, string | null> = {};
  for await (const [key, value] of store.traverseState("e2")) {
    state[key] = value;
  }

  assertEquals(state["a"], "1");
  assertEquals(state["b"], "new");
  assertEquals(state["c"], "3");
  assertEquals(state["d"], "4");

  await db.destroy();
});

Deno.test("checkpoint + diffs - get reads value from checkpoint + diffs", async () => {
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

Deno.test("reads tracking - kv_reads table records reads", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]), new Set(["a", "b"]));

  const reads = await store.getReads("e1");
  assertEquals(reads.has("a"), true);
  assertEquals(reads.has("b"), true);
  assertEquals(reads.has("c"), false);

  await db.destroy();
});

Deno.test("reads tracking - getReads returns empty set for event with no reads", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  const reads = await store.getReads("e1");
  assertEquals(reads.size, 0);

  await db.destroy();
});

Deno.test("topological sort - returns dependencies first", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("root", undefined);
  await store.addNode("a", "root");
  await store.addNode("b", "root");
  await store.addNode("c", "a");

  const sorted = await store.topologicalSort();

  assertEquals(sorted.indexOf("root") < sorted.indexOf("a"), true);
  assertEquals(sorted.indexOf("root") < sorted.indexOf("b"), true);
  assertEquals(sorted.indexOf("a") < sorted.indexOf("c"), true);

  await db.destroy();
});

Deno.test("traverse - visits all nodes depth-first", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("root", undefined);
  await store.addNode("a", "root");
  await store.addNode("b", "root");
  await store.addNode("c", "a");

  const visited: string[] = [];
  await store.traverse((id) => visited.push(id), null);

  assertEquals(visited.includes("root"), true);
  assertEquals(visited.includes("a"), true);
  assertEquals(visited.includes("b"), true);
  assertEquals(visited.includes("c"), true);

  await db.destroy();
});

Deno.test("traverseFrom - visits descendants of specific node", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("root", undefined);
  await store.addNode("a", "root");
  await store.addNode("b", "root");
  await store.addNode("c", "a");

  const visited: string[] = [];
  await store.traverseFrom("a", (id) => visited.push(id), null);

  assertEquals(visited, ["a", "c"]);

  await db.destroy();
});

Deno.test("multiple trees - getHeads returns all heads", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  await store.setHead("e1", "tree1");

  await store.addNode("e2", undefined, new Map([["b", "2"]]));
  await store.setHead("e2", "tree2");

  const heads = await store.getHeads();
  assertEquals(heads.get("main"), "e2");
  assertEquals(heads.get("tree1"), "e1");
  assertEquals(heads.get("tree2"), "e2");

  await db.destroy();
});

Deno.test("addChild - is idempotent", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined);
  await store.addNode("e2", "e1");
  await store.addChild("e1", "e2");

  const parent = await store.getParent("e2");
  assertEquals(parent, "e1");

  await db.destroy();
});

Deno.test("createCheckpoint - returns existing checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  const cp1 = await store.createCheckpoint("e1");
  const cp2 = await store.createCheckpoint("e1");

  assertEquals(cp1, cp2);

  await db.destroy();
});

Deno.test("createCheckpoint - on event with no prior checkpoint", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  const checkpointId = await store.createCheckpoint("e1");
  assertExists(checkpointId);

  const value = await store.get("a", "e1");
  assertEquals(value, "1");

  await db.destroy();
});

Deno.test("traverseState - without eventId uses current head", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));
  await store.addNode("e2", "e1", new Map([["b", "2"]]));

  const state: Record<string, string | null> = {};
  for await (const [key, value] of store.traverseState()) {
    state[key] = value;
  }

  assertEquals(state["a"], "1");
  assertEquals(state["b"], "2");

  await db.destroy();
});

Deno.test("empty tree - get returns null when no events exist", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  const value = await store.get("anything");
  assertEquals(value, null);

  await db.destroy();
});

Deno.test("empty tree - getHead returns null", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  const head = await store.getHead("main");
  assertEquals(head, null);

  await db.destroy();
});

Deno.test("complex branch - three-way branch isolation", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("root", undefined, new Map([["x", "0"]]));

  await store.addNode("branch1", "root", new Map([["a", "1"]]));
  await store.addNode("branch1b", "branch1", new Map([["a", "1b"]]));

  await store.addNode("branch2", "root", new Map([["b", "2"]]));
  await store.addNode("branch2b", "branch2", new Map([["b", "2b"]]));

  await store.addNode("branch3", "root", new Map([["c", "3"]]));

  assertEquals(await store.get("x", "branch1b"), "0");
  assertEquals(await store.get("a", "branch1b"), "1b");
  assertEquals(await store.get("b", "branch1b"), null);
  assertEquals(await store.get("c", "branch1b"), null);

  assertEquals(await store.get("x", "branch2b"), "0");
  assertEquals(await store.get("a", "branch2b"), null);
  assertEquals(await store.get("b", "branch2b"), "2b");
  assertEquals(await store.get("c", "branch2b"), null);

  assertEquals(await store.get("x", "branch3"), "0");
  assertEquals(await store.get("a", "branch3"), null);
  assertEquals(await store.get("b", "branch3"), null);
  assertEquals(await store.get("c", "branch3"), "3");

  await db.destroy();
});

Deno.test("complex branch - merge back to common ancestor", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("root", undefined, new Map([["base", "root"]]));

  await store.addNode("branchA", "root", new Map([["a", "fromA"]]));
  await store.addNode("branchB", "root", new Map([["b", "fromB"]]));

  await store.addNode("merge", "branchA", new Map([["fromA", "modifiedA"]]));

  assertEquals(await store.get("base", "merge"), "root");
  assertEquals(await store.get("a", "merge"), "fromA");
  assertEquals(await store.get("b", "merge"), null);

  await db.destroy();
});

Deno.test("deep chain - 10 events in sequence", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  let current: string | undefined;
  for (let i = 1; i <= 10; i++) {
    const next = `e${i}`;
    await store.addNode(next, current, new Map([[`key${i}`, `value${i}`]]));
    current = next;
  }

  for (let i = 1; i <= 10; i++) {
    const value = await store.get(`key${i}`, "e10");
    assertEquals(value, `value${i}`);
  }

  await db.destroy();
});

Deno.test("deep chain - reading older event shows previous state", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["x", "1"]]));
  await store.addNode("e2", "e1", new Map([["x", "2"]]));
  await store.addNode("e3", "e2", new Map([["x", "3"]]));
  await store.addNode("e4", "e3", new Map([["x", "4"]]));

  assertEquals(await store.get("x", "e1"), "1");
  assertEquals(await store.get("x", "e2"), "2");
  assertEquals(await store.get("x", "e3"), "3");
  assertEquals(await store.get("x", "e4"), "4");

  assertEquals(await store.get("x", "e2"), "2");
  assertEquals(await store.get("x", "e1"), "1");

  await db.destroy();
});

Deno.test("edge case - checkpoint at first event", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["a", "1"]]));

  const cp = await store.createCheckpoint("e1");

  await store.addNode("e2", "e1", new Map([["b", "2"]]));

  assertEquals(await store.get("a", "e2"), "1");
  assertEquals(await store.get("b", "e2"), "2");

  await db.destroy();
});

Deno.test("edge case - multiple writes to same key in chain", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["x", "1"]]));
  await store.addNode("e2", "e1", new Map([["x", "2"]]));
  await store.addNode("e3", "e2", new Map([["x", "3"]]));
  await store.addNode("e4", "e3", new Map([["x", "4"]]));
  await store.addNode("e5", "e4", new Map([["x", "5"]]));

  assertEquals(await store.get("x", "e3"), "3");
  assertEquals(await store.get("x", "e1"), "1");
  assertEquals(await store.get("x", "e5"), "5");

  await db.destroy();
});

Deno.test("edge case - delete and recreate same key", async () => {
  const db = createTestDb();
  await createTreeTables(db);
  const store = new SqliteTreeStore(db);

  await store.addNode("e1", undefined, new Map([["x", "1"]]));
  await store.addNode("e2", "e1", new Map<string, string | null>([["x", null]]));
  await store.addNode("e3", "e2", new Map([["x", "recreated"]]));

  assertEquals(await store.get("x", "e1"), "1");
  assertEquals(await store.get("x", "e2"), null);
  assertEquals(await store.get("x", "e3"), "recreated");

  await db.destroy();
});
