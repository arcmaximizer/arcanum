import { Database } from "@db/sqlite";
import { Kysely } from "kysely";
import { assertEquals } from "@std/assert";
import * as fc from "npm:fast-check@^3.23.2";
import { createTreeTables, SqliteTreeStore, TreeStore } from "../svc/store.ts";
import { ReferenceTreeStore } from "../svc/reference_store.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import type { TreeDatabase } from "../svc/store.ts";

function createTestDb(): Kysely<TreeDatabase> {
  const db = new Database(":memory:");
  return new Kysely<TreeDatabase>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
}

// Define command types for property testing
type Command =
  | {
    type: "addNode";
    id: string;
    parent?: string;
    kvDiffs?: Map<string, string | null>;
  }
  | { type: "get"; key: string; eventId?: string }
  | { type: "setHead"; eventId: string; treeId?: string }
  | { type: "getHead"; treeId?: string }
  | { type: "addChild"; parent: string; child: string };

// Generate arbitrary commands
const commandArbitrary = fc.oneof(
  fc.record({
    type: fc.constant("addNode" as const),
    id: fc.uuid(),
    parent: fc.option(fc.uuid(), { nil: undefined }),
    kvDiffs: fc.option(
      fc.array(
        fc.tuple(
          fc.string(),
          fc.oneof(fc.string(), fc.constant(null) as fc.Arbitrary<null>),
        ),
      ).map((arr: [string, string | null][]) => new Map(arr)),
      { nil: undefined },
    ),
  }),
  fc.record({
    type: fc.constant("get" as const),
    key: fc.string(),
    eventId: fc.option(fc.uuid(), { nil: undefined }),
  }),
  fc.record({
    type: fc.constant("setHead" as const),
    eventId: fc.uuid(),
    treeId: fc.option(fc.string(), { nil: undefined }),
  }),
  fc.record({
    type: fc.constant("getHead" as const),
    treeId: fc.option(fc.string(), { nil: undefined }),
  }),
  fc.record({
    type: fc.constant("addChild" as const),
    parent: fc.uuid(),
    child: fc.uuid(),
  }),
);

async function runCommands(
  store: TreeStore,
  commands: Command[],
): Promise<Map<string, any>> {
  const results = new Map<string, any>();

  for (const cmd of commands) {
    try {
      switch (cmd.type) {
        case "addNode":
          await store.addNode(
            cmd.id,
            cmd.parent,
            cmd.kvDiffs,
          );
          break;
        case "get":
          const val = await store.get(cmd.key, cmd.eventId);
          results.set(`get:${cmd.key}:${cmd.eventId ?? "head"}`, val);
          break;
        case "setHead":
          await store.setHead(cmd.eventId, cmd.treeId);
          break;
        case "getHead":
          const head = await store.getHead(cmd.treeId);
          const key = `head:${cmd.treeId ?? "main"}`;
          results.set(key, head);
          break;
        case "addChild":
          await store.addChild(cmd.parent, cmd.child);
          break;
      }
    } catch (e: unknown) {
      // Store errors too to compare
      if (e instanceof Error) {
        results.set(`error:${cmd.type}:${JSON.stringify(cmd)}`, e.message);
      }
    }
  }

  return results;
}

Deno.test("property test - reference store equivalent to sqlite store", async () => {
  await fc.assert(
    fc.asyncProperty(
      commandArbitrary,
      fc.array(commandArbitrary, { maxLength: 20 }),
      async (_seed, commands) => {
        // Setup both stores
        const db = createTestDb();
        await createTreeTables(db);
        const sqliteStore = new SqliteTreeStore(db);
        const refStore = new ReferenceTreeStore(db);

        try {
          // Run commands on both stores
          const sqliteResults = await runCommands(sqliteStore, commands);
          const refResults = await runCommands(refStore, commands);

          // Compare results
          assertEquals(
            sqliteResults,
            refResults,
            "Results should be equivalent",
          );

          // Compare heads
          const sqliteHeads = await sqliteStore.getHeads();
          const refHeads = await refStore.getHeads();
          assertEquals(sqliteHeads, refHeads, "Heads should be equivalent");

          // Compare nodes
          const sqliteNodes = await sqliteStore.nodes();
          const refNodes = await refStore.nodes();
          assertEquals(sqliteNodes, refNodes, "Nodes should be equivalent");
        } finally {
          await db.destroy();
        }
      },
    ),
    { numRuns: 100 },
  );
});

Deno.test("property test - state reconstruction equivalence", async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.array(fc.tuple(fc.string(), fc.string()), { maxLength: 10 }),
      fc.array(fc.uuid(), { maxLength: 5 }),
      async (writes, eventIds) => {
        const db = createTestDb();
        await createTreeTables(db);
        const sqliteStore = new SqliteTreeStore(db);
        const refStore = new ReferenceTreeStore(db);

        try {
          // Create a chain of events with writes
          let prevId: string | undefined;
          for (const [i, eventId] of eventIds.entries()) {
            const kvDiffs = new Map(
              writes.slice(0, i + 1).map(([k, v]) => [k, v]),
            );
            await sqliteStore.addNode(eventId, prevId, kvDiffs);
            await refStore.addNode(eventId, prevId, kvDiffs);
            prevId = eventId;
          }

          // Test get on each event
          for (const eventId of eventIds) {
            for (const [key, _value] of writes) {
              const sqliteVal = await sqliteStore.get(key, eventId);
              const refVal = await refStore.get(key, eventId);
              assertEquals(
                sqliteVal,
                refVal,
                `Values should match for key ${key} at event ${eventId}`,
              );
            }
          }

          // Test traverseState
          const sqliteState: [string, string | null][] = [];
          for await (
            const entry of sqliteStore.traverseState(
              eventIds[eventIds.length - 1],
            )
          ) {
            sqliteState.push(entry);
          }

          const refState: [string, string | null][] = [];
          for await (
            const entry of refStore.traverseState(eventIds[eventIds.length - 1])
          ) {
            refState.push(entry);
          }

          assertEquals(
            new Map(sqliteState),
            new Map(refState),
            "State traversal should match",
          );
        } finally {
          await db.destroy();
        }
      },
    ),
    { numRuns: 50 },
  );
});
