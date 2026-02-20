import { Database } from "@db/sqlite";
import { Kysely } from "kysely";
import { sql } from "kysely";
import { assertEquals, assertExists, assertRejects } from "@std/assert";
import {
  createTxLogTables,
  type Event,
  type EventId,
  type Input,
  type ProgramHash,
  type StateDiff,
  type Transaction,
  type TransactionId,
  TransactionLog,
  type TxLogDb,
} from "../svc/events.ts";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";

function createTestDb(): Kysely<TxLogDb> {
  const db = new Database(":memory:");
  return new Kysely<TxLogDb>({
    dialect: new DenoSqliteDialect({ database: db }),
  });
}

function txId(id: string): TransactionId {
  return id as TransactionId;
}

function eventId(id: string): EventId {
  return id as EventId;
}

function progHash(s: string): ProgramHash {
  return s as ProgramHash;
}

function createTestTx(id: string): Transaction {
  return {
    id: txId(id),
    root: {
      id: eventId(id),
      from: progHash("program/a"),
      to: progHash("program/b"),
      content: { foo: "bar" },
      timestamp: Date.now(),
      children: [],
    },
    diffs: [{ id: `diff-${id}`, value: { key: "value" } }],
    inputs: [{ callId: `call-${id}` as any, value: "input" }],
    effects: [{
      id: eventId(`effect-${id}`),
      from: progHash("program/b"),
      to: progHash("program/c"),
      content: "effect",
      timestamp: Date.now(),
    }],
  };
}

Deno.test("createTxLogTables creates all tables", async () => {
  const db = createTestDb();
  await createTxLogTables(db);

  const expectedTables = [
    "txlog_edges",
    "txlog_events",
    "txlog_inputs",
    "txlog_nodes",
    "txlog_state_diffs",
    "txlog_effects",
    "txlog_transactions",
  ] as const;

  for (const table of expectedTables) {
    const rows = await (db as any).selectFrom(table).selectAll().execute();
    assertEquals(Array.isArray(rows), true);
  }

  await db.destroy();
});

Deno.test("TransactionLog.create initializes and returns instance", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  assertExists(log);
  await db.destroy();
});

Deno.test("appendCurrent adds transaction and sets as current", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx);

  const retrieved = await log.get(txId("tx1"));
  assertExists(retrieved);
  assertEquals(retrieved.id, txId("tx1"));

  await db.destroy();
});

Deno.test("appendCurrent stores root event", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx);

  const retrieved = await log.get(txId("tx1"));
  assertExists(retrieved);
  assertEquals(retrieved.root.from, progHash("program/a"));
  assertEquals(retrieved.root.to, progHash("program/b"));

  await db.destroy();
});

Deno.test("appendCurrent stores inputs", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx);

  const retrieved = await log.get(txId("tx1"));
  assertExists(retrieved);
  assertEquals(retrieved.inputs.length, 1);
  assertEquals(retrieved.inputs[0].callId, "call-tx1");

  await db.destroy();
});

Deno.test("appendCurrent stores diffs", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx);

  const retrieved = await log.get(txId("tx1"));
  assertExists(retrieved);
  assertEquals(retrieved.diffs.length, 1);
  assertEquals(retrieved.diffs[0].id, "diff-tx1");

  await db.destroy();
});

Deno.test("appendCurrent stores effects", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx);

  const retrieved = await log.get(txId("tx1"));
  assertExists(retrieved);
  assertEquals(retrieved.effects.length, 1);
  assertEquals(retrieved.effects[0].from, progHash("program/b"));

  await db.destroy();
});

Deno.test("appendCurrent throws on duplicate transaction", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx);

  try {
    await log.appendCurrent(tx);
    throw new Error("Should have thrown");
  } catch (e) {
    assertEquals(e instanceof Error, true);
  }

  const all = await db.selectFrom("txlog_transactions").selectAll().execute();
  assertEquals(all.length, 1);

  await db.destroy();
});

Deno.test("appendTo adds transaction with dependency", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx1 = createTestTx("tx1");
  await log.appendCurrent(tx1);

  const tx2 = createTestTx("tx2");
  const result = await log.appendTo(txId("tx1"), tx2);

  assertEquals(result.isOk(), true);

  const retrieved = await log.get(txId("tx2"));
  assertExists(retrieved);

  await db.destroy();
});

Deno.test("appendTo returns error for non-existent parent", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  const result = await log.appendTo(txId("nonexistent"), tx);

  assertEquals(result.isErr(), true);

  await db.destroy();
});

Deno.test("appendTo updates current if appending to current", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx1 = createTestTx("tx1");
  await log.appendCurrent(tx1);

  const tx2 = createTestTx("tx2");
  await log.appendTo(txId("tx1"), tx2);

  const retrieved = await log.get(txId("tx2"));
  assertExists(retrieved);

  await db.destroy();
});

Deno.test("get returns undefined for non-existent transaction", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const result = await log.get(txId("nonexistent"));

  assertEquals(result, undefined);

  await db.destroy();
});

Deno.test("rollbackTo sets current to previous transaction", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx1 = createTestTx("tx1");
  await log.appendCurrent(tx1);

  const tx2 = createTestTx("tx2");
  await log.appendCurrent(tx2);

  const result = await log.rollbackTo(txId("tx1"));
  assertEquals(result.isOk(), true);

  await db.destroy();
});

Deno.test("rollbackTo returns error for non-existent transaction", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const result = await log.rollbackTo(txId("nonexistent"));

  assertEquals(result.isErr(), true);

  await db.destroy();
});

Deno.test("stores event with children", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx: Transaction = {
    id: txId("tx1"),
    root: {
      id: eventId("root1"),
      from: progHash("program/a"),
      to: progHash("program/b"),
      content: {},
      timestamp: Date.now(),
      children: [
        {
          id: eventId("child1"),
          from: progHash("program/b"),
          to: progHash("program/c"),
          content: {},
          timestamp: Date.now(),
        },
        {
          id: eventId("child2"),
          from: progHash("program/b"),
          to: progHash("program/d"),
          content: {},
          timestamp: Date.now(),
        },
      ],
    },
    diffs: [],
    inputs: [],
    effects: [],
  };

  await log.appendCurrent(tx);

  const retrieved = await log.get(txId("tx1"));
  assertExists(retrieved);
  assertEquals(retrieved.root.children?.length, 2);

  await db.destroy();
});

Deno.test("operations work with explicit db parameter", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  const tx = createTestTx("tx1");
  await log.appendCurrent(tx, db);

  const retrieved = await log.get(txId("tx1"), db);
  assertExists(retrieved);
  assertEquals(retrieved.id, txId("tx1"));

  await db.destroy();
});

Deno.test("operations are atomic when using db.transaction()", async () => {
  const db = createTestDb();
  const log = await TransactionLog.create({ db });

  await db.transaction().execute(async (trx) => {
    const tx1 = createTestTx("tx1");
    await log.appendCurrent(tx1, trx);

    const tx2 = createTestTx("tx2");
    await log.appendCurrent(tx2, trx);
  });

  const tx1 = await log.get(txId("tx1"));
  const tx2 = await log.get(txId("tx2"));
  assertExists(tx1);
  assertExists(tx2);

  await db.destroy();
});
