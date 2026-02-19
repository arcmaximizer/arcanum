import type { Kysely } from "kysely";
import { sql } from "kysely";
import { Branded, Serializable } from "../lib/types.ts";
import {
  createDAG,
  createDAGTables,
  KyselyDAGraph,
} from "../lib/dag_sqlite.ts";
import { err, ok, Result } from "neverthrow";

export class TxNotExistsError extends Error {}
export class ConfigError extends Error {}
export class TxExistsError extends Error {}

export type ProgramHash = Branded<string, "ProgramHash">;
export type TransactionId = Branded<string, "TransactionId">;
export type EventId = Branded<string, "EventId">;
export type CallId = Branded<string, "CallId">;

export interface Input {
  callId: CallId;
  value: Serializable;
}

export interface Event {
  id: EventId;
  from: ProgramHash;
  to: ProgramHash;
  content: Serializable;
  timestamp: number;
  children?: Event[];
}

export interface StateDiff {
  id: string;
  value: Serializable;
}

export interface Transaction {
  id: TransactionId;
  root: Event;
  diffs: StateDiff[];
  inputs: Input[];
  effects: Event[];
}

const TXLOG_PREFIX = "txlog";

export async function createTxLogTables(db: Kysely<any>): Promise<void> {
  await createDAGTables(db, TXLOG_PREFIX);

  await sql`
    CREATE TABLE IF NOT EXISTS ${sql.id(TXLOG_PREFIX + "_transactions")} (
      id TEXT NOT NULL PRIMARY KEY,
      is_current INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS ${sql.id(TXLOG_PREFIX + "_events")} (
      id TEXT NOT NULL PRIMARY KEY,
      from_program TEXT NOT NULL,
      to_program TEXT NOT NULL,
      content TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      parent_event_id TEXT,
      transaction_id TEXT NOT NULL,
      is_effect INTEGER DEFAULT 0,
      FOREIGN KEY (transaction_id) REFERENCES ${
    sql.id(TXLOG_PREFIX + "_transactions")
  } (id),
      FOREIGN KEY (parent_event_id) REFERENCES ${
    sql.id(TXLOG_PREFIX + "_events")
  } (id)
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS ${sql.id(TXLOG_PREFIX + "_inputs")} (
      id TEXT NOT NULL PRIMARY KEY,
      call_id TEXT NOT NULL,
      value TEXT NOT NULL,
      transaction_id TEXT NOT NULL,
      FOREIGN KEY (transaction_id) REFERENCES ${
    sql.id(TXLOG_PREFIX + "_transactions")
  } (id)
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS ${sql.id(TXLOG_PREFIX + "_state_diffs")} (
      id TEXT NOT NULL PRIMARY KEY,
      value TEXT NOT NULL,
      transaction_id TEXT NOT NULL,
      FOREIGN KEY (transaction_id) REFERENCES ${
    sql.id(TXLOG_PREFIX + "_transactions")
  } (id)
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS ${sql.id(TXLOG_PREFIX + "_effects")} (
      id TEXT NOT NULL PRIMARY KEY,
      from_program TEXT NOT NULL,
      to_program TEXT NOT NULL,
      content TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      transaction_id TEXT NOT NULL,
      FOREIGN KEY (transaction_id) REFERENCES ${
    sql.id(TXLOG_PREFIX + "_transactions")
  } (id)
    )
  `.execute(db);
}

export interface SqliteTransactionLogOptions {
  db: Kysely<any>;
}

export class TransactionLog {
  private readonly db: Kysely<any>;
  private readonly dag: KyselyDAGraph;
  private current?: TransactionId;

  constructor(options: SqliteTransactionLogOptions) {
    this.db = options.db;
    this.dag = createDAG(this.db, TXLOG_PREFIX);
  }

  static async create(
    options: SqliteTransactionLogOptions,
  ): Promise<TransactionLog> {
    await createTxLogTables(options.db);
    const log = new TransactionLog(options);
    await log.loadCurrent();
    return log;
  }

  private async loadCurrent(): Promise<void> {
    const row = await this.db
      .selectFrom(TXLOG_PREFIX + "_transactions")
      .select("id")
      .where("is_current", "=", 1)
      .executeTakeFirst();

    if (row) {
      this.current = row.id as TransactionId;
    }
  }

  private async insertTransaction(tx: Transaction): Promise<void> {
    const txId = tx.id;
    const isCurrent = this.current === undefined ? 1 : 0;

    await this.db
      .insertInto(TXLOG_PREFIX + "_transactions")
      .values({
        id: txId,
        is_current: isCurrent,
        created_at: tx.root.timestamp,
      } as any)
      .onConflict((oc) => oc.column("id").doNothing())
      .execute();

    if (isCurrent) {
      this.current = txId;
    }

    await this.insertEvent(tx.root, txId, null);
  }

  private async insertEvent(
    event: Event,
    transactionId: TransactionId,
    parentEventId: EventId | null,
  ): Promise<void> {
    await this.db
      .insertInto(TXLOG_PREFIX + "_events")
      .values({
        id: event.id,
        from_program: event.from,
        to_program: event.to,
        content: JSON.stringify(event.content),
        timestamp: event.timestamp,
        parent_event_id: parentEventId,
        transaction_id: transactionId,
        is_effect: 0,
      } as any)
      .onConflict((oc) => oc.column("id").doNothing())
      .execute();

    if (event.children) {
      for (const child of event.children) {
        await this.insertEvent(child, transactionId, event.id);
      }
    }
  }

  private async insertInputs(
    inputs: Input[],
    transactionId: TransactionId,
  ): Promise<void> {
    for (const input of inputs) {
      await this.db
        .insertInto(TXLOG_PREFIX + "_inputs")
        .values({
          id: crypto.randomUUID(),
          call_id: input.callId,
          value: JSON.stringify(input.value),
          transaction_id: transactionId,
        } as any)
        .execute();
    }
  }

  private async insertDiffs(
    diffs: StateDiff[],
    transactionId: TransactionId,
  ): Promise<void> {
    for (const diff of diffs) {
      await this.db
        .insertInto(TXLOG_PREFIX + "_state_diffs")
        .values({
          id: diff.id,
          value: JSON.stringify(diff.value),
          transaction_id: transactionId,
        } as any)
        .execute();
    }
  }

  private async insertEffects(
    effects: Event[],
    transactionId: TransactionId,
  ): Promise<void> {
    for (const effect of effects) {
      await this.db
        .insertInto(TXLOG_PREFIX + "_effects")
        .values({
          id: effect.id,
          from_program: effect.from,
          to_program: effect.to,
          content: JSON.stringify(effect.content),
          timestamp: effect.timestamp,
          transaction_id: transactionId,
        } as any)
        .execute();
    }
  }

  private async getEvent(eventId: EventId): Promise<Event | null> {
    const rows = await this.db
      .selectFrom(TXLOG_PREFIX + "_events")
      .selectAll()
      .where("id", "=", eventId)
      .execute();

    if (rows.length === 0) return null;

    const row = rows[0] as any;
    return {
      id: row.id as EventId,
      from: row.from_program,
      to: row.to_program,
      content: JSON.parse(row.content),
      timestamp: row.timestamp,
      children: [],
    };
  }

  private async getEventWithChildren(eventId: EventId): Promise<Event | null> {
    const event = await this.getEvent(eventId);
    if (!event) return null;

    const childrenRows = await this.db
      .selectFrom(TXLOG_PREFIX + "_events")
      .selectAll()
      .where("parent_event_id", "=", eventId)
      .execute();

    const children: Event[] = [];
    for (const childRow of childrenRows as any[]) {
      const child = await this.getEventWithChildren(childRow.id);
      if (child) children.push(child);
    }

    event.children = children;
    return event;
  }

  private async getInputs(transactionId: TransactionId): Promise<Input[]> {
    const rows = await this.db
      .selectFrom(TXLOG_PREFIX + "_inputs")
      .selectAll()
      .where("transaction_id", "=", transactionId)
      .execute();

    return (rows as any[]).map((row) => ({
      callId: row.call_id,
      value: JSON.parse(row.value),
    }));
  }

  private async getDiffs(transactionId: TransactionId): Promise<StateDiff[]> {
    const rows = await this.db
      .selectFrom(TXLOG_PREFIX + "_state_diffs")
      .selectAll()
      .where("transaction_id", "=", transactionId)
      .execute();

    return (rows as any[]).map((row) => ({
      id: row.id,
      value: JSON.parse(row.value),
    }));
  }

  async appendCurrent(tx: Transaction): Promise<void> {
    const exists = await this.dag.getNode(tx.id);
    if (exists) return;

    const hasCurrent = !!this.current;

    await this.insertTransaction(tx);
    await this.insertInputs(tx.inputs, tx.id);
    await this.insertDiffs(tx.diffs, tx.id);
    await this.insertEffects(tx.effects, tx.id);

    if (hasCurrent && this.current) {
      await this.dag.addEdge(this.current, tx.id);
    } else {
      await this.dag.addNode(tx.id);
    }
  }

  async appendTo(
    from: TransactionId,
    tx: Transaction,
  ): Promise<Result<void, Error>> {
    const nodeExists = await this.dag.getNode(from);
    if (!nodeExists) return err(new Error("Transaction not found"));

    await this.insertTransaction(tx);
    await this.insertInputs(tx.inputs, tx.id);
    await this.insertDiffs(tx.diffs, tx.id);
    await this.insertEffects(tx.effects, tx.id);

    await this.dag.addEdge(from, tx.id);

    if (from === this.current) this.current = tx.id;
    return ok();
  }

  async rollbackTo(id: TransactionId): Promise<Result<void, Error>> {
    const nodeExists = await this.dag.getNode(id);
    if (!nodeExists) return err(new Error("Transaction not found"));

    if (this.current) {
      await this.db
        .updateTable(TXLOG_PREFIX + "_transactions")
        .set({ is_current: 0 })
        .where("id", "=", this.current)
        .execute();
    }

    await this.db
      .updateTable(TXLOG_PREFIX + "_transactions")
      .set({ is_current: 1 })
      .where("id", "=", id)
      .execute();

    this.current = id;
    return ok();
  }

  async get(id: TransactionId): Promise<Transaction | undefined> {
    const txRows = await this.db
      .selectFrom(TXLOG_PREFIX + "_transactions")
      .selectAll()
      .where("id", "=", id)
      .execute();

    if (txRows.length === 0) return undefined;

    const txRow = txRows[0] as any;

    // Find root event: transaction_id = txId AND parent_event_id IS NULL
    const rootRows = await this.db
      .selectFrom(TXLOG_PREFIX + "_events")
      .selectAll()
      .where("transaction_id", "=", id)
      .where("parent_event_id", "is", null)
      .execute();

    if (rootRows.length === 0) return undefined;

    const root = await this.getEventWithChildren(rootRows[0].id as EventId);

    if (!root) return undefined;

    const [inputs, diffs, effects] = await Promise.all([
      this.getInputs(id),
      this.getDiffs(id),
      this.getEffects(id),
    ]);

    return {
      id: txRow.id as TransactionId,
      root,
      inputs,
      diffs,
      effects,
    };
  }

  private async getEffects(transactionId: TransactionId): Promise<Event[]> {
    const rows = await this.db
      .selectFrom(TXLOG_PREFIX + "_effects")
      .selectAll()
      .where("transaction_id", "=", transactionId)
      .execute();

    return (rows as any[]).map((row) => ({
      id: row.id as EventId,
      from: row.from_program,
      to: row.to_program,
      content: JSON.parse(row.content),
      timestamp: row.timestamp,
    }));
  }
}
