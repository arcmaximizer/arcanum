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

export interface TxLogDb {
  txlog_transactions: TxLogTransaction;
  txlog_events: TxLogEvent;
  txlog_inputs: TxLogInput;
  txlog_state_diffs: TxLogStateDiff;
  txlog_effects: TxLogEffect;
}

export interface TxLogTransaction {
  id: string;
  is_current: number;
  created_at: number;
}

export interface TxLogEvent {
  id: string;
  from_program: string;
  to_program: string;
  content: string;
  timestamp: number;
  parent_event_id: string | null;
  transaction_id: string;
  is_effect: number;
}

export interface TxLogInput {
  id: string;
  call_id: string;
  value: string;
  transaction_id: string;
}

export interface TxLogStateDiff {
  id: string;
  value: string;
  transaction_id: string;
}

export interface TxLogEffect {
  id: string;
  from_program: string;
  to_program: string;
  content: string;
  timestamp: number;
  transaction_id: string;
}

export async function createTxLogTables(db: Kysely<any>): Promise<void> {
  await createDAGTables(db, "txlog");

  await sql`
    CREATE TABLE IF NOT EXISTS txlog_transactions (
      id TEXT NOT NULL PRIMARY KEY,
      is_current INTEGER DEFAULT 0,
      created_at INTEGER NOT NULL
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS txlog_events (
      id TEXT NOT NULL PRIMARY KEY,
      from_program TEXT NOT NULL,
      to_program TEXT NOT NULL,
      content TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      parent_event_id TEXT,
      transaction_id TEXT NOT NULL,
      is_effect INTEGER DEFAULT 0,
      FOREIGN KEY (transaction_id) REFERENCES txlog_transactions (id),
      FOREIGN KEY (parent_event_id) REFERENCES txlog_events (id)
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS txlog_inputs (
      id TEXT NOT NULL PRIMARY KEY,
      call_id TEXT NOT NULL,
      value TEXT NOT NULL,
      transaction_id TEXT NOT NULL,
      FOREIGN KEY (transaction_id) REFERENCES txlog_transactions (id)
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS txlog_state_diffs (
      id TEXT NOT NULL PRIMARY KEY,
      value TEXT NOT NULL,
      transaction_id TEXT NOT NULL,
      FOREIGN KEY (transaction_id) REFERENCES txlog_transactions (id)
    )
  `.execute(db);

  await sql`
    CREATE TABLE IF NOT EXISTS txlog_effects (
      id TEXT NOT NULL PRIMARY KEY,
      from_program TEXT NOT NULL,
      to_program TEXT NOT NULL,
      content TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      transaction_id TEXT NOT NULL,
      FOREIGN KEY (transaction_id) REFERENCES txlog_transactions (id)
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
    this.dag = createDAG(this.db, "txlog");
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
      .selectFrom("txlog_transactions")
      .select("id")
      .where("is_current", "=", 1)
      .executeTakeFirst();

    if (row) {
      this.current = row.id as TransactionId;
    }
  }

  private async insertTransaction(tx: Transaction, dbParam?: Kysely<any>): Promise<void> {
    const qb = dbParam ?? this.db;
    const txId = tx.id;
    const isCurrent = this.current === undefined ? 1 : 0;

    try {
      await qb
        .insertInto("txlog_transactions")
        .values({
          id: txId,
          is_current: isCurrent,
          created_at: tx.root.timestamp,
        })
        .execute();
    } catch (e) {
      console.error(
        `CRITICAL: Failed to insert transaction ${txId} - ${e instanceof Error ? e.message : "unknown error"}`,
      );
      throw new Error(`Transaction insert failed for ${txId}`);
    }

    if (isCurrent) {
      this.current = txId;
    }

    await this.insertEvent(tx.root, txId, null, dbParam);
  }

  private async insertEvent(
    event: Event,
    transactionId: TransactionId,
    parentEventId: EventId | null,
    dbParam?: Kysely<any>,
  ): Promise<void> {
    const qb = dbParam ?? this.db;
    try {
      await qb
        .insertInto("txlog_events")
        .values({
          id: event.id,
          from_program: event.from,
          to_program: event.to,
          content: JSON.stringify(event.content),
          timestamp: event.timestamp,
          parent_event_id: parentEventId,
          transaction_id: transactionId,
          is_effect: 0,
        })
        .execute();
    } catch (e) {
      console.error(
        `CRITICAL: Failed to insert event ${event.id} - ${e instanceof Error ? e.message : "unknown error"}`,
      );
      throw e;
    }

    if (event.children) {
      for (const child of event.children) {
        await this.insertEvent(child, transactionId, event.id, dbParam);
      }
    }
  }

  private async insertInputs(
    inputs: Input[],
    transactionId: TransactionId,
    dbParam?: Kysely<any>,
  ): Promise<void> {
    const qb = dbParam ?? this.db;
    for (const input of inputs) {
      try {
        await qb
          .insertInto("txlog_inputs")
          .values({
            id: crypto.randomUUID(),
            call_id: input.callId,
            value: JSON.stringify(input.value),
            transaction_id: transactionId,
          })
          .execute();
      } catch (e) {
        console.error(
          `CRITICAL: Failed to insert input for transaction ${transactionId} - ${e instanceof Error ? e.message : "unknown error"}`,
        );
        throw e;
      }
    }
  }

  private async insertDiffs(
    diffs: StateDiff[],
    transactionId: TransactionId,
    dbParam?: Kysely<any>,
  ): Promise<void> {
    const qb = dbParam ?? this.db;
    for (const diff of diffs) {
      try {
        await qb
          .insertInto("txlog_state_diffs")
          .values({
            id: diff.id,
            value: JSON.stringify(diff.value),
            transaction_id: transactionId,
          })
          .execute();
      } catch (e) {
        console.error(
          `CRITICAL: Failed to insert state diff ${diff.id} - ${e instanceof Error ? e.message : "unknown error"}`,
        );
        throw e;
      }
    }
  }

  private async insertEffects(
    effects: Event[],
    transactionId: TransactionId,
    dbParam?: Kysely<any>,
  ): Promise<void> {
    const qb = dbParam ?? this.db;
    for (const effect of effects) {
      try {
        await qb
          .insertInto("txlog_effects")
          .values({
            id: effect.id,
            from_program: effect.from,
            to_program: effect.to,
            content: JSON.stringify(effect.content),
            timestamp: effect.timestamp,
            transaction_id: transactionId,
          })
          .execute();
      } catch (e) {
        console.error(
          `CRITICAL: Failed to insert effect ${effect.id} - ${e instanceof Error ? e.message : "unknown error"}`,
        );
        throw e;
      }
    }
  }

  private async getEvent(eventId: EventId, dbParam?: Kysely<any>): Promise<Event | null> {
    const qb = dbParam ?? this.db;
    const rows = await qb
      .selectFrom("txlog_events")
      .selectAll()
      .where("id", "=", eventId)
      .execute();

    if (rows.length === 0) return null;

    const row = rows[0] as TxLogEvent;
    return {
      id: row.id as EventId,
      from: row.from_program as ProgramHash,
      to: row.to_program as ProgramHash,
      content: JSON.parse(row.content),
      timestamp: row.timestamp,
      children: [],
    };
  }

  private async getEventWithChildren(eventId: EventId, dbParam?: Kysely<any>): Promise<Event | null> {
    const event = await this.getEvent(eventId, dbParam);
    if (!event) return null;

    const qb = dbParam ?? this.db;
    const childrenRows = await qb
      .selectFrom("txlog_events")
      .selectAll()
      .where("parent_event_id", "=", eventId)
      .execute();

    const children: Event[] = [];
    for (const childRow of childrenRows as TxLogEvent[]) {
      const child = await this.getEventWithChildren(childRow.id as EventId, dbParam);
      if (child) children.push(child);
    }

    event.children = children;
    return event;
  }

  private async getInputs(transactionId: TransactionId, dbParam?: Kysely<any>): Promise<Input[]> {
    const qb = dbParam ?? this.db;
    const rows = await qb
      .selectFrom("txlog_inputs")
      .selectAll()
      .where("transaction_id", "=", transactionId)
      .execute();

    return (rows as TxLogInput[]).map((row) => ({
      callId: row.call_id as CallId,
      value: JSON.parse(row.value),
    }));
  }

  private async getDiffs(transactionId: TransactionId, dbParam?: Kysely<any>): Promise<StateDiff[]> {
    const qb = dbParam ?? this.db;
    const rows = await qb
      .selectFrom("txlog_state_diffs")
      .selectAll()
      .where("transaction_id", "=", transactionId)
      .execute();

    return (rows as TxLogStateDiff[]).map((row) => ({
      id: row.id,
      value: JSON.parse(row.value),
    }));
  }

  async appendCurrent(tx: Transaction, dbParam?: Kysely<any>): Promise<void> {
    const exists = await this.dag.getNode(tx.id, dbParam);
    if (exists) {
      console.error(
        `CRITICAL: Transaction ${tx.id} already exists - refusing to append!`,
      );
      throw new Error(`Transaction ${tx.id} already exists`);
    }

    const hasCurrent = !!this.current;

    await this.insertTransaction(tx, dbParam);
    await this.insertInputs(tx.inputs, tx.id, dbParam);
    await this.insertDiffs(tx.diffs, tx.id, dbParam);
    await this.insertEffects(tx.effects, tx.id, dbParam);

    if (hasCurrent && this.current) {
      await this.dag.addEdge(this.current, tx.id, dbParam);
    } else {
      await this.dag.addNode(tx.id, dbParam);
    }
  }

  async appendTo(
    from: TransactionId,
    tx: Transaction,
    dbParam?: Kysely<any>,
  ): Promise<Result<void, Error>> {
    const nodeExists = await this.dag.getNode(from, dbParam);
    if (!nodeExists) {
      console.error(
        `CRITICAL: Cannot append transaction ${tx.id} - parent transaction ${from} does not exist!`,
      );
      return err(new Error(`Parent transaction ${from} not found`));
    }

    await this.insertTransaction(tx, dbParam);
    await this.insertInputs(tx.inputs, tx.id, dbParam);
    await this.insertDiffs(tx.diffs, tx.id, dbParam);
    await this.insertEffects(tx.effects, tx.id, dbParam);

    await this.dag.addEdge(from, tx.id, dbParam);

    if (from === this.current) this.current = tx.id;
    return ok();
  }

  async rollbackTo(id: TransactionId, dbParam?: Kysely<any>): Promise<Result<void, Error>> {
    const nodeExists = await this.dag.getNode(id, dbParam);
    if (!nodeExists) {
      console.error(
        `CRITICAL: Cannot rollback to ${id} - transaction does not exist!`,
      );
      return err(new Error(`Transaction ${id} not found`));
    }

    const qb = dbParam ?? this.db;

    if (this.current) {
      try {
        await qb
          .updateTable("txlog_transactions")
          .set({ is_current: 0 })
          .where("id", "=", this.current)
          .execute();
      } catch (e) {
        console.error(
          `CRITICAL: Failed to unset current transaction ${this.current} - ${e instanceof Error ? e.message : "unknown error"}`,
        );
        throw e;
      }
    }

    try {
      await qb
        .updateTable("txlog_transactions")
        .set({ is_current: 1 })
        .where("id", "=", id)
        .execute();
    } catch (e) {
      console.error(
        `CRITICAL: Failed to set current transaction to ${id} - ${e instanceof Error ? e.message : "unknown error"}`,
      );
      throw e;
    }

    this.current = id;
    return ok();
  }

  async get(id: TransactionId, dbParam?: Kysely<any>): Promise<Transaction | undefined> {
    const qb = dbParam ?? this.db;
    const txRows = await qb
      .selectFrom("txlog_transactions")
      .selectAll()
      .where("id", "=", id)
      .execute();

    if (txRows.length === 0) return undefined;

    const txRow = txRows[0] as TxLogTransaction;

    const rootRows = await qb
      .selectFrom("txlog_events")
      .selectAll()
      .where("transaction_id", "=", id)
      .where("parent_event_id", "is", null)
      .execute();

    if (rootRows.length === 0) return undefined;

    const root = await this.getEventWithChildren(rootRows[0].id as EventId, dbParam);

    if (!root) return undefined;

    const [inputs, diffs, effects] = await Promise.all([
      this.getInputs(id, dbParam),
      this.getDiffs(id, dbParam),
      this.getEffects(id, dbParam),
    ]);

    return {
      id: txRow.id as TransactionId,
      root,
      inputs,
      diffs,
      effects,
    };
  }

  private async getEffects(transactionId: TransactionId, dbParam?: Kysely<any>): Promise<Event[]> {
    const qb = dbParam ?? this.db;
    const rows = await qb
      .selectFrom("txlog_effects")
      .selectAll()
      .where("transaction_id", "=", transactionId)
      .execute();

    return (rows as TxLogEffect[]).map((row) => ({
      id: row.id as EventId,
      from: row.from_program as ProgramHash,
      to: row.to_program as ProgramHash,
      content: JSON.parse(row.content),
      timestamp: row.timestamp,
    }));
  }
}
