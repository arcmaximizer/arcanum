import { Branded, createError, ProgramId, Serializable } from "../lib/types";
import { createDAG, DAGraph } from "@sha1n/dagraph";
import { err, ok, Result } from "neverthrow";

export const TxNotExistsError = createError<"TxNotExistsError", {}>(
  "TxNotExistsError",
);
export const ConfigError = createError<"ConfigError", {}>(
  "ConfigError",
);
export const TxExistsError = createError<"TxExistsError", {}>(
  "TxExistsError",
);

// Points to a specific program at a specific version
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

  // All state diffs during the transaction
  diffs: StateDiff[];

  // All inputs from I/O (they are later sent to respective calls in order)
  inputs: Input[];

  // All side effects (they are events as they are later sent to runtime or
  // runtime extensions)
  effects: Event[];
}

// Stores executed transactions in a DAG
export class TransactionLog {
  dag: DAGraph<Transaction>;
  current?: TransactionId;
  pending: Event[];

  constructor(
    dag?: DAGraph<Transaction>,
    current?: TransactionId,
    pending?: Event[],
  ) {
    this.pending = pending ?? [];
    this.dag = dag ?? createDAG();
    this.current = current;

    if (dag && !current) throw new ConfigError();
  }

  appendCurrent(tx: Transaction) {
    const current = this.dag.getNode(this.current);

    if (this.dag.getNode(tx.id)) return;

    if (current) {
      this.dag.addEdge({ id: this.current } as Transaction, tx);
    } else {
      this.dag.addNode(tx);
    }

    this.current = tx.id;
  }

  appendTo(
    from: TransactionId,
    tx: Transaction,
  ): Result<void, typeof TxNotExistsError> {
    const node = this.dag.getNode(from);

    if (!node) return err(new TxNotExistsError());

    this.dag.addEdge(node, tx);
    if (from == this.current) this.current = tx.id;
    return ok();
  }

  rollbackTo(id: TransactionId): Result<void, typeof TxNotExistsError> {
    if (this.dag.getNode(id) != undefined) {
      this.current = id;
      return ok();
    } else {
      return err(new TxNotExistsError());
    }
  }

  get(id: TransactionId): Transaction | undefined {
    return this.dag.getNode(id);
  }
}
