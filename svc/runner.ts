// Runner module handles running a given event and returning a transaction
// as well as any state changes and so on

import type { Event, Transaction, TransactionId } from "./events.ts";
import type { ResultAsync } from "neverthrow";
import { ok } from "neverthrow";

class Runner {
  constructor() {}
  spawn() {
  }
  async executeRoot(
    id: TransactionId,
    root: Event,
  ): ResultAsync<Transaction, Error> {
    // execute a root event

    return ok({
      id,
      root,
      diffs: [],
      inputs: [],
      effects: [],
    });
  }
}
