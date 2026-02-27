// Runner module handles running a given event and returning a transaction
// as well as any state changes and so on

import type { Event, TransactionId } from "./events.ts";

class Runner {
  constructor() {}
  spawn() {
  }
  async executeRoot(
    id: TransactionId,
    root: Event,
  ): Result<Transaction, Error> {
    // execute a root event

    return {
      id,
      root,
      diffs: [],
      inputs: [],
      effects: [],
    };
  }
}
