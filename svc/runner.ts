// Runner module handles running a given event and returning a transaction
// as well as any state changes and so on

import type { Result } from "neverthrow";
import { ok } from "neverthrow";
import { ProgramId } from "../lib/types.ts";
import {TraversalState} from "./store.ts"

export default class Runner {
  workers: Worker[] = [];

  constructor() {
    // hello world
  }

  spawn(id: ProgramId) {
    // spawn
    const worker = new Worker(new URL("./worker.ts", import.meta.url).href, {
      type: "module",
    });
    this.workers.push(worker);
  }

  async execute(
    id: TransactionId,
    root: Event,
    state: 
  ): Promise<Result<Transaction, Error>> {
    // execute an event
    this.spawn();

    return ok({
      id,
      root,
      diffs: [],
      inputs: [],
      effects: [],
    });
  }
}
