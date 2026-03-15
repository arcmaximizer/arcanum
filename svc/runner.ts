// Runner module handles running a given event and returning a transaction
// as well as any state changes and so on

import type { Result } from "neverthrow";
import { ok } from "neverthrow";
import { Hash, ProgramId, Serializable } from "../lib/types.ts";
import { TreeStore } from "./store.ts";
import { PackageStore } from "./packages.ts";

interface EventProposal {
  from: ProgramId;
  to: Hash;
  input: Serializable;
  base?: string;
  metadata: Serializable;
}

interface EventPrecommit extends EventProposal {
  diffs: Map<string, string>;
  output?: Serializable;
  children: EventPrecommit[];
}

export interface RunnerLikeWorker {
  terminate?(): void;
}

export type WorkerFactory = (id: ProgramId | Hash) => RunnerLikeWorker;

/*
when it receives a "spawn" request
- create a contention on the base (basically tell the store "cache the current head's state" in case that another event comes by and gets committed during execution. base = kinda like snapshot time for the event, where the event gets its state)
- spin up the worker if it wasn't created yet
- post message to worker, "new event" plus an ID
- worker's glue code translates this to userspace, calls a function in user app code
- user app code does stuff, occasionally requests state or spawns new events
- remove contentions
- return the "final" event data to the system for a commit later on, plus any return data

when receiving a state get request from a worker:
- look up the event ID passed (event here means "what event is this worker processing rn")
- fetch the key at the event's base
- if the event is already committed, noop but print a warning
- return the key to the event's base

when receiving a spawn request from a worker:
- do all the stuff that the spawn function does
- return control flow to the higher level spawn function
(recursion !!!)
*/
export default class Runner {
  store: TreeStore;
  packages: PackageStore;
  transactions: Map<string, Set<string>> = new Map();
  workers: Map<string, RunnerLikeWorker> = new Map();
  private readonly workerFactory: WorkerFactory;

  constructor(
    store: TreeStore,
    packages: PackageStore,
    workerFactory?: WorkerFactory,
  ) {
    this.store = store;
    this.packages = packages;
    this.workerFactory = workerFactory ??
      (() =>
        new Worker(new URL("./glue.ts", import.meta.url).href, {
          type: "module",
        }));
  }

  spawn(id: ProgramId | Hash) {
    const worker = this.workerFactory(id);
    this.workers.set(id, worker);
  }

  async execute(
    root: EventProposal,
  ): Promise<Result<EventPrecommit, Error>> {
    const head = await this.store.getHead();

    const resolved: EventProposal = {
      ...root,
      base: root.base ?? head ?? undefined,
    };

    if (!this.workers.has(resolved.to)) {
      this.spawn(resolved.to);
    }

    if (resolved.base) {
      this.store.getCache().addContention(resolved.base);
      await this.store.traverseState(resolved.base).next();
    }

    try {
      return ok({
        ...resolved,
        diffs: new Map(),
        children: [],
      });
    } finally {
      if (resolved.base) {
        this.store.getCache().removeContention(resolved.base);
      }
    }
  }
}
