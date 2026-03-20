import type { HostIPC } from "../lib/ipc/mod.ts";
import { createHostIPC } from "../lib/ipc/mod.ts";
import type { Hash, ProgramId, Serializable } from "../lib/types.ts";
import { generateUUIDv7 } from "../lib/types.ts";
import type { TreeStore } from "./store.ts";
import type { PackageStore } from "./packages.ts";

interface EventProposal {
  from: ProgramId;
  to: Hash;
  input: Serializable;
  base?: string;
  metadata: Serializable;
}

interface EventPrecommit extends EventProposal {
  id: string;
  base: string;
  diffs: Map<string, string | null>;
  output: Serializable;
  children: EventPrecommit[];
  effects: Serializable[];
}

export type WorkerFactory = (id: ProgramId | Hash) => Worker;

interface PendingEvent {
  resolve: () => void;
  reject: (err: Error) => void;
  settled: boolean;
  base: string;
  timeoutId?: number;
  output?: unknown;
  reads?: string[];
  writes?: Record<string, string | null>;
}

export interface RunnerOptions {
  timeout?: number;
  resolveModuleUrl?: (appId: string) => string;
}

const DEFAULT_TIMEOUT = 30_000;

export default class Runner {
  store: TreeStore;
  packages: PackageStore;
  workers: Map<string, Worker> = new Map();
  ipcs: Map<string, HostIPC> = new Map();

  transactions: Map<string, Set<string>> = new Map();
  private pendingEvents: Map<string, PendingEvent> = new Map();

  private readonly workerFactory: WorkerFactory;
  private readonly timeout: number;
  private readonly resolveModuleUrl: (appId: string) => string;

  constructor(
    store: TreeStore,
    packages: PackageStore,
    workerFactory?: WorkerFactory,
    options?: RunnerOptions,
  ) {
    this.store = store;
    this.packages = packages;
    this.workerFactory = workerFactory ??
      (() => {
        const root = new URL("..", import.meta.url).pathname;
        return new Worker(
          new URL("../lib/runner/glue.ts", import.meta.url).href,
          {
            type: "module",
            deno: {
              permissions: {
                read: [`${root}lib/ipc/`, `${root}lib/types.ts`],
                net: false,
                write: false,
                run: false,
              },
            },
          } as WorkerOptions,
        );
      });
    this.timeout = options?.timeout ?? DEFAULT_TIMEOUT;
    this.resolveModuleUrl = options?.resolveModuleUrl ??
      ((id) => `file://${id}`);
  }

  async execute(
    root: EventProposal,
  ): Promise<EventPrecommit> {
    const head = await this.store.getHead();

    if (!head) throw new Error("No head found in store");

    const base = root.base ?? head;
    const proposal = { ...root, base } as EventProposal & { base: string; moduleUrl?: string };
    if (!proposal.moduleUrl) {
      proposal.moduleUrl = this.resolveModuleUrl(proposal.to);
    }
    return this.executeEvent(proposal);
  }

  private async executeEvent(
    proposal: EventProposal & { base: string },
  ): Promise<EventPrecommit> {
    const eventId = generateUUIDv7();
    const rootId =
      (proposal.metadata as Record<string, unknown>)?.rootId as string ??
        eventId;

    const { promise: treeComplete, resolve: resolveTree, reject: rejectTree } =
      Promise.withResolvers<void>();

    // Track this event in the transaction tree
    if (!this.transactions.has(rootId)) {
      this.transactions.set(rootId, new Set());
    }
    this.transactions.get(rootId)!.add(eventId);

    const pendingEvent: PendingEvent = {
      resolve: resolveTree,
      reject: rejectTree,
      settled: false,
      base: proposal.base,
    };
    this.pendingEvents.set(eventId, pendingEvent);

    // Get or create IPC for target worker
    const ipc = this.getOrCreateWorker(proposal.to);
    this.setupHandlers(ipc, rootId);

    // Event-specific result handler — receives output from glue
    ipc.on("result", (body) => {
      const { eventId: resultEventId, output, reads, writes } = body as {
        eventId: string;
        output: unknown;
        reads?: string[];
        writes?: Record<string, string | null>;
      };
      const pending = this.pendingEvents.get(resultEventId);
      if (!pending || pending.settled) return;
      pending.output = output;
      pending.reads = reads;
      pending.writes = writes;
      pending.settled = true;
      pending.resolve();
    });

    // Event-specific error handler — resolves treeComplete so the
    // runner doesn't hang waiting for a result that will never come.
    // The actual error propagates via dispatch's rejection.
    ipc.on("error", (body) => {
      const { eventId: resultEventId } = body as { eventId: string };
      const pending = this.pendingEvents.get(resultEventId);
      if (!pending || pending.settled) return;
      pending.settled = true;
      pending.resolve();
    });

    // Add contention on base
    this.store.getCache().addContention(proposal.base);

    // Timeout promise for root event
    let timeoutReject: ((err: Error) => void) | undefined;
    if (eventId === rootId) {
      const timeoutErr = new Error(`Event tree ${rootId} timed out`);
      const timeoutPromise = new Promise<never>((_, reject) => {
        timeoutReject = () => reject(timeoutErr);
        pendingEvent.timeoutId = setTimeout(
          () => reject(timeoutErr),
          this.timeout,
        ) as unknown as number;
      });

      try {
        // Race: either the dispatch + tree completes, or the timeout fires
        await Promise.race([
          (async () => {
            await this.dispatch(ipc, proposal, eventId);
            await treeComplete;
          })(),
          timeoutPromise,
        ]);

        return {
          ...proposal,
          id: eventId,
          diffs: new Map(
            Object.entries(pendingEvent.writes ?? {}).map(([k, v]) => [
              k,
              v as string | null,
            ]),
          ),
          output: pendingEvent.output as Serializable,
          children: [],
          effects: [],
        };
      } catch (err) {
        // On timeout, send abort messages to workers but don't reject
        // pending events — the error propagates via throw
        if (!pendingEvent.settled) {
          const eventIds = this.transactions.get(rootId);
          if (eventIds) {
            for (const [, ipc] of this.ipcs) {
              for (const eid of eventIds) {
                ipc.call("abort", { eventId: eid }).catch(() => {});
              }
            }
          }
        }
        pendingEvent.settled = true;
        throw err;
      } finally {
        this.store.getCache().removeContention(proposal.base);
        if (pendingEvent.timeoutId) clearTimeout(pendingEvent.timeoutId);
        this.pendingEvents.delete(eventId);
      }
    }

    // No timeout — run normally
    try {
      await this.dispatch(ipc, proposal, eventId);
      await treeComplete;

      return {
        ...proposal,
        id: eventId,
        diffs: new Map(
          Object.entries(pendingEvent.writes ?? {}).map(([k, v]) => [
            k,
            v as string | null,
          ]),
        ),
        output: pendingEvent.output as Serializable,
        children: [],
        effects: [],
      };
    } catch (err) {
      pendingEvent.settled = true;
      throw err;
    } finally {
      this.store.getCache().removeContention(proposal.base);
      this.pendingEvents.delete(eventId);
    }
  }

  private async dispatch(
    ipc: HostIPC,
    proposal: EventProposal & { base: string },
    eventId: string,
  ): Promise<void> {
    await ipc.call("execute", { ...proposal, eventId });
  }

  private setupHandlers(ipc: HostIPC, rootId: string): void {
    // Derived event call from worker
    ipc.on("call", async (body) => {
      const proposal = body as EventProposal & { base: string; moduleUrl?: string };
      if (!proposal.moduleUrl) {
        proposal.moduleUrl = this.resolveModuleUrl(proposal.to);
      }
      const eventId = generateUUIDv7();

      if (!this.transactions.has(rootId)) {
        this.transactions.set(rootId, new Set());
      }
      this.transactions.get(rootId)!.add(eventId);

      this.pendingEvents.set(eventId, {
        resolve: () => {},
        reject: () => {},
        settled: false,
        base: proposal.base,
      });

      try {
        return await this.executeEvent(proposal);
      } finally {
        this.pendingEvents.delete(eventId);
      }
    });

    // State read from worker
    ipc.on("getState", async (body) => {
      const { key, eventId } = body as { key: string; eventId: string };
      const pending = this.pendingEvents.get(eventId);
      if (!pending) return null;
      return this.store.get(key, pending.base);
    });

    // Fire-and-forget notification
    ipc.on("notify", async (body) => {
      const proposal = body as EventProposal & { base: string };
      const eventId = generateUUIDv7();
      const notifyRootId = eventId;

      this.transactions.set(notifyRootId, new Set([eventId]));
      this.pendingEvents.set(eventId, {
        resolve: () => {},
        reject: () => {},
        settled: false,
        base: proposal.base,
      });

      try {
        await this.executeEvent(proposal);
      } finally {
        this.pendingEvents.delete(eventId);
        this.transactions.delete(notifyRootId);
      }
    });
  }

  private getOrCreateWorker(to: string): HostIPC {
    let ipc = this.ipcs.get(to);
    if (!ipc) {
      let worker = this.workers.get(to);
      if (!worker) {
        worker = this.workerFactory(to as ProgramId | Hash);
        this.workers.set(to, worker);
      }
      ipc = createHostIPC(worker);
      this.ipcs.set(to, ipc);
    }
    return ipc;
  }

  private handleTimeout(rootId: string): void {
    const err = new Error(`Event tree ${rootId} timed out`);

    // Reject all pending events with the timeout error BEFORE abandoning
    const eventIds = this.transactions.get(rootId);
    if (eventIds) {
      for (const eventId of eventIds) {
        const pending = this.pendingEvents.get(eventId);
        if (pending) pending.reject(err);
      }
    }

    this.abandon(rootId);
  }

  private abandon(rootId: string): void {
    const eventIds = this.transactions.get(rootId);
    if (!eventIds) return;

    for (const eventId of eventIds) {
      const pending = this.pendingEvents.get(eventId);
      if (pending && !pending.settled) {
        pending.settled = true;
        pending.reject(new Error("Transaction abandoned"));
      }
    }

    for (const [, ipc] of this.ipcs) {
      for (const eventId of eventIds) {
        ipc.call("abort", { eventId }).catch(() => {});
      }
    }

    this.transactions.delete(rootId);
  }

  spawn(id: ProgramId | Hash): Worker {
    const worker = this.workerFactory(id);
    this.workers.set(id, worker);
    const ipc = createHostIPC(worker);
    this.ipcs.set(id, ipc);
    return worker;
  }
}
