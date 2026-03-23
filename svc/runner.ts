import type { HostIPC } from "../lib/ipc/mod.ts";
import { createHostIPC } from "../lib/ipc/mod.ts";
import type { Hash, ProgramId, Serializable } from "../lib/types.ts";
import { generateUUIDv7 } from "../lib/types.ts";
import type { TreeStore } from "./store.ts";
import type { PackageStore } from "./packages.ts";

export interface EventProposal {
  from: ProgramId;
  to: Hash;
  input: Serializable;
  base?: string;
  metadata: Serializable;
}

export interface EventPrecommit extends EventProposal {
  id: string;
  base: string;
  reads: Set<string>;
  diffs: Map<string, string | null>;
  output: Serializable;
  children: EventPrecommit[];
  effects: Serializable[];
}

export type WorkerFactory = (id: ProgramId | Hash) => Promise<Worker>;

interface PendingEvent {
  resolve: () => void;
  reject: (err: Error) => void;
  settled: boolean;
  base: string;
  proposal: EventProposal & { base: string };
  timeoutId?: number;
  output?: unknown;
  reads?: string[];
  writes?: Record<string, string | null>;
  error?: string;
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
      (async (id) => {
        const root = new URL("..", import.meta.url).pathname;

        const result = await this.packages.getEntrypoint(id as Hash);
        if (result.isErr()) throw result.error;
        const code = await result.value.arrayBuffer();

        const worker = new Worker(
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

        worker.postMessage({ type: "init", entrypoint: code });

        return worker;
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
    const proposal = { ...root, base } as EventProposal & {
      base: string;
      moduleUrl?: string;
    };
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
      proposal,
    };
    this.pendingEvents.set(eventId, pendingEvent);

    // Get or create IPC for target worker
    const ipc = await this.getOrCreateWorker(proposal.to);

    // Add contention on base
    this.store.getCache().addContention(proposal.base);

    // Timeout promise for root event
    if (eventId === rootId) {
      const timeoutErr = new Error(`Event tree ${rootId} timed out`);
      const timeoutPromise = new Promise<never>((_, reject) => {
        pendingEvent.timeoutId = setTimeout(
          () => reject(timeoutErr),
          this.timeout,
        ) as unknown as number;
      });

      try {
        await Promise.race([
          (async () => {
            await this.dispatch(ipc, proposal, eventId);
            await treeComplete;
            if (pendingEvent.error) {
              throw new Error(pendingEvent.error);
            }
          })(),
          timeoutPromise,
        ]);

        return this.buildPrecommit(proposal, eventId, pendingEvent, rootId);
      } catch (err) {
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
        const eventIds = this.transactions.get(rootId);
        if (eventIds) {
          for (const childId of eventIds) {
            this.pendingEvents.delete(childId);
          }
        }
        this.transactions.delete(rootId);
      }
    }

    // No timeout — run normally
    try {
      await this.dispatch(ipc, proposal, eventId);
      await treeComplete;
      if (pendingEvent.error) {
        throw new Error(pendingEvent.error);
      }

      return this.buildPrecommit(proposal, eventId, pendingEvent, rootId);
    } catch (err) {
      pendingEvent.settled = true;
      throw err;
    } finally {
      this.store.getCache().removeContention(proposal.base);
    }
  }

  private buildPrecommit(
    proposal: EventProposal & { base: string },
    eventId: string,
    pending: PendingEvent,
    rootId: string,
  ): EventPrecommit {
    const children: EventPrecommit[] = [];
    const eventIds = this.transactions.get(rootId);
    if (eventIds) {
      for (const childId of eventIds) {
        if (childId === eventId) continue;
        const cp = this.pendingEvents.get(childId);
        if (cp?.settled && !cp.error) {
          const cpProposal = cp.proposal;
          children.push({
            ...cpProposal,
            id: childId,
            base: cp.base,
            reads: new Set(cp.reads ?? []),
            diffs: new Map(
              Object.entries(cp.writes ?? {}).map(([k, v]) => [
                k,
                v as string | null,
              ]),
            ),
            output: cp.output as Serializable,
            children: [],
            effects: [],
          });
        }
      }
    }

    return {
      ...proposal,
      id: eventId,
      reads: new Set(pending.reads ?? []),
      diffs: new Map(
        Object.entries(pending.writes ?? {}).map(([k, v]) => [
          k,
          v as string | null,
        ]),
      ),
      output: pending.output as Serializable,
      children,
      effects: [],
    };
  }

  private async dispatch(
    ipc: HostIPC,
    proposal: EventProposal & { base: string },
    eventId: string,
  ): Promise<void> {
    await ipc.call("execute", { ...proposal, eventId });
  }

  private async getOrCreateWorker(to: string): Promise<HostIPC> {
    let ipc = this.ipcs.get(to);
    if (!ipc) {
      let worker = this.workers.get(to);
      if (!worker) {
        worker = await this.workerFactory(to as ProgramId | Hash);
        this.workers.set(to, worker);
      }
      ipc = createHostIPC(worker);
      this.ipcs.set(to, ipc);
      this.registerHandlers(ipc);
    }
    return ipc;
  }

  private registerHandlers(ipc: HostIPC): void {
    // Derived event call from worker
    ipc.on("call", async (body) => {
      const proposal = body as EventProposal & {
        base: string;
        moduleUrl?: string;
      };
      if (!proposal.moduleUrl) {
        proposal.moduleUrl = this.resolveModuleUrl(proposal.to);
      }
      return await this.executeEvent(proposal);
    });

    // State read from worker
    ipc.on("getState", async (body) => {
      const { key, eventId } = body as { key: string; eventId: string };
      const pending = this.pendingEvents.get(eventId);
      if (!pending) return null;
      return this.store.get(key, pending.base);
    });

    // Result from worker — receives output from glue
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

    // Error from worker — stores error and resolves so executeEvent can throw
    ipc.on("error", (body) => {
      const { eventId: resultEventId, error } = body as {
        eventId: string;
        error?: string;
      };
      const pending = this.pendingEvents.get(resultEventId);
      if (!pending || pending.settled) return;
      pending.error = error ?? "Unknown error";
      pending.settled = true;
      pending.resolve();
    });
  }
}
