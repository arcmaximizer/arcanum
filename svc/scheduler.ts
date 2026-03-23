import type { Hash, ProgramId, Serializable } from "../lib/types.ts";
import type { EventPrecommit, EventProposal } from "./runner.ts";
import Runner from "./runner.ts";
import type { TreeStore } from "./store.ts";
import type { PackageStore } from "./packages.ts";

export class OCCConflictError extends Error {
  readonly conflictKeys: string[];

  constructor(conflictKeys: string[]) {
    super(`OCC conflict on keys: ${conflictKeys.join(", ")}`);
    this.conflictKeys = conflictKeys;
  }
}

export interface EventResult {
  precommit: EventPrecommit;
  head: string;
}

export default class Scheduler {
  private readonly runner: Runner;
  private readonly store: TreeStore;

  constructor(
    store: TreeStore,
    packages: PackageStore,
    options?: { timeout?: number },
  ) {
    this.runner = new Runner(store, packages, undefined, options);
    this.store = store;
  }

  async execute(
    proposal: Omit<EventProposal, "base"> & { base?: string },
  ): Promise<EventResult> {
    const baseHead = await this.store.getHead();
    if (!baseHead) throw new Error("No head found in store");

    const base = proposal.base ?? baseHead;
    const precommit = await this.runner.execute({ ...proposal, base });

    // Re-read head to detect TOCTOU: another execution may have committed
    // between our base read and now.
    const currentHead = await this.store.getHead();
    if (!currentHead) throw new Error("Head disappeared during execution");

    const conflicts = await this.detectConflicts(precommit, currentHead);
    if (conflicts.length > 0) {
      throw new OCCConflictError(conflicts);
    }

    const newHead = await this.commit(precommit, currentHead);
    return { precommit, head: newHead };
  }

  private async detectConflicts(
    precommit: EventPrecommit,
    head: string,
  ): Promise<string[]> {
    const conflicts: string[] = [];
    const seen = new Set<string>();

    await this.validateEvent(precommit, head, conflicts, seen);
    for (const child of precommit.children) {
      await this.validateEvent(child, head, conflicts, seen);
    }

    this.validateIntraTree(precommit, conflicts, seen);

    return conflicts;
  }

  private async validateEvent(
    event: EventPrecommit,
    head: string,
    conflicts: string[],
    seen: Set<string>,
  ): Promise<void> {
    if (!event.reads || event.reads.size === 0) return;

    for (const key of event.reads) {
      if (seen.has(key)) continue;
      seen.add(key);

      const baseValue = await this.store.get(key, event.base);
      const headValue = await this.store.get(key, head);
      if (baseValue !== headValue) {
        conflicts.push(key);
      }
    }
  }

  private validateIntraTree(
    precommit: EventPrecommit,
    conflicts: string[],
    seen: Set<string>,
  ): void {
    if (precommit.children.length === 0) return;

    const allWrites = new Map<string, Set<string>>();

    const addWrites = (event: EventPrecommit) => {
      if (event.diffs) {
        for (const key of event.diffs.keys()) {
          if (!allWrites.has(key)) allWrites.set(key, new Set());
          allWrites.get(key)!.add(event.id);
        }
      }
    };

    addWrites(precommit);
    for (const child of precommit.children) {
      addWrites(child);
    }

    const checkReads = (event: EventPrecommit) => {
      if (!event.reads) return;
      for (const key of event.reads) {
        if (seen.has(key)) continue;
        const writers = allWrites.get(key);
        if (writers && !writers.has(event.id)) {
          conflicts.push(key);
          seen.add(key);
        }
      }
    };

    checkReads(precommit);
    for (const child of precommit.children) {
      checkReads(child);
    }
  }

  private async commit(
    precommit: EventPrecommit,
    head: string,
  ): Promise<string> {
    for (const child of precommit.children) {
      await this.store.addNode(
        child.id,
        precommit.id,
        child.diffs,
        new Set(child.diffs.keys()),
        child.base,
        undefined,
        {
          from: child.from as unknown as string,
          to: child.to as unknown as string,
          data: child.input,
          returns: child.output,
        },
      );
    }

    const childIds = precommit.children.map((c) => c.id);
    await this.store.addNode(
      precommit.id,
      head,
      precommit.diffs,
      new Set(precommit.diffs.keys()),
      precommit.base,
      undefined,
      {
        from: precommit.from as unknown as string,
        to: precommit.to as unknown as string,
        data: precommit.input,
        returns: precommit.output,
      },
      childIds,
      precommit.effects,
    );

    return precommit.id;
  }
}
