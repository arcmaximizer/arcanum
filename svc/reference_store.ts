import { type Kysely } from "kysely";
import type {
  CacheService,
  EventData,
  TraversalState,
  TreeDatabase,
  TreeStore,
  TreeVisitor,
} from "./store.ts";

// Naive in-memory implementation of TreeStore
// No caching, no checkpoints, purely event-based reconstruction
export class ReferenceTreeStore implements TreeStore {
  private nodeData = new Map<string, {
    type: string;
    parent: string | undefined;
    base: string | undefined;
    index: number | undefined;
  }>();
  private eventData = new Map<string, {
    from: string | undefined;
    to: string | undefined;
    data: string | undefined;
    returns: string | undefined;
  }>();
  private kvWrites = new Map<
    string,
    Array<{ key: string; value: string | null }>
  >(); // eventId -> writes
  private kvReads = new Map<string, Set<string>>(); // eventId -> keys read
  private heads = new Map<string, string>(); // treeId -> eventId
  private derivedEvents = new Map<string, string[]>(); // originId -> [derivedIds]
  private effects = new Map<string, any[]>(); // eventId -> effects
  private codeUpgrades = new Map<string, string>(); // nodeId -> hash

  constructor(private readonly db: Kysely<TreeDatabase>) {}

  // No caching in reference implementation
  getCache(): CacheService {
    return {
      addContention: () => {},
      removeContention: () => {},
      cacheState: () => {},
      getCachedState: () => undefined,
      clear: () => {},
    };
  }

  getStateBuildStats() {
    return {
      checkpointHits: 0,
      fullRebuilds: 0,
      lineageEventsApplied: 0,
      cachedStateHits: 0,
    };
  }

  resetStateBuildStats() {}

  async addNode(
    id: string,
    parent?: string,
    kvDiffs?: Map<string, string | null>,
    kvReads?: Set<string>,
    base?: string,
    index?: number,
    event?: EventData,
    derived?: string[],
    effects?: any[],
    type: string = "event",
  ): Promise<void> {
    // Store node data (idempotent - don't overwrite if exists)
    if (!this.nodeData.has(id)) {
      this.nodeData.set(id, {
        type,
        parent,
        base: base ?? parent ?? id,
        index,
      });
    }

    // Store event data if provided
    if (event && !this.eventData.has(id)) {
      this.eventData.set(id, {
        from: event.from,
        to: event.to,
        data: event.data !== undefined ? JSON.stringify(event.data) : undefined,
        returns: event.returns !== undefined
          ? JSON.stringify(event.returns)
          : undefined,
      });
    }

    // Store KV writes
    if (kvDiffs && kvDiffs.size > 0) {
      const writes = Array.from(kvDiffs.entries()).map(([key, value]) => ({
        key,
        value,
      }));
      this.kvWrites.set(id, writes);
    }

    // Store KV reads
    if (kvReads && kvReads.size > 0) {
      this.kvReads.set(id, new Set(kvReads));
    }

    // Store derived events
    if (derived && derived.length > 0) {
      this.derivedEvents.set(id, [...derived]);
    }

    // Store effects
    if (effects && effects.length > 0) {
      this.effects.set(id, [...effects]);
    }

    // Set head (default to "main" if not specified, matching SqliteTreeStore behavior)
    // Always set head even if node exists (matches SqliteTreeStore behavior)
    await this.setHead(id, "main");
  }

  async getNode(id: string): Promise<boolean> {
    return this.nodeData.has(id);
  }

  async getNodeDetails(id: string): Promise<
    {
      id: string;
      type: string;
      parent: string | undefined;
      base: string | undefined;
      checkpoint_id: string | undefined;
      index: number | undefined;
      from: string | undefined;
      to: string | undefined;
      data: any;
      returns: any;
    } | null
  > {
    const node = this.nodeData.get(id);
    if (!node) return null;

    const event = this.eventData.get(id);
    return {
      id,
      type: node.type,
      parent: node.parent,
      base: node.base,
      checkpoint_id: undefined, // No checkpoints in reference implementation
      index: node.index,
      from: event?.from,
      to: event?.to,
      data: event?.data ? JSON.parse(event.data) : undefined,
      returns: event?.returns ? JSON.parse(event.returns) : undefined,
    };
  }

  async addChild(parent: string, child: string): Promise<void> {
    const existingChild = this.nodeData.get(child);
    if (existingChild?.parent !== undefined && existingChild.parent !== null) {
      if (existingChild.parent === parent) return;
      throw new Error(
        `Node [${child}] already has a parent [${existingChild.parent}]. Trees require single parent.`,
      );
    }

    // If child doesn't exist yet, create a minimal node entry
    if (!existingChild) {
      this.nodeData.set(child, {
        type: "event",
        parent,
        base: parent,
        index: undefined,
      });
    } else {
      // Update existing node's parent
      existingChild.parent = parent;
      this.nodeData.set(child, existingChild);
    }
  }

  async getParent(childId: string): Promise<string | null> {
    const node = this.nodeData.get(childId);
    return node?.parent ?? null;
  }

  async getChildren(parentId: string): Promise<string[]> {
    const children: string[] = [];
    for (const [id, node] of this.nodeData) {
      if (node.parent === parentId) {
        children.push(id);
      }
    }
    return children.sort(); // Deterministic order
  }

  async getHead(treeId?: string): Promise<string | null> {
    const id = treeId ?? "main";
    return this.heads.get(id) ?? null;
  }

  async getHeads(): Promise<Map<string, string>> {
    return new Map(this.heads);
  }

  async setHead(eventId: string, treeId?: string): Promise<void> {
    const id = treeId ?? "main";
    this.heads.set(id, eventId);
  }

  async nodes(): Promise<string[]> {
    return Array.from(this.nodeData.keys()).sort();
  }

  async createCheckpoint(eventId: string): Promise<string> {
    // No checkpoints in reference implementation
    // Return a dummy ID to maintain interface compatibility
    return `checkpoint_${eventId}`;
  }

  async addCodeUpgrade(
    id: string,
    parent: string,
    hash: string,
    index?: number,
  ): Promise<void> {
    if (!this.nodeData.has(id)) {
      this.nodeData.set(id, {
        type: "upgrade",
        parent,
        base: parent,
        index,
      });
    }
    this.codeUpgrades.set(id, hash);
    await this.setHead(id, "main");
  }

  async getCodeUpgrade(nodeId: string): Promise<string | null> {
    return this.codeUpgrades.get(nodeId) ?? null;
  }

  async getCodeAtBase(baseNodeId: string): Promise<string | null> {
    const lineage = await this.getLineage(baseNodeId);
    // Walk lineage from base (end) to root, return the first code upgrade found
    for (let i = lineage.length - 1; i >= 0; i--) {
      const nodeId = lineage[i];
      if (nodeId && this.codeUpgrades.has(nodeId)) {
        return this.codeUpgrades.get(nodeId)!;
      }
    }
    return null;
  }

  async get(key: string, eventId?: string): Promise<string | null> {
    const targetEventId = eventId ?? await this.getHead("main");
    if (!targetEventId) return null;

    // Naive approach: reconstruct state by walking lineage
    const state = await this.rebuildState(targetEventId);
    return state.get(key) ?? null;
  }

  async getMany(
    keys: string[],
    eventId?: string,
  ): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>();
    const targetEventId = eventId ?? await this.getHead("main");
    const finalState = targetEventId
      ? await this.rebuildState(targetEventId)
      : new Map<string, string | null>();

    for (const key of keys) {
      result.set(key, finalState.get(key) ?? null);
    }
    return result;
  }

  async getReads(eventId: string): Promise<Set<string>> {
    return this.kvReads.get(eventId) ?? new Set();
  }

  async *traverseState(
    eventId?: string,
  ): AsyncGenerator<[string, string | null], void, unknown> {
    const targetEventId = eventId ?? await this.getHead("main");
    if (!targetEventId) return;

    const state = await this.rebuildState(targetEventId);
    for (const [key, value] of state) {
      yield [key, value];
    }
  }

  async topologicalSort(): Promise<string[]> {
    const roots = await this.getRoots();
    if (roots.length === 0) return [];

    const sorted: string[] = [];
    const visited = new Set<string>();

    const traverse = async (nodeId: string): Promise<void> => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);

      sorted.push(nodeId);

      const children = await this.getChildren(nodeId);
      for (const child of children) {
        await traverse(child);
      }
    };

    for (const root of roots) {
      await traverse(root);
    }

    return sorted;
  }

  async traverse<C>(visitor: TreeVisitor<C>, context: C): Promise<void> {
    const roots = await this.getRoots();
    if (roots.length === 0) return;

    // Use stack for iterative traversal
    const stack: Array<[string, string | null, number, number, number]> = [];

    // Push roots onto stack (sorted for determinism)
    const sortedRoots = roots.sort();
    for (let i = sortedRoots.length - 1; i >= 0; i--) {
      const root = sortedRoots[i];
      if (root) {
        stack.push([root, null, 0, i, sortedRoots.length]);
      }
    }

    // Pre-load children for efficiency
    const allChildren = new Map<string, string[]>();
    for (const [id, node] of this.nodeData) {
      if (node.parent) {
        const children = allChildren.get(node.parent) ?? [];
        children.push(id);
        allChildren.set(node.parent, children);
      }
    }
    // Sort children for determinism
    for (const children of allChildren.values()) {
      children.sort();
    }

    while (stack.length > 0) {
      const item = stack.pop();
      if (!item) break;
      const [id, parent, depth, index, total] = item;
      visitor(id, { parent, depth, index, total }, context);

      const children = allChildren.get(id) ?? [];
      // Push children in reverse order so they are processed in correct order
      for (let i = children.length - 1; i >= 0; i--) {
        const child = children[i];
        if (child) {
          stack.push([child, id, depth + 1, i, children.length]);
        }
      }
    }
  }

  async traverseFrom<C>(
    nodeId: string,
    visitor: TreeVisitor<C>,
    context: C,
  ): Promise<void> {
    // Pre-load children for efficiency
    const allChildren = new Map<string, string[]>();
    for (const [id, node] of this.nodeData) {
      if (node.parent) {
        const children = allChildren.get(node.parent) ?? [];
        children.push(id);
        allChildren.set(node.parent, children);
      }
    }
    // Sort children for determinism
    for (const children of allChildren.values()) {
      children.sort();
    }

    const childCount = (allChildren.get(nodeId) ?? []).length;
    const stack: Array<[string, string | null, number, number, number]> = [];
    stack.push([nodeId, null, 0, 0, childCount]);

    while (stack.length > 0) {
      const item = stack.pop();
      if (!item) break;
      const [id, parent, depth, index, total] = item;
      visitor(id, { parent, depth, index, total }, context);

      const children = allChildren.get(id) ?? [];
      // Push children in reverse order so they are processed in correct order
      for (let i = children.length - 1; i >= 0; i--) {
        const child = children[i];
        if (child) {
          stack.push([child, id, depth + 1, i, children.length]);
        }
      }
    }
  }

  // Private helper to rebuild state naively
  private async rebuildState(
    eventId: string,
  ): Promise<Map<string, string | null>> {
    const lineage = await this.getLineage(eventId);
    const state = new Map<string, string | null>();

    for (const id of lineage) {
      const writes = this.kvWrites.get(id) ?? [];
      for (const { key, value } of writes) {
        if (value === null) {
          state.delete(key);
        } else {
          state.set(key, value);
        }
      }
    }

    return state;
  }

  private async getLineage(eventId: string): Promise<string[]> {
    const lineage: string[] = [];
    let current: string | null = eventId;

    while (current) {
      lineage.push(current);
      const node = this.nodeData.get(current);
      current = node?.parent ?? null;
    }

    lineage.reverse();
    return lineage;
  }

  private async getRoots(): Promise<string[]> {
    const roots: string[] = [];
    for (const [id, node] of this.nodeData) {
      if (!node.parent) {
        roots.push(id);
      }
    }
    return roots.sort();
  }
}
