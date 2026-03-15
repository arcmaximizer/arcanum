import { assertEquals, assertExists } from "@std/assert";
import Runner from "../svc/runner.ts";
import type {
  CacheService,
  TraversalState,
  TreeStore,
  TreeVisitor,
} from "../svc/store.ts";

class FakeCache implements CacheService {
  readonly states = new Map<string, Map<string, string | null>>();
  readonly contentions = new Map<string, number>();

  addContention(eventId: string): void {
    this.contentions.set(eventId, (this.contentions.get(eventId) ?? 0) + 1);
  }

  removeContention(eventId: string): void {
    const next = (this.contentions.get(eventId) ?? 0) - 1;
    if (next <= 0) {
      this.contentions.delete(eventId);
    } else {
      this.contentions.set(eventId, next);
    }
  }

  cacheState(eventId: string, state: Map<string, string | null>): void {
    this.states.set(eventId, new Map(state));
  }

  getCachedState(eventId: string): Map<string, string | null> | undefined {
    return this.states.get(eventId);
  }

  clear(): void {
    this.states.clear();
    this.contentions.clear();
  }
}

class FakeStore implements TreeStore {
  readonly cache = new FakeCache();
  readonly traversedBases: string[] = [];
  head: string | null = "head-1";

  async addNode(): Promise<void> {}
  async getNode(): Promise<boolean> {
    return false;
  }
  async addChild(): Promise<void> {}
  async getParent(): Promise<string | null> {
    return null;
  }
  async getChildren(): Promise<string[]> {
    return [];
  }
  async getHead(): Promise<string | null> {
    return this.head;
  }
  async getHeads(): Promise<Map<string, string>> {
    return new Map();
  }
  async setHead(): Promise<void> {}
  async nodes(): Promise<string[]> {
    return [];
  }
  async createCheckpoint(): Promise<string> {
    return "checkpoint";
  }
  async get(): Promise<string | null> {
    return null;
  }
  async getNodeDetails(): Promise<
    {
      id: string;
      parent: string | undefined;
      base: string | undefined;
      checkpoint_id: string | undefined;
      from: string | undefined;
      to: string | undefined;
      index: number | undefined;
      data: any;
      returns: any;
    } | null
  > {
    return null;
  }
  async getMany(): Promise<Map<string, string | null>> {
    return new Map();
  }
  async getReads(): Promise<Set<string>> {
    return new Set();
  }
  getCache(): CacheService {
    return this.cache;
  }
  getStateBuildStats() {
    return {
      checkpointHits: 0,
      fullRebuilds: 0,
      lineageEventsApplied: 0,
      cachedStateHits: 0,
    } as const;
  }
  resetStateBuildStats(): void {}
  async *traverseState(
    eventId?: string,
  ): AsyncGenerator<[string, string | null], void, unknown> {
    if (eventId) {
      this.traversedBases.push(eventId);
      this.cache.cacheState(eventId, new Map([["warm", "1"]]));
    }
    yield ["warm", "1"];
  }
  async topologicalSort(): Promise<string[]> {
    return [];
  }
  async traverse<C>(_visitor: TreeVisitor<C>, _context: C): Promise<void> {}
  async traverseFrom<C>(
    _nodeId: string,
    _visitor: TreeVisitor<C>,
    _context: C,
  ): Promise<void> {}
}

Deno.test("runner execute warms base cache and releases contention", async () => {
  const store = new FakeStore();
  const workerIds: string[] = [];
  const runner = new Runner(
    store,
    {} as never,
    (id) => {
      workerIds.push(String(id));
      return {};
    },
  );

  const result = await runner.execute({
    from: "app/a" as never,
    to: "app/b" as never,
    input: null,
    metadata: null,
  });

  assertEquals(result.isOk(), true);
  assertEquals(workerIds, ["app/b"]);
  assertEquals(store.traversedBases, ["head-1"]);
  assertExists(store.cache.getCachedState("head-1"));
  assertEquals(store.cache.contentions.get("head-1"), undefined);
});

Deno.test("runner execute respects explicit base instead of current head", async () => {
  const store = new FakeStore();
  store.head = "head-2";
  const runner = new Runner(store, {} as never, () => ({}));

  const result = await runner.execute({
    from: "app/a" as never,
    to: "app/c" as never,
    input: null,
    metadata: null,
    base: "snapshot-1",
  });

  assertEquals(result.isOk(), true);
  assertEquals(store.traversedBases, ["snapshot-1"]);
  assertExists(store.cache.getCachedState("snapshot-1"));
});
