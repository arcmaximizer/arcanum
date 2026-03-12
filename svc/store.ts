import { type Kysely } from "kysely";

export interface TreeDatabase {
  nodes: {
    id: string;
    parent: string | undefined;
    base: string | undefined;
    checkpoint_id: string | undefined;
  };
  checkpoints: {
    id: string;
    parent: string | undefined;
    event_id: string;
  };
  kv_writes: {
    key: string;
    event_id: string;
    value: string | null;
    checkpoint_id: string;
  };
  heads: {
    id: string;
    event_id: string;
  };
}

export interface TreeStore {
  addNode(
    id: string,
    parent?: string,
    kvDiffs?: Map<string, string | null>,
    base?: string,
  ): Promise<void>;
  getNode(id: string): Promise<boolean>;
  addChild(parent: string, child: string): Promise<void>;
  getParent(childId: string): Promise<string | null>;
  getChildren(parentId: string): Promise<string[]>;
  getHead(treeId?: string): Promise<string | null>;
  getHeads(): Promise<Map<string, string>>;
  setHead(eventId: string, treeId?: string): Promise<void>;
  nodes(): Promise<string[]>;
  createCheckpoint(eventId: string): Promise<string>;
  get(key: string, eventId?: string): Promise<string | null>;
  getMany(
    keys: string[],
    eventId?: string,
  ): Promise<Map<string, string | null>>;
  traverseState(
    eventId?: string,
  ): AsyncGenerator<[string, string | null], void, unknown>;
  topologicalSort(): Promise<string[]>;
  traverse<C>(visitor: TreeVisitor<C>, context: C): Promise<void>;
  traverseFrom<C>(
    nodeId: string,
    visitor: TreeVisitor<C>,
    context: C,
  ): Promise<void>;
}

type State = Map<string, string | null>;

export interface CacheService {
  addContention(eventId: string): void;
  removeContention(eventId: string): void;
  cacheState(eventId: string, state: State): void;
  getCachedState(eventId: string): State | undefined;
  incrementRefCount(eventId: string): void;
  decrementRefCount(eventId: string): void;
  clear(): void;
}

export class InMemoryCacheService implements CacheService {
  private computedStateCache = new Map<string, State>();
  private refCountCache = new Map<string, number>();
  private contentionCache = new Map<string, number>();

  addContention(eventId: string): void {
    const current = this.contentionCache.get(eventId) ?? 0;
    this.contentionCache.set(eventId, current + 1);
  }

  removeContention(eventId: string): void {
    const current = this.contentionCache.get(eventId) ?? 0;
    if (current <= 1) {
      this.contentionCache.delete(eventId);
    } else {
      this.contentionCache.set(eventId, current - 1);
    }
    this.tryEvict(eventId);
  }

  cacheState(eventId: string, state: State): void {
    this.computedStateCache.set(eventId, state);
  }

  getCachedState(eventId: string): State | undefined {
    return this.computedStateCache.get(eventId);
  }

  private tryEvict(eventId: string): void {
    const refCount = this.refCountCache.get(eventId) ?? 0;
    const contention = this.contentionCache.get(eventId) ?? 0;

    if (refCount === 0 && contention === 0) {
      this.computedStateCache.delete(eventId);
      this.refCountCache.delete(eventId);
      this.contentionCache.delete(eventId);
    }
  }

  incrementRefCount(eventId: string): void {
    const current = this.refCountCache.get(eventId) ?? 0;
    this.refCountCache.set(eventId, current + 1);
  }

  decrementRefCount(eventId: string): void {
    const current = this.refCountCache.get(eventId) ?? 0;
    if (current <= 1) {
      this.refCountCache.delete(eventId);
    } else {
      this.refCountCache.set(eventId, current - 1);
    }
    this.tryEvict(eventId);
  }

  clear(): void {
    this.computedStateCache.clear();
    this.refCountCache.clear();
    this.contentionCache.clear();
  }
}

export interface TraversalState {
  readonly parent: string | null;
  readonly depth: number;
  readonly index: number;
  readonly total: number;
}

export type TreeVisitor<C> = (
  id: string,
  state: TraversalState,
  context: C,
) => void;

export async function up(db: Kysely<any>): Promise<void> {
  await db.schema
    .createTable("nodes")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("parent", "text")
    .addColumn("base", "text")
    .addColumn("checkpoint_id", "text")
    .addPrimaryKeyConstraint("pk_nodes", ["id"])
    .execute();

  await db.schema
    .createTable("checkpoints")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("parent", "text")
    .addColumn("event_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_checkpoints", ["id"])
    .execute();

  await db.schema
    .createTable("kv_writes")
    .addColumn("key", "text", (col) => col.notNull())
    .addColumn("event_id", "text", (col) => col.notNull())
    .addColumn("value", "text")
    .addColumn("checkpoint_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_kv_writes", ["key", "event_id"])
    .execute();

  await db.schema
    .createTable("heads")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("event_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_heads", ["id"])
    .execute();
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropTable("nodes").execute();
  await db.schema.dropTable("checkpoints").execute();
  await db.schema.dropTable("kv_writes").execute();
  await db.schema.dropTable("heads").execute();
}

export const createTreeTables = up;
export const dropTreeTables = down;

export async function initTreeTables(db: Kysely<any>): Promise<void> {
  try {
    await up(db);
  } catch (e) {
    if (e instanceof Error && e.message.includes("already exists")) {
      return;
    }
    throw e;
  }
}

export class SqliteTreeStore implements TreeStore {
  private readonly cache: CacheService;

  constructor(
    private readonly db: Kysely<TreeDatabase>,
    cache?: CacheService,
  ) {
    this.cache = cache ?? new InMemoryCacheService();
  }

  async addNode(
    id: string,
    parent?: string,
    kvDiffs?: Map<string, string | null>,
    base?: string,
  ): Promise<void> {
    const parentNode = parent
      ? await this.db
        .selectFrom("nodes")
        .select("checkpoint_id")
        .where("id", "=", parent)
        .executeTakeFirst()
      : null;

    let checkpointId: string | undefined = parentNode?.checkpoint_id ??
      undefined;

    await this.db
      .insertInto("nodes")
      .values({ id, parent, base: base ?? parent, checkpoint_id: checkpointId })
      .onConflict((oc) => oc.column("id").doNothing())
      .execute();

    if (kvDiffs && kvDiffs.size > 0) {
      if (!checkpointId) {
        checkpointId = await this.createCheckpoint(id);
      }

      const writes = Array.from(kvDiffs.entries()).map(([key, value]) => ({
        key,
        event_id: id,
        value,
        checkpoint_id: checkpointId!,
      }));
      await this.db.insertInto("kv_writes").values(writes).execute();
    }

    await this.setHead(id, "main");
  }

  async getNode(id: string): Promise<boolean> {
    const row = await this.db
      .selectFrom("nodes")
      .select("id")
      .where("id", "=", id)
      .executeTakeFirst();
    return row !== undefined;
  }

  async addChild(parent: string, child: string): Promise<void> {
    const existingChild = await this.db
      .selectFrom("nodes")
      .select("parent")
      .where("id", "=", child)
      .executeTakeFirst();

    if (existingChild?.parent !== undefined && existingChild.parent !== null) {
      if (existingChild.parent === parent) {
        return;
      }
      throw new Error(
        `Node [${child}] already has a parent [${existingChild.parent}]. Trees require single parent.`,
      );
    }

    await this.db
      .insertInto("nodes")
      .values({ id: child, parent, checkpoint_id: undefined })
      .onConflict((oc) => oc.column("id").doUpdateSet({ parent }))
      .execute();
  }

  async getParent(childId: string): Promise<string | null> {
    const row = await this.db
      .selectFrom("nodes")
      .select("parent")
      .where("id", "=", childId)
      .executeTakeFirst();
    return row?.parent ?? null;
  }

  async getChildren(parentId: string): Promise<string[]> {
    const rows = await this.db
      .selectFrom("nodes")
      .select("id")
      .where("parent", "=", parentId)
      .execute();
    return rows.map((r) => r.id);
  }

  async getHead(treeId: string): Promise<string | null> {
    const row = await this.db
      .selectFrom("heads")
      .select("event_id")
      .where("id", "=", treeId)
      .executeTakeFirst();
    return row?.event_id ?? null;
  }

  async getHeads(): Promise<Map<string, string>> {
    const rows = await this.db
      .selectFrom("heads")
      .selectAll()
      .execute();
    const result = new Map<string, string>();
    for (const row of rows) {
      result.set(row.id, row.event_id);
    }
    return result;
  }

  async setHead(eventId: string, treeId: string): Promise<void> {
    await this.db
      .insertInto("heads")
      .values({ id: treeId, event_id: eventId })
      .onConflict((oc) => oc.column("id").doUpdateSet({ event_id: eventId }))
      .execute();
  }

  async nodes(): Promise<string[]> {
    const rows = await this.db
      .selectFrom("nodes")
      .select("id")
      .execute();
    return rows.map((r) => r.id);
  }

  async createCheckpoint(eventId: string): Promise<string> {
    const event = await this.db
      .selectFrom("nodes")
      .selectAll()
      .where("id", "=", eventId)
      .executeTakeFirst();

    if (!event) {
      throw new Error(`Event ${eventId} not found`);
    }

    if (event.checkpoint_id) {
      return event.checkpoint_id;
    }

    const parentCheckpointId = event.checkpoint_id ?? null;

    const parentCheckpoint = parentCheckpointId
      ? await this.db
        .selectFrom("checkpoints")
        .select("event_id")
        .where("id", "=", parentCheckpointId)
        .executeTakeFirst()
      : null;

    const parentEventId = parentCheckpoint?.event_id ?? null;

    const checkpointId = `checkpoint_${eventId}_${Date.now()}`;

    await this.db
      .insertInto("checkpoints")
      .values({
        id: checkpointId,
        parent: parentCheckpointId ?? undefined,
        event_id: eventId,
      })
      .execute();

    await this.db
      .updateTable("nodes")
      .set({ checkpoint_id: checkpointId })
      .where("id", "=", eventId)
      .execute();

    return checkpointId;
  }

  async get(key: string, eventId?: string): Promise<string | null> {
    const targetEventId = eventId ?? await this.getHead("main");
    if (!targetEventId) return null;

    const cachedState = this.cache.getCachedState(targetEventId);
    if (cachedState) {
      return cachedState.get(key) ?? null;
    }

    const event = await this.db
      .selectFrom("nodes")
      .select("checkpoint_id")
      .where("id", "=", targetEventId)
      .executeTakeFirst();

    if (!event) return null;

    const checkpointId = event.checkpoint_id;
    if (!checkpointId) return null;

    const checkpoint = await this.db
      .selectFrom("checkpoints")
      .selectAll()
      .where("id", "=", checkpointId)
      .executeTakeFirst();

    if (!checkpoint) return null;

    const checkpointEventId = checkpoint.event_id;

    const writes = await this.db
      .selectFrom("kv_writes")
      .selectAll()
      .where("key", "=", key)
      .where("event_id", ">", checkpointEventId)
      .where("event_id", "<=", targetEventId)
      .orderBy("event_id", "asc")
      .execute();

    if (writes.length === 0) {
      const priorWrite = await this.db
        .selectFrom("kv_writes")
        .selectAll()
        .where("key", "=", key)
        .where("event_id", "<=", checkpointEventId)
        .orderBy("event_id", "desc")
        .executeTakeFirst();
      return priorWrite?.value ?? null;
    }

    const latestWrite = writes[writes.length - 1];
    return latestWrite?.value ?? null;
  }

  async getMany(
    keys: string[],
    eventId?: string,
  ): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>();

    for (const key of keys) {
      result.set(key, await this.get(key, eventId));
    }

    return result;
  }

  async *traverseState(
    eventId?: string,
  ): AsyncGenerator<[string, string | null], void, unknown> {
    const targetEventId = eventId ?? await this.getHead("main");
    if (!targetEventId) return;

    const event = await this.db
      .selectFrom("nodes")
      .select("checkpoint_id")
      .where("id", "=", targetEventId)
      .executeTakeFirst();

    if (!event || !event.checkpoint_id) return;

    const checkpoint = await this.db
      .selectFrom("checkpoints")
      .select("event_id")
      .where("id", "=", event.checkpoint_id)
      .executeTakeFirst();

    if (!checkpoint) return;

    const checkpointEventId = checkpoint.event_id;

    const allWrites = await this.db
      .selectFrom("kv_writes")
      .selectAll()
      .where("event_id", "<=", targetEventId)
      .orderBy("event_id", "asc")
      .execute();

    const state = new Map<string, string | null>();

    for (const write of allWrites) {
      if (write.value === null) {
        state.delete(write.key);
      } else {
        state.set(write.key, write.value);
      }
    }

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

  private async getRoots(): Promise<string[]> {
    const rows = await this.db
      .selectFrom("nodes")
      .select("id")
      .where("parent", "is", null)
      .execute();
    return rows.map((r) => r.id);
  }

  async traverse<C>(
    visitor: TreeVisitor<C>,
    context: C,
  ): Promise<void> {
    const roots = await this.getRoots();
    if (roots.length === 0) return;

    const outgoing = await this.loadOutgoing();

    const visitNode = (
      id: string,
      parent: string | null,
      depth: number,
      index: number,
      total: number,
    ): void => {
      visitor(id, { parent, depth, index, total }, context);

      const children = outgoing.get(id) ?? [];
      children.forEach((childId, i) => {
        visitNode(childId, id, depth + 1, i, children.length);
      });
    };

    roots.forEach((root, i) => {
      visitNode(root, null, 0, i, roots.length);
    });
  }

  async traverseFrom<C>(
    nodeId: string,
    visitor: TreeVisitor<C>,
    context: C,
  ): Promise<void> {
    const outgoing = await this.loadOutgoing();

    const childCount = (outgoing.get(nodeId) ?? []).length;

    const visitNode = (
      id: string,
      parent: string | null,
      depth: number,
      index: number,
      total: number,
    ): void => {
      visitor(id, { parent, depth, index, total }, context);

      const children = outgoing.get(id) ?? [];
      children.forEach((childId, i) => {
        visitNode(childId, id, depth + 1, i, children.length);
      });
    };

    visitNode(nodeId, null, 0, 0, childCount);
  }

  private async loadOutgoing(): Promise<Map<string, string[]>> {
    const allNodes = await this.db.selectFrom("nodes").selectAll().execute();

    const out = new Map<string, string[]>();
    for (const { id, parent } of allNodes) {
      if (parent) {
        let arr = out.get(parent);
        if (!arr) {
          arr = [];
          out.set(parent, arr);
        }
        arr.push(id);
      }
    }
    return out;
  }
}
